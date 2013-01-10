/** 
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * @author Felipe Oliveira (http://mashup.fm)
 * 
 */
package play.modules.elasticsearch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang.Validate;
import org.elasticsearch.client.Client;

import play.Logger;
import play.Play;
import play.PlayPlugin;
import play.db.Model;
import play.modules.elasticsearch.ElasticSearchIndexEvent.Type;
import play.modules.elasticsearch.adapter.ElasticSearchAdapter;
import play.modules.elasticsearch.client.ElasticSearchClientLifecycle;
import play.modules.elasticsearch.mapping.MapperFactory;
import play.modules.elasticsearch.mapping.MappingUtil;
import play.modules.elasticsearch.mapping.ModelMapper;
import play.modules.elasticsearch.mapping.impl.DefaultMapperFactory;
import play.modules.elasticsearch.util.ReflectionUtil;
import play.mvc.Router;

// TODO: Auto-generated Javadoc
/**
 * The Class ElasticSearchPlugin.
 */
public class ElasticSearchPlugin extends PlayPlugin {

	/** Signals whether to save events instead of processing them asynchronously. */
	private static boolean blockEvents = false;

	/** The mapper factory */
	private static MapperFactory mapperFactory = null;

	/** The mappers index. */
	private static Map<Class<?>, ModelMapper<?>> mappers = null;

	/** The started indices. */
	private static Set<Class<?>> indicesStarted = null;

	/** Index type -> Class lookup */
	private static Map<String, Class<?>> modelLookup = null;

	private static ElasticSearchClientLifecycle elasticClientLifecycle = new ElasticSearchClientLifecycle();

	private static final Queue<Model> blockedIndexOperations = new ConcurrentLinkedQueue<Model>();

	private static final Queue<Model> blockedDeleteOperations = new ConcurrentLinkedQueue<Model>();

	// TODO usages of this method should be removed and moved to a common 'elastic interface'
	public static Client client() {
		return elasticClientLifecycle.getTransportClient();
	}

	private static boolean shouldUseTransportClient() {
		// TODO get from property
		return true;
	}

	public static void setMapperFactory(final MapperFactory factory) {
		mapperFactory = factory;
		mappers.clear();
	}

	public static MapperFactory getMapperFactory() {
		return mapperFactory;
	}

	/**
	 * Gets the delivery mode.
	 * 
	 * @return the delivery mode
	 */
	public static ElasticSearchDeliveryMode getDeliveryMode() {
		final String s = Play.configuration.getProperty("elasticsearch.delivery");
		if (s == null) {
			return ElasticSearchDeliveryMode.LOCAL;
		}
		if ("CUSTOM".equals(s)) {
			return ElasticSearchDeliveryMode.createCustomIndexEventHandler(Play.configuration.getProperty(
					"elasticsearch.customIndexEventHandler", "play.modules.elasticsearch.LocalIndexEventHandler"));
		}
		return ElasticSearchDeliveryMode.valueOf(s.toUpperCase());
	}

	/**
	 * This method is called when the application starts - It will start ES instance
	 * 
	 * @see play.PlayPlugin#onApplicationStart()
	 */
	@Override
	public void onApplicationStart() {
		// (re-)set caches
		mappers = new HashMap<Class<?>, ModelMapper<?>>();
		modelLookup = new HashMap<String, Class<?>>();
		indicesStarted = new HashSet<Class<?>>();

		mapperFactory = new DefaultMapperFactory(getIndexPrefix());

		ReflectionUtil.clearCache();

		// Make sure it doesn't get started more than once
		if (elasticClientLifecycle.started()) {
			Logger.debug("Elastic Search Started Already!");
			return;
		}

		elasticClientLifecycle.start(shouldUseTransportClient());

		// Bind Admin
		Router.addRoute("GET", "/es-admin", "elasticsearch.ElasticSearchAdmin.index");

		// Check Client
		if (elasticClientLifecycle.notStarted()) {
			throw new RuntimeException(
					"Elastic Search Client cannot be null - please check the configuration provided and the health of your Elastic Search instances.");
		}
	}

	public static String getIndexPrefix() {
		if (Play.configuration.containsKey("elasticsearch.indexprefix")) {
			return Play.configuration.getProperty("elasticsearch.indexprefix");
		} else {
			return "";
		}
	}

	@SuppressWarnings("unchecked")
	public static <M> ModelMapper<M> getMapper(final Class<M> clazz) {
		if (mappers.containsKey(clazz)) {
			return (ModelMapper<M>) mappers.get(clazz);
		}

		final ModelMapper<M> mapper = mapperFactory.getMapper(clazz);
		mappers.put(clazz, mapper);
		modelLookup.put(mapper.getTypeName(), clazz);

		return mapper;
	}

	private static void startIndexIfNeeded(final Class<Model> clazz) {
		if (!indicesStarted.contains(clazz)) {
			final ModelMapper<Model> mapper = getMapper(clazz);
			Logger.info("Start Index for Class: %s", clazz);
			ElasticSearchAdapter.startIndex(client(), mapper);
			indicesStarted.add(clazz);
		}
	}

	private static boolean isInterestingEvent(final String event) {
		return event.endsWith(".objectPersisted") || event.endsWith(".objectUpdated")
				|| event.endsWith(".objectDeleted");
	}

	/**
	 * This is the method that will be sending data to ES instance
	 * 
	 * @see play.PlayPlugin#onEvent(java.lang.String, java.lang.Object)
	 */
	@Override
	public void onEvent(final String message, final Object context) {
		// Log Debug
		Logger.debug("Received %s Event, Object: %s", message, context);

		if (isInterestingEvent(message) == false) {
			return;
		}

		Logger.debug("Processing %s Event", message);

		// Check if object is searchable
		if (MappingUtil.isSearchable(context.getClass()) == false) {
			return;
		}

		// Sanity check, we only index models
		Validate.isTrue(context instanceof Model, "Only play.db.Model subclasses can be indexed");

		// Start index if needed
		@SuppressWarnings("unchecked")
		final Class<Model> clazz = (Class<Model>) context.getClass();
		startIndexIfNeeded(clazz);

		// Define Event
		ElasticSearchIndexEvent event = null;
		if (message.endsWith(".objectPersisted") || message.endsWith(".objectUpdated")) {
			// Index Model
			if (blockEvents) {
				// If blocked, just enqueue the operation
				blockedIndexOperations.offer((Model) context);
				return;
			}
			event = new ElasticSearchIndexEvent((Model) context, ElasticSearchIndexEvent.Type.INDEX);

		} else if (message.endsWith(".objectDeleted")) {
			// Delete Model from Index
			if (blockEvents) {
				// If blocked, just enqueue the operation
				blockedDeleteOperations.offer((Model) context);
				return;
			}
			event = new ElasticSearchIndexEvent((Model) context, ElasticSearchIndexEvent.Type.DELETE);
		}

		// Sync with Elastic Search
		Logger.debug("Elastic Search Index Event: %s", event);
		if (event != null) {
			final ElasticSearchDeliveryMode deliveryMode = getDeliveryMode();
			final IndexEventHandler handler = deliveryMode.getHandler();
			handler.handle(event);
		}
	}

	<M extends Model> void index(final M model) {
		@SuppressWarnings("unchecked")
		final Class<Model> clazz = (Class<Model>) model.getClass();

		// Check if object is searchable
		if (MappingUtil.isSearchable(clazz) == false) {
			throw new IllegalArgumentException("model is not searchable");
		}

		startIndexIfNeeded(clazz);

		final ElasticSearchIndexEvent event = new ElasticSearchIndexEvent(model, Type.INDEX);
		final ElasticSearchDeliveryMode deliveryMode = getDeliveryMode();
		final IndexEventHandler handler = deliveryMode.getHandler();
		handler.handle(event);
	}

	public static void batchProcessBlockedOperations() {
		Model model;
		try {
			while ((model = blockedIndexOperations.poll()) != null) {
				@SuppressWarnings("unchecked")
				final ModelMapper<Model> mapper = (ModelMapper<Model>) getMapper(model.getClass());

				ElasticSearchAdapter.indexModel(client(), mapper, model);
			}
			while ((model = blockedDeleteOperations.poll()) != null) {
				@SuppressWarnings("unchecked")
				final ModelMapper<Model> mapper = (ModelMapper<Model>) getMapper(model.getClass());
				ElasticSearchAdapter.deleteModel(client(), mapper, model);
			}
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	public static boolean isBlockEvents() {
		return blockEvents;
	}

	public static void setBlockEvents(final boolean blockEvents) {
		ElasticSearchPlugin.blockEvents = blockEvents;
	}

	/**
	 * Looks up the model class based on the index type name
	 * 
	 * @param indexType
	 * @return Class of the Model
	 */
	public static Class<?> lookupModel(final String indexType) {
		return modelLookup.get(indexType);
	}

	public static void startTransportClient() {
		elasticClientLifecycle.start(true);
	}

	public static void startJestClient() {
		elasticClientLifecycle.start(false);
	}

}
