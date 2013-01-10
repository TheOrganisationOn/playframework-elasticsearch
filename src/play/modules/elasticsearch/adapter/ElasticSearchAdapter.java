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
package play.modules.elasticsearch.adapter;

import java.io.IOException;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import play.Logger;
import play.db.Model;
import play.modules.elasticsearch.client.ElasticSearchClientInterface;
import play.modules.elasticsearch.mapping.ModelMapper;

/**
 * The Class ElasticSearchAdapter.
 */
public abstract class ElasticSearchAdapter {

	/**
	 * Start index.
	 * 
	 * @param client
	 *            the client
	 * @param mapper
	 *            the model mapper
	 */
	public static <T> void startIndex(ElasticSearchClientInterface clientInterface, ModelMapper<T> mapper) {
		createIndex(clientInterface, mapper);
		createType(clientInterface, mapper);
	}

	/**
	 * Creates the index.
	 * 
	 * @param client
	 *            the client
	 * @param mapper
	 *            the model mapper
	 */
	private static void createIndex(ElasticSearchClientInterface client, ModelMapper<?> mapper) {
		String indexName = mapper.getIndexName();

		client.createIndex(indexName);
	}

	/**
	 * Creates the type.
	 * 
	 * @param client
	 *            the client
	 * @param mapper
	 *            the model mapper
	 */
	private static void createType(ElasticSearchClientInterface client, ModelMapper<?> mapper) {
		String indexName = mapper.getIndexName();
		String typeName = mapper.getTypeName();

		client.createType(indexName, typeName, mapper);
	}

	public static <T> void indexModel(ElasticSearchClientInterface client, ModelMapper<T> mapper, T object)
			throws IOException {
		Logger.debug("Index Model: %s", object);

		// Define Content Builder
		XContentBuilder contentBuilder = null;

		// Index Model
		try {
			// Define Index Name
			String indexName = mapper.getIndexName();
			String typeName = mapper.getTypeName();
			String documentId = mapper.getDocumentId(object);
			Logger.debug("Index Name: %s", indexName);

			contentBuilder = XContentFactory.jsonBuilder().prettyPrint();
			mapper.addModel(object, contentBuilder);
			Logger.debug("Index json: %s", contentBuilder.string());

			client.indexDocument(indexName, typeName, documentId, contentBuilder.string());

		} finally {
			if (contentBuilder != null) {
				contentBuilder.close();
			}
		}
	}

	/**
	 * Delete model.
	 * 
	 * @param <T>
	 *            the generic type
	 * @param client
	 *            the client
	 * @param mapper
	 *            the model mapper
	 * @param model
	 *            the model
	 * @throws Exception
	 *             the exception
	 */
	public static <T extends Model> void deleteModel(Client client, ModelMapper<T> mapper, T model)
			throws Exception {
		Logger.debug("Delete Model: %s", model);
		String indexName = mapper.getIndexName();
		String typeName = mapper.getTypeName();
		String documentId = mapper.getDocumentId(model);
		DeleteResponse response = client.prepareDelete(indexName, typeName, documentId)
				.setOperationThreaded(false).execute().actionGet();
		Logger.debug("Delete Response: %s", response);

	}

}
