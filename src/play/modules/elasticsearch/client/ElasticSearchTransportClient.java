package play.modules.elasticsearch.client;

import static org.elasticsearch.node.NodeBuilder.*;

import java.util.Enumeration;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import play.Logger;
import play.Play;
import play.db.Model;
import play.modules.elasticsearch.ElasticSearch;
import play.modules.elasticsearch.Query;
import play.modules.elasticsearch.mapping.MappingUtil;
import play.modules.elasticsearch.mapping.ModelMapper;
import play.modules.elasticsearch.search.SearchResults;
import play.modules.elasticsearch.util.ExceptionUtil;

public class ElasticSearchTransportClient implements ElasticSearchClientInterface {

	private Client client;

	@Override
	public void start() {
		Builder settings = getNativeSettings();
		if (this.isLocalMode()) {
			Logger.info("Starting Elastic Search for Play! in Local Mode");
			final NodeBuilder nb = nodeBuilder().settings(settings).local(true).client(false).data(true);
			final Node node = nb.node();
			client = node.client();

		} else {
			Logger.info("Connecting Play! to Elastic Search in Client Mode");
			final TransportClient c = new TransportClient(settings);
			if (Play.configuration.getProperty("elasticsearch.client") == null) {
				throw new RuntimeException(
						"Configuration required - elasticsearch.client when local model is disabled!");
			}
			final String[] hosts = getHosts().trim().split(",");
			boolean done = false;
			for (final String host : hosts) {
				final String[] parts = host.split(":");
				if (parts.length != 2) {
					throw new RuntimeException("Invalid Host: " + host);
				}
				Logger.info("Transport Client - Host: %s Port: %s", parts[0], parts[1]);
				if (Integer.valueOf(parts[1]) == 9200) {
					Logger.info("Note: Port 9200 is usually used by the HTTP Transport. You might want to use 9300 instead.");
				}
				c.addTransportAddress(new InetSocketTransportAddress(parts[0], Integer.valueOf(parts[1])));
				done = true;
			}
			if (done == false) {
				throw new RuntimeException("No Hosts Provided for Elastic Search!");
			}
			client = c;
		}
	}

	/**
	 * Gets the hosts.
	 * 
	 * @return the hosts
	 */
	public static String getHosts() {
		final String s = Play.configuration.getProperty("elasticsearch.client");
		if (s == null) {
			return "";
		}
		return s;
	}

	/**
	 * Checks if is local mode.
	 * 
	 * @return true, if is local mode
	 */
	private boolean isLocalMode() {
		try {
			final String client = Play.configuration.getProperty("elasticsearch.client");
			final Boolean local = Boolean.getBoolean(Play.configuration.getProperty("elasticsearch.local", "true"));

			if (client == null) {
				return true;
			}
			if (client.equalsIgnoreCase("false") || client.equalsIgnoreCase("true")) {
				return true;
			}

			return local;
		} catch (final Exception e) {
			Logger.error("Error! Starting in Local Model: %s", ExceptionUtil.getStackTrace(e));
			return true;
		}
	}

	private Builder getNativeSettings() {
		// Start Node Builder
		final Builder settings = ImmutableSettings.settingsBuilder();
		// settings.put("client.transport.sniff", true);

		// Import anything from play configuration that starts with elasticsearch.native.
		Enumeration<Object> keys = Play.configuration.keys();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			if (key.startsWith("elasticsearch.native.")) {
				String nativeKey = key.replaceFirst("elasticsearch.native.", "");
				Logger.error("Adding native [" + nativeKey + "," + Play.configuration.getProperty(key) + "]");
				settings.put(nativeKey, Play.configuration.getProperty(key));
			}
		}
		return settings;
	}

	@Override
	public Client getIntenalClient() {
		return client;
	}

	@Override
	public void deleteIndex(String indexName) {
		try {
			client.admin().indices().prepareDelete(indexName).execute().actionGet();
		} catch (IndexMissingException e) {
			// TODO what to do with exceptions from underlaying api
		}
	}

	@Override
	public void createIndex(String indexName) {
		try {
			Logger.debug("Starting Elastic Search Index %s", indexName);
			CreateIndexResponse response = client.admin().indices().create(new CreateIndexRequest(indexName))
					.actionGet();
			Logger.debug("Response: %s", response);
		} catch (IndexAlreadyExistsException iaee) {
			Logger.debug("Index already exists: %s", indexName);
		} catch (Throwable t) {
			Logger.warn(ExceptionUtil.getStackTrace(t));
		}
	}

	@Override
	public void createType(String indexName, String typeName, ModelMapper<?> mapper) {
		try {
			Logger.debug("Create Elastic Search Type %s/%s", indexName, typeName);
			PutMappingRequest request = Requests.putMappingRequest(indexName).type(typeName);
			XContentBuilder mapping = MappingUtil.getMapping(mapper);
			Logger.debug("Type mapping: \n %s", mapping.string());
			request.source(mapping);
			PutMappingResponse response = client.admin().indices().putMapping(request).actionGet();
			Logger.debug("Response: %s", response);

		} catch (IndexAlreadyExistsException iaee) {
			Logger.debug("Index already exists: %s", indexName);

		} catch (Throwable t) {
			Logger.warn(ExceptionUtil.getStackTrace(t));
		}
	}

	@Override
	public void refreshAllIndexes() {
		try {
			client.admin().indices().refresh(new RefreshRequest()).get();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <T extends Model> SearchResults<T> searchAndHydrateAll(QueryBuilder queryBuilder, Class<T> clazz) {
		long numberOfResults = ElasticSearch.searchAndHydrate(queryBuilder, clazz).totalCount;
		Query<T> search = ElasticSearch.query(queryBuilder, clazz);
		search.hydrate(true);
		search.size((int) numberOfResults);
		return search.fetch();
	}

}
