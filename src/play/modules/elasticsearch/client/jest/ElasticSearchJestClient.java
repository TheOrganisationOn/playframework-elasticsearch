package play.modules.elasticsearch.client.jest;

import io.searchbox.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.client.config.ClientConstants;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

import java.util.LinkedHashSet;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

import play.Logger;
import play.db.Model;
import play.modules.elasticsearch.Query;
import play.modules.elasticsearch.client.ElasticSearchClientInterface;
import play.modules.elasticsearch.mapping.ModelMapper;
import play.modules.elasticsearch.search.SearchResults;

public class ElasticSearchJestClient implements ElasticSearchClientInterface {

	private JestClient jestClient;

	@Override
	public void start() {
		ClientConfig clientConfig = new ClientConfig();
		LinkedHashSet<String> servers = new LinkedHashSet<String>();
		// TODO get from conf
		servers.add("http://localhost:9200");
		clientConfig.getServerProperties().put(ClientConstants.SERVER_LIST, servers);
		clientConfig.getClientFeatures().put(ClientConstants.IS_MULTI_THREADED, true);

		JestClientFactory factory = new JestClientFactory();
		factory.setClientConfig(clientConfig);
		jestClient = factory.getObject();
	}

	@Override
	public Client getInternalClient() {
		throw new IllegalArgumentException("no transport client in jest impl");
	}

	@Override
	public void deleteIndex(String indexName) {
		tryToExecute(new DeleteIndex(indexName), "deleting index " + indexName);
	}

	@Override
	public void createIndex(String indexName) {
		tryToExecute(new CreateIndex(indexName), "creating index " + indexName);
	}

	@Override
	public void createType(String indexName, String typeName, ModelMapper<?> mapper) {
		// TODO Auto-generated method stub

	}

	@Override
	public void refreshAllIndexes() {
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends Model> SearchResults<T> searchAndHydrateAll(QueryBuilder queryBuilder, Class<T> clazz) {
		return createQuery(queryBuilder, clazz).fetch();
	}

	@Override
	public void indexDocument(String indexName, String typeName, String documentId, String documentJson) {
		Index index = new Index.Builder(documentJson).index(indexName).type(typeName).id(documentId)
				.build();
		tryToExecute(index, "indexing in " + indexName + " a doc " + documentJson);
	}

	private void tryToExecute(Action action, String additionalInfoIfNotSucceeded) {
		tryToExecute(action, additionalInfoIfNotSucceeded, this.jestClient);
	}

	public static JestResult tryToExecute(Action action, String additionalInfoIfNotSucceeded, JestClient jestClient) {
		try {
			System.err.println("is executing in jest " + action);
			JestResult result = jestClient.execute(action);
			logIfNotSucceeded(additionalInfoIfNotSucceeded, result);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	private static void logIfNotSucceeded(String additionalInfo, JestResult jestResult) {
		if (jestResult.isSucceeded() == false) {
			Logger.warn("error when %s : $%s", additionalInfo, jestResult.getJsonString());
		}
	}

	@Override
	public <T extends Model> Query<T> createQuery(QueryBuilder query, Class<T> clazz) {
		return new JestQuery<T>(clazz, query, jestClient);
	}

}
