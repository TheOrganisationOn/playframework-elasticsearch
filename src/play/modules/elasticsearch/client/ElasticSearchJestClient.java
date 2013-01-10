package play.modules.elasticsearch.client;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.client.config.ClientConstants;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

import java.util.LinkedHashSet;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

import play.Logger;
import play.db.Model;
import play.modules.elasticsearch.mapping.ModelMapper;
import play.modules.elasticsearch.search.SearchResults;

import com.google.common.collect.Lists;

public class ElasticSearchJestClient implements ElasticSearchClientInterface {

	private JestClient jestClient;

	@Override
	public void start() {
		ClientConfig clientConfig = new ClientConfig();
		LinkedHashSet<String> servers = new LinkedHashSet<String>();
		// TODO get from conf
		servers.add("http://localhost:9200");
		clientConfig.getServerProperties().put(ClientConstants.SERVER_LIST, servers);

		JestClientFactory factory = new JestClientFactory();
		factory.setClientConfig(clientConfig);
		jestClient = factory.getObject();
	}

	@Override
	public Client getIntenalClient() {
		throw new IllegalArgumentException("no transport client in jest impl");
	}

	@Override
	public void deleteIndex(String indexName) {
		try {
			JestResult jestResult = jestClient.execute(new DeleteIndex(indexName));
			logIfNotSucceeded("creating index " + indexName, jestResult);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void createIndex(String indexName) {
		try {
			JestResult jestResult = jestClient.execute(new CreateIndex(indexName));
			logIfNotSucceeded("creating index " + indexName, jestResult);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void logIfNotSucceeded(String additionalInfo, JestResult jestResult) {
		if (jestResult.isSucceeded() == false) {
			Logger.warn("error when %s : $%s", additionalInfo, jestResult.getJsonString());
		}
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
		return new SearchResults<T>(0, (List<T>) Lists.newArrayList(), null);
	}

	@Override
	public void indexDocument(String indexName, String typeName, String documentId, String documentJson) {
		// TODO Auto-generated method stub

	}

}
