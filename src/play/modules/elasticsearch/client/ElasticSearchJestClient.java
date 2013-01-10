package play.modules.elasticsearch.client;

import io.searchbox.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.client.config.ClientConstants;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

import java.util.LinkedHashSet;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

import play.Logger;
import play.db.Model;
import play.modules.elasticsearch.ElasticSearchPlugin;
import play.modules.elasticsearch.Query;
import play.modules.elasticsearch.client.jest.IdOnly;
import play.modules.elasticsearch.client.jest.JestQuery;
import play.modules.elasticsearch.mapping.ModelMapper;
import play.modules.elasticsearch.search.SearchResults;
import play.modules.elasticsearch.transformer.JPATransformer;

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

		ModelMapper<T> mapper = ElasticSearchPlugin.getMapper(clazz);
		String index = mapper.getIndexName();
		Search search = new Search(Search.createQueryWithBuilder(queryBuilder.toString()));
		search.addIndex(index);
		try {
			JestResult result = jestClient.execute(search);
			List<IdOnly> withIds = result.getSourceAsObjectList(IdOnly.class);
			List<Object> ids = Lists.newArrayList();
			for (IdOnly withId : withIds) {
				System.err.println("got object with id " + withId.id);
				ids.add(withId.id);
			}
			if (ids.isEmpty() == false) {
				List<T> modelObjects = JPATransformer.loadFromDb(clazz, ids);
				// TODO sort by order
				return new SearchResults<T>(modelObjects.size(), modelObjects, null);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new SearchResults<T>(0L, Lists.<T> newArrayList(), null);
	}

	@Override
	public void indexDocument(String indexName, String typeName, String documentId, String documentJson) {
		Index index = new Index.Builder(documentJson).index(indexName).type(typeName).id(documentId)
				.build();
		tryToExecute(index, "indexing in " + indexName + " a doc " + documentJson);
	}

	private void tryToExecute(Action action, String additionalInfoIfNotSucceeded) {
		try {
			JestResult result = jestClient.execute(action);
			logIfNotSucceeded(additionalInfoIfNotSucceeded, result);
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
	public <T extends Model> Query<T> createQuery(QueryBuilder query, Class<T> clazz) {
		return new JestQuery<T>(clazz, query);
	}

}
