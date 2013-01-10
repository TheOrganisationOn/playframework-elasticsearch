package play.modules.elasticsearch.client;

import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

import play.db.Model;
import play.modules.elasticsearch.mapping.ModelMapper;
import play.modules.elasticsearch.search.SearchResults;

import com.google.common.collect.Lists;

public class ElasticSearchJestClient implements ElasticSearchClientInterface {

	@Override
	public void start() {

	}

	@Override
	public Client getIntenalClient() {
		return null;
	}

	@Override
	public void deleteIndex(String indexName) {

	}

	@Override
	public void createIndex(String indexName) {
		// TODO Auto-generated method stub

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

}
