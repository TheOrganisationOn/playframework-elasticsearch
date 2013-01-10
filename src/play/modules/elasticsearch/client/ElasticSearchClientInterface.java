package play.modules.elasticsearch.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

import play.db.Model;
import play.modules.elasticsearch.Query;
import play.modules.elasticsearch.mapping.ModelMapper;
import play.modules.elasticsearch.search.SearchResults;

public interface ElasticSearchClientInterface {

	void start();

	Client getInternalClient();

	void deleteIndex(String indexName);

	void createIndex(String indexName);

	void createType(String indexName, String typeName, ModelMapper<?> mapper);

	void refreshAllIndexes();

	<T extends Model> SearchResults<T> searchAndHydrateAll(QueryBuilder queryBuilder, Class<T> clazz);

	void indexDocument(String indexName, String typeName, String documentId, String documentJson);

	<T extends Model> Query<T> createQuery(QueryBuilder query, Class<T> clazz);

}
