package play.modules.elasticsearch.client;

import org.elasticsearch.client.Client;

import play.modules.elasticsearch.mapping.ModelMapper;

public interface ElasticSearchClientInterface {

	void start();

	Client getIntenalClient();

	void deleteIndex(String indexName);

	void createIndex(String indexName);

	void createType(String indexName, String typeName, ModelMapper<?> mapper);

}
