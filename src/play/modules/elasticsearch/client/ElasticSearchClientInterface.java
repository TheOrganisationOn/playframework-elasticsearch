package play.modules.elasticsearch.client;

import org.elasticsearch.client.Client;

public interface ElasticSearchClientInterface {

	void start();

	Client getIntenalClient();

	void deleteIndex(String indexName);

}
