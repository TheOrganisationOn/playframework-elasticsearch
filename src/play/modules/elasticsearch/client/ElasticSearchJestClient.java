package play.modules.elasticsearch.client;

import org.elasticsearch.client.Client;

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

}
