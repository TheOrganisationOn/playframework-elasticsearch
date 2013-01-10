package play.modules.elasticsearch.client;

import org.elasticsearch.client.Client;

import play.modules.elasticsearch.mapping.ModelMapper;

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

}
