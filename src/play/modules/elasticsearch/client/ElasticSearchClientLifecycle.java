package play.modules.elasticsearch.client;

import org.elasticsearch.client.Client;

public class ElasticSearchClientLifecycle {

	ElasticSearchClientInterface elasticClient;

	public boolean started() {
		return elasticClient != null;
	}

	public void start(boolean useTransportClient) {
		if (useTransportClient) {
			elasticClient = new ElasticSearchTransportClient();
		} else {
			elasticClient = null;
			// todo Jest impl;
		}
		elasticClient.start();
	}

	public boolean notStarted() {
		return started() == false;
	}

	// TODO transitive implementation
	public Client getTransportClient() {
		return elasticClient.getIntenalClient();
	}

}
