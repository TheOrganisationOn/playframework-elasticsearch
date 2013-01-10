package play.modules.elasticsearch.client;

import org.elasticsearch.client.Client;

public class ElasticSearchClientLifecycle {

	ElasticSearchClientInterface elasticClient;

	public boolean started() {
		return elasticClient != null;
	}

	public void start() {
		if (configuredToUseTransportClient()) {
			elasticClient = new ElasticSearchTransportClient();
		} else {
			// todo Jest impl;
		}
		elasticClient.start();
	}

	private boolean configuredToUseTransportClient() {
		// TODO get from play conf
		return true;
	}

	public boolean notStarted() {
		return started() == false;
	}

	// TODO transitive implementation
	public Client getTransportClient() {
		return elasticClient.getIntenalClient();
	}

}
