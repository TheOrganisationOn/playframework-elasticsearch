package play.modules.elasticsearch.client;

import static org.elasticsearch.node.NodeBuilder.*;

import java.util.Enumeration;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import play.Logger;
import play.Play;
import play.modules.elasticsearch.ElasticSearchPlugin;
import play.modules.elasticsearch.util.ExceptionUtil;

public class ElasticSearchTransportClient implements ElasticSearchClientInterface {

	private Client client;

	@Override
	public void start() {
		Builder settings = getNativeSettings();
		if (this.isLocalMode()) {
			Logger.info("Starting Elastic Search for Play! in Local Mode");
			final NodeBuilder nb = nodeBuilder().settings(settings).local(true).client(false).data(true);
			final Node node = nb.node();
			client = node.client();

		} else {
			Logger.info("Connecting Play! to Elastic Search in Client Mode");
			final TransportClient c = new TransportClient(settings);
			if (Play.configuration.getProperty("elasticsearch.client") == null) {
				throw new RuntimeException(
						"Configuration required - elasticsearch.client when local model is disabled!");
			}
			final String[] hosts = getHosts().trim().split(",");
			boolean done = false;
			for (final String host : hosts) {
				final String[] parts = host.split(":");
				if (parts.length != 2) {
					throw new RuntimeException("Invalid Host: " + host);
				}
				Logger.info("Transport Client - Host: %s Port: %s", parts[0], parts[1]);
				if (Integer.valueOf(parts[1]) == 9200) {
					Logger.info("Note: Port 9200 is usually used by the HTTP Transport. You might want to use 9300 instead.");
				}
				c.addTransportAddress(new InetSocketTransportAddress(parts[0], Integer.valueOf(parts[1])));
				done = true;
			}
			if (done == false) {
				throw new RuntimeException("No Hosts Provided for Elastic Search!");
			}
			client = c;
		}
	}

	/**
	 * Gets the hosts.
	 * 
	 * @return the hosts
	 */
	public static String getHosts() {
		final String s = Play.configuration.getProperty("elasticsearch.client");
		if (s == null) {
			return "";
		}
		return s;
	}

	/**
	 * Checks if is local mode.
	 * 
	 * @return true, if is local mode
	 */
	private boolean isLocalMode() {
		try {
			final String client = Play.configuration.getProperty("elasticsearch.client");
			final Boolean local = Boolean.getBoolean(Play.configuration.getProperty("elasticsearch.local", "true"));

			if (client == null) {
				return true;
			}
			if (client.equalsIgnoreCase("false") || client.equalsIgnoreCase("true")) {
				return true;
			}

			return local;
		} catch (final Exception e) {
			Logger.error("Error! Starting in Local Model: %s", ExceptionUtil.getStackTrace(e));
			return true;
		}
	}

	private Builder getNativeSettings() {
		// Start Node Builder
		final Builder settings = ImmutableSettings.settingsBuilder();
		// settings.put("client.transport.sniff", true);

		// Import anything from play configuration that starts with elasticsearch.native.
		Enumeration<Object> keys = Play.configuration.keys();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			if (key.startsWith("elasticsearch.native.")) {
				String nativeKey = key.replaceFirst("elasticsearch.native.", "");
				Logger.error("Adding native [" + nativeKey + "," + Play.configuration.getProperty(key) + "]");
				settings.put(nativeKey, Play.configuration.getProperty(key));
			}
		}
		return settings;
	}

	@Override
	public Client getIntenalClient() {
		return client;
	}

	@Override
	public void deleteIndex(String indexName) {
		try {
			ElasticSearchPlugin.client().admin().indices().prepareDelete(indexName).execute().actionGet();
		} catch (IndexMissingException e) {
			// TODO what to do with exceptions from underlaying api
		}
	}

}
