package com.neucloud.presto.kairosdb_connector;

import java.util.Map;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

public class KairosdbConnectorFactory implements ConnectorFactory {

	public String getName() {
		return "KairosdbConnector";
	}

	public ConnectorHandleResolver getHandleResolver() {
		return new KairosdbHandleResolver();
	}

	public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
		String url = config.get("url");
		KairosdbConnector connector = new KairosdbConnector(url, catalogName);
		return connector;
	}

}
