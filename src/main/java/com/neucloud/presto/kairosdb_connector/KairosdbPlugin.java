package com.neucloud.presto.kairosdb_connector;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

public class KairosdbPlugin implements Plugin {

	public Iterable<ConnectorFactory> getConnectorFactories() {
		return ImmutableList.of(new KairosdbConnectorFactory());
	}

}
