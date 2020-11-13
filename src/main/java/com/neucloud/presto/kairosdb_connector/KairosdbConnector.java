package com.neucloud.presto.kairosdb_connector;

import java.net.MalformedURLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.neucloud.presto.kairosdb_connector.util.KairosdbUtil;

public class KairosdbConnector implements Connector {

	private static Logger log = LoggerFactory.getLogger(KairosdbConnector.class);

	private final KairosdbMetadata metadata;
	private final KairosdbSplitManager splitManager;
	private final KairosdbRecordSetProvider recordSetProvider;

	public KairosdbConnector(String url, String catalogName) {
		log.debug("初始化Connector");
		try {
			KairosdbUtil.instance(url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		this.metadata = KairosdbMetadata.getInstance(catalogName);
		this.splitManager = KairosdbSplitManager.getInstance();
		this.recordSetProvider = KairosdbRecordSetProvider.getInstance();
	}

	@Override
	public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
		return KairosdbTransactionHandle.INSTANCE;
	}

	@Override
	public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
		return metadata;
	}

	@Override
	public ConnectorSplitManager getSplitManager() {
		return splitManager;
	}
	
	@Override
	public ConnectorRecordSetProvider getRecordSetProvider() {
		return recordSetProvider;
	}


}
