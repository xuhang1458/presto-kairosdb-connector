package com.neucloud.presto.kairosdb_connector;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class KairosdbHandleResolver implements ConnectorHandleResolver {

	@Override
	public Class<? extends ConnectorTableHandle> getTableHandleClass() {
		return KairosdbTableHandle.class;
	}

	@Override
	public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
		return KairosdbTableLayoutHandle.class;
	}

	@Override
	public Class<? extends ColumnHandle> getColumnHandleClass() {
		return KairosdbColumnHandle.class;
	}

	@Override
	public Class<? extends ConnectorSplit> getSplitClass() {
		return KairosdbSplit.class;
	}

	@Override
	public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
		return KairosdbTransactionHandle.class;
	}

	
	

}
