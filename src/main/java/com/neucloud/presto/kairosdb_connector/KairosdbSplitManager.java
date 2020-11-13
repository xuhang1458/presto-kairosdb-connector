package com.neucloud.presto.kairosdb_connector;

import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class KairosdbSplitManager implements ConnectorSplitManager {

	public static KairosdbSplitManager single;

	public static KairosdbSplitManager getInstance() {
		if (single == null) {
			single = new KairosdbSplitManager();
		}
		return single;
	}

	private KairosdbSplitManager() {
	}

	public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
			ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext) {
		KairosdbTableLayoutHandle layoutHandle = (KairosdbTableLayoutHandle) layout;
		KairosdbTableHandle tableHandle = layoutHandle.getTable();

		List<ConnectorSplit> splits = new ArrayList<>();
		splits.add(new KairosdbSplit(tableHandle.getSchemaName(), tableHandle.getTableName()));

		return new FixedSplitSource(splits);
	}

}
