package com.neucloud.presto.kairosdb_connector;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

public class KairosdbRecordSetProvider implements ConnectorRecordSetProvider {

	private static Logger log = LoggerFactory.getLogger(KairosdbRecordSetProvider.class);

	private static KairosdbRecordSetProvider single;

	public static KairosdbRecordSetProvider getInstance() {
		log.debug("初始化KairosdbRecordSetProvider");
		if (single == null) {
			single = new KairosdbRecordSetProvider();
		}
		return single;
	}

	@Override
	public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
			ConnectorSplit split, List<? extends ColumnHandle> columns) {
		KairosdbSplit kairosdbSplit = (KairosdbSplit) split;
		log.debug("------------------------KairosdbRecordSetProvider:{}", kairosdbSplit.getTableName());
			
		ImmutableList.Builder<KairosdbColumnHandle> handles = ImmutableList.builder();
		for (ColumnHandle handle : columns) {
			KairosdbColumnHandle kairosdbHandle = (KairosdbColumnHandle) handle;
			log.debug("------------------------handleName:{},handleType:{}", kairosdbHandle.getColumnName(),kairosdbHandle.getColumnType());
			handles.add(kairosdbHandle);
		}

		return new KairosdbRecordSet(kairosdbSplit, handles.build());
	}

}
