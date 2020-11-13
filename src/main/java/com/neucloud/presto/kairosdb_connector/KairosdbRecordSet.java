package com.neucloud.presto.kairosdb_connector;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

public class KairosdbRecordSet implements RecordSet {

	
	private Logger log = LoggerFactory.getLogger(KairosdbRecordSet.class);
	private final List<KairosdbColumnHandle> columnHandles;
	private final List<Type> columnTypes;
	private final KairosdbSplit split;

	public KairosdbRecordSet(KairosdbSplit split, List<KairosdbColumnHandle> columnHandles) {
		log.debug("KairosdbRecordSet-------------");
		this.split = requireNonNull(split, "split is null");
		;
		this.columnHandles = requireNonNull(columnHandles, "column handles is null");
		ImmutableList.Builder<Type> types = ImmutableList.builder();
		for (KairosdbColumnHandle column : columnHandles) {
			types.add(column.getColumnType());
		}
		this.columnTypes = types.build();

	}

	@Override
	public List<Type> getColumnTypes() {
		return columnTypes;
	}

	@Override
	public RecordCursor cursor() {
		log.debug("cursor-------------");
		return new KairosdbRecordCursor(columnHandles, split);
	}

}
