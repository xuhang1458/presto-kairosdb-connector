package com.neucloud.presto.kairosdb_connector.util;

import java.util.HashMap;

public class KairosdbRow {

	private HashMap<String, String> columnMap;

	public KairosdbRow() {
	}

	public KairosdbRow(long timestamp, String value, HashMap<String, String> columnMap) {
		super();
		this.columnMap = columnMap;
		this.columnMap.put("timestamp", timestamp + "");
		this.columnMap.put("value", value);
	}

	public HashMap<String, String> getColumnMap() {
		return columnMap;
	}

	public void setColumnMap(HashMap<String, String> columnMap) {
		this.columnMap = columnMap;
	}

}
