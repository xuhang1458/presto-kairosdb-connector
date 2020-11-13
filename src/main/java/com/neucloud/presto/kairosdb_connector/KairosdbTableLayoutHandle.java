package com.neucloud.presto.kairosdb_connector;

import java.util.Objects;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class KairosdbTableLayoutHandle implements ConnectorTableLayoutHandle {

	private final KairosdbTableHandle table;

	@JsonCreator
	public KairosdbTableLayoutHandle(@JsonProperty("table") KairosdbTableHandle table) {
		this.table = table;
	}

	@JsonProperty
	public KairosdbTableHandle getTable() {
		return table;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		KairosdbTableLayoutHandle that = (KairosdbTableLayoutHandle) o;
		return Objects.equals(table, that.table);
	}

	@Override
	public int hashCode() {
		return Objects.hash(table);
	}

	@Override
	public String toString() {
		return table.toString();
	}
}
