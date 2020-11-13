package com.neucloud.presto.kairosdb_connector;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class KairosdbColumnHandle implements ColumnHandle {
	
	private final String columnName;
	private final Type columnType;
	private final int ordinalPosition;
	private final String connectorId;

	
	@JsonCreator
	public KairosdbColumnHandle(
			@JsonProperty("connectorId") String connectorId,
			@JsonProperty("columnName") String columnName, 
			@JsonProperty("columnType") Type columnType,
			@JsonProperty("ordinalPosition") int ordinalPosition) {
		
		
		this.ordinalPosition = ordinalPosition;
		this.connectorId = requireNonNull(connectorId, "connectorId is null");
		this.columnType = requireNonNull(columnType, "type is null");
		this.columnName = requireNonNull(columnName, "columnName is null");
	}

	@JsonProperty
	public String getConnectorId() {
		return connectorId;
	}

	@JsonProperty
	public int getOrdinalPosition() {
		return ordinalPosition;
	}

	@JsonProperty
	public String getColumnName() {
		return columnName;
	}
	
	@JsonProperty
	public Type getColumnType() {
		return columnType;
	}

	public ColumnMetadata getColumnMetadata() {
		return new ColumnMetadata(columnName, columnType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(connectorId, columnName);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if ((obj == null) || (getClass() != obj.getClass())) {
			return false;
		}

		KairosdbColumnHandle other = (KairosdbColumnHandle) obj;
		return Objects.equals(this.connectorId, other.connectorId) && Objects.equals(this.columnName, other.columnName);
	}

	@Override
	public String toString() {
		return toStringHelper(this).add("connectorId", connectorId).add("columnName", columnName)
				.add("columnType", columnType).add("ordinalPosition", ordinalPosition).toString();
	}
}
