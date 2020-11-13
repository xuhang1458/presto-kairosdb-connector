package com.neucloud.presto.kairosdb_connector;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.Objects;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

public class KairosdbTableHandle implements ConnectorTableHandle {

	private String schemaName;
	private String tableName;
	private String connectorId;

	@JsonCreator
	public KairosdbTableHandle(@JsonProperty("connectorId") String connectorId,
			@JsonProperty("schemaName") String schemaName, @JsonProperty("tableName") String tableName) {
		this.schemaName = requireNonNull(schemaName, "schemaName is null");
		this.tableName = requireNonNull(tableName, "tableName is null");
		this.connectorId = requireNonNull(connectorId, "connectorId is null");
	}

	@JsonProperty
	public String getConnectorId() {
		return connectorId;
	}

	@JsonProperty
	public String getSchemaName() {
		return schemaName;
	}

	@JsonProperty
	public String getTableName() {
		return tableName;
	}

	public SchemaTableName toSchemaTableName() {
		return new SchemaTableName(schemaName, tableName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(connectorId, schemaName, tableName);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if ((obj == null) || (getClass() != obj.getClass())) {
			return false;
		}

		KairosdbTableHandle other = (KairosdbTableHandle) obj;
		return Objects.equals(this.connectorId, other.connectorId) && Objects.equals(this.schemaName, other.schemaName)
				&& Objects.equals(this.tableName, other.tableName);
	}

	@Override
	public String toString() {
		return Joiner.on(":").join(connectorId, schemaName, tableName);
	}

}
