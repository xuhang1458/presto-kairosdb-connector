package com.neucloud.presto.kairosdb_connector;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

public class KairosdbConnectorId {
	private final String id;

	public KairosdbConnectorId(String id) {
		this.id = requireNonNull(id, "id is null");
	}

	@Override
	public String toString() {
		return id;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if ((obj == null) || (getClass() != obj.getClass())) {
			return false;
		}

		KairosdbConnectorId other = (KairosdbConnectorId) obj;
		return Objects.equals(this.id, other.id);
	}

}
