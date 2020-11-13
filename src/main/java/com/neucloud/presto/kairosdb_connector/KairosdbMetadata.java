package com.neucloud.presto.kairosdb_connector;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.neucloud.presto.kairosdb_connector.util.KairosdbUtil;

public class KairosdbMetadata implements ConnectorMetadata {

	private static final Logger log = LoggerFactory.getLogger(KairosdbMetadata.class);

	private static KairosdbMetadata single;

	private static String connectorId;
	public static KairosdbMetadata getInstance(String catalogName) {
		if (single == null) {
			single = new KairosdbMetadata(catalogName);
		}
		return single;
	}

	private KairosdbMetadata(String catalogName) {
		connectorId = new KairosdbConnectorId(catalogName).toString();
	}

	public List<String> listSchemaNames(ConnectorSession session) {
		ArrayList<String> schemaList = new ArrayList<String>();
		schemaList.add(KairosdbNameSpace.SCAHEMA);
		return schemaList;
	}

	public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
		return new KairosdbTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
	}

	public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table,
			Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
		KairosdbTableHandle tableHandle = (KairosdbTableHandle) table;
		ConnectorTableLayout layout = new ConnectorTableLayout(new KairosdbTableLayoutHandle(tableHandle));
		return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
	}

	public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
		List<SchemaTableName> listTable = new ArrayList<SchemaTableName>();
		for (String table : KairosdbUtil.getTableName()) {
			if (!table.startsWith("kairosdb")) {
				listTable.add(new SchemaTableName(KairosdbNameSpace.SCAHEMA, table));
			}
		}
		return listTable;
	}

	public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
		return new ConnectorTableLayout(handle);
	}

	public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
		KairosdbTableHandle kairosdbTable = (KairosdbTableHandle) table;
		checkArgument(kairosdbTable.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
		List<ColumnMetadata> list = KairosdbUtil.getColumn(kairosdbTable.getTableName());
		SchemaTableName tableName = new SchemaTableName(kairosdbTable.getSchemaName(), kairosdbTable.getTableName());
		return new ConnectorTableMetadata(tableName, list);
	}

	public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
		KairosdbTableHandle kairosdbTable = (KairosdbTableHandle) tableHandle;
		log.debug("调用getColumnHandles");
		checkArgument(kairosdbTable.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
		
		Map<String, ColumnHandle> map = new HashMap<String, ColumnHandle>();
		List<ColumnMetadata> list = KairosdbUtil.getColumn(kairosdbTable.getTableName());
		for (int i = 0; i < list.size(); i++) {
			ColumnMetadata meta = list.get(i);
			log.debug("准备meta： Name：{}， Type：{}", meta.getName(),meta.getType());
			map.put(meta.getName(), new KairosdbColumnHandle(connectorId, meta.getName(), meta.getType(), i));
		}
		return map;
	}

	public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
			SchemaTablePrefix prefix) {
		requireNonNull(prefix, "prefix is null");
		Map<SchemaTableName, List<ColumnMetadata>> columns = new HashMap<SchemaTableName, List<ColumnMetadata>>();
		Optional<String> optional = null;
		List<SchemaTableName> list = listTables(session, optional);
		for (SchemaTableName table : list) {
			if (table.getTableName().startsWith(prefix.getTableName())) {
				columns.put(table, KairosdbUtil.getColumn(table.getTableName()));
			}
		}
		return columns;
	}

	public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
			ColumnHandle columnHandle) {
		return ((KairosdbColumnHandle) columnHandle).getColumnMetadata();
	}

	public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
		KairosdbTableHandle table = (KairosdbTableHandle) tableHandle;
		KairosdbUtil.deleteTable(table.getTableName());
	}

}
