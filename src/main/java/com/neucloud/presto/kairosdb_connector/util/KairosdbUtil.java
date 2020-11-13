package com.neucloud.presto.kairosdb_connector.util;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.QueryTagBuilder;
import org.kairosdb.client.builder.grouper.TagGrouper;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.QueryResult;
import org.kairosdb.client.response.QueryTagResponse;
import org.kairosdb.client.response.Result;
import org.kairosdb.client.response.TagQueryResult;
import org.kairosdb.client.response.TagResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;

public class KairosdbUtil {

	private static Logger log = LoggerFactory.getLogger(KairosdbUtil.class);
	
	public static HttpClient client;

	public static void instance(String url) throws MalformedURLException {
		client = new HttpClient(url);
	}

	public static List<ColumnMetadata> getColumn(String tableName) {
		log.debug("获取Column， table：{}",tableName);
		ArrayList<ColumnMetadata> list = new ArrayList<ColumnMetadata>();
		QueryTagBuilder builder = QueryTagBuilder.getInstance();
		builder.setStart(new Date(0l)).addMetric(tableName);
		QueryTagResponse response = client.queryTags(builder);
		for (TagQueryResult tagQueryResult : response.getQueries()) {
			for (TagResult tagResult : tagQueryResult.getResults()) {
				Map<String, List<String>> map = tagResult.getTags();
				for (String tag : map.keySet()) {
					list.add(new ColumnMetadata(tag, VarcharType.createUnboundedVarcharType()));
				}
			}
		}
		list.add(new ColumnMetadata("timeStamp", BigintType.BIGINT));
		list.add(new ColumnMetadata("value", DoubleType.DOUBLE));
		return list;
	}

	public static Iterator<KairosdbRow> select(String tableName) {
		List<KairosdbRow> list = new ArrayList<KairosdbRow>();
		List<ColumnMetadata> columns = getColumn(tableName);
		List<String> tags = new ArrayList<String>();
		for (ColumnMetadata column : columns) {
			tags.add(column.getName());
		}

		QueryBuilder builder = QueryBuilder.getInstance();
		builder.setStart(new Date(0l)).addMetric(tableName).addGrouper(new TagGrouper(tags));
		QueryResponse response = client.query(builder);
		for (QueryResult queryResult : response.getQueries()) {
			for (Result result : queryResult.getResults()) {
				for (DataPoint dataPoint : result.getDataPoints()) {
					long timestamp = dataPoint.getTimestamp();
					String value = dataPoint.getValue().toString();
					HashMap<String, String> columnMap = new HashMap<String, String>();
					for (Map.Entry<String, List<String>> entry : result.getTags().entrySet()) {
						String tagKey = entry.getKey();
						String tagValue = entry.getValue().get(0);
						columnMap.put(tagKey, tagValue);
					}
					KairosdbRow row = new KairosdbRow(timestamp, value, columnMap);
					list.add(row);
				}

			}
		}

		return list.iterator();
	}

	public static List<String> getTableName() {
		List<String> list = new ArrayList<String>();
		List<String> metricNames = client.getMetricNames();
		for (String metricName : metricNames) {
			list.add(metricName);
		}
		return list;
	}

	public static void deleteTable(String tableName) {
		client.deleteMetric(tableName);
	}

}
