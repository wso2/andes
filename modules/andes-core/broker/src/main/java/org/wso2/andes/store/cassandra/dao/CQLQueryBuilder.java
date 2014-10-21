/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.store.cassandra.dao;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.store.cassandra.dao.CassandraHelper.ColumnValueMap;
import org.wso2.andes.store.cassandra.dao.CassandraHelper.WHERE_OPERATORS;
import org.wso2.andes.server.store.util.CassandraDataAccessException;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.datastax.driver.core.querybuilder.Update.Where;

public class CQLQueryBuilder {

	private static Log log = LogFactory.getLog(CQLQueryBuilder.class);
	public static final String WITH = "WITH";

	public static String buildTableQuery(Table table) throws CassandraDataAccessException {

		StringBuilder columns = new StringBuilder();
		Map<String, DataType> columnType = table.getColumnType();
		Set<String> primaryKeys = table.getPrimaryKeys();

		boolean hasColumns = columnType.isEmpty() ? false : true;
		boolean hasPrimaryKeys = primaryKeys.isEmpty() ? false : true;
		String storageType = table.getStorageType() == null ? "" : WITH + table.getStorageType();
		if (hasColumns) {
			columns.append(" ( ");
			Set<Entry<String, DataType>> entries = columnType.entrySet();
			int count = 0;
			for (Entry<String, DataType> entry : entries) {
				++count;
				columns.append(entry.getKey()).append("  ")
						.append(entry.getValue().toString().toLowerCase());
				if (count < entries.size()) {
					columns.append(",");
				}
			}
		}

		if (hasColumns && hasPrimaryKeys) {
			columns.append(", PRIMARY KEY (");
			int count = 0;
			for (String primaryKey : primaryKeys) {
				++count;
				columns.append(primaryKey);
				if (count < primaryKeys.size()) {
					columns.append(",");
				}
			}
			columns.append(" ) ");
		}

		if (hasColumns) {
			columns.append(" ) ");
		}
        String sql;
        if(table.getStorageType()==null){
             sql = "CREATE TABLE " + table.getKeySpace() + "." + table.getName()
                    + columns.toString() + "WITH gc_grace_seconds = "+table.getGcGraceSeconds() + ";";
        }else{
		     sql = "CREATE TABLE " + table.getKeySpace() + "." + table.getName()
				+ columns.toString() + storageType + " AND gc_grace_seconds = "+table.getGcGraceSeconds()+";";
        }

		return sql;

	}

	public static Select buildSelect(CQLQueryBuilder.CqlSelect cqlSelect) {
		String table = cqlSelect.getTable();
		Set<String> columns = cqlSelect.getColumns();
		List<ColumnValueMap> whereClause = cqlSelect.getWhereClause();
		boolean filtering = cqlSelect.isAllowFilter();
		long limit = cqlSelect.getLimit();

		Selection selection = QueryBuilder.select();
		if (columns == null || columns.isEmpty()) {
			selection.countAll();
		} else {
			for (String column : columns) {
				selection.column(column);
			}
		}

		Select select = selection.from(table);
		com.datastax.driver.core.querybuilder.Select.Where where = select.where();

		if(whereClause != null){
			for (ColumnValueMap cv : whereClause) {
				Clause clause = null;
				switch (cv.getOperator()) {
				case EQ:
					clause = eq(cv.getColumn(), cv.getValue());

					break;
				case LT:
					clause = lt(cv.getColumn(), cv.getValue());

					break;
				case GT:
					clause = gt(cv.getColumn(), cv.getValue());

					break;
				case GTE:
					clause = gte(cv.getColumn(), cv.getValue());

					break;
				case LTE:
					clause = lte(cv.getColumn(), cv.getValue());

					break;
				case IN:
					Object obj = cv.getValue();
					if(obj instanceof String[]){
						clause = in(cv.getColumn(), (String[])cv.getValue());
					}else if(obj instanceof long[]){
						clause = in(cv.getColumn(), (long[])cv.getValue());
					}else if(obj instanceof int[]){
						clause = in(cv.getColumn(), (int[])cv.getValue());
					}

					break;
				}

				if (clause != null) {
					where.and(clause);
				}

			}
		}
		

		List<Ordering> orderBy = new ArrayList<Ordering>();

		if (cqlSelect.getAscendingColumns() != null) {
			for(String name : cqlSelect.getAscendingColumns()){
				orderBy.add(QueryBuilder.asc(name));
			}
		}

		if (cqlSelect.getDescendingColumns() != null) {
			for(String name : cqlSelect.getDescendingColumns()){
				orderBy.add(QueryBuilder.desc(name));
			}
		}

		if (!orderBy.isEmpty()) {
			select.orderBy(orderBy.toArray(new Ordering[orderBy.size()]));
		}

		if (filtering) {
			select.allowFiltering();
		}

		if (limit > 0) {
			if(limit == Long.MAX_VALUE){
				limit = Integer.MAX_VALUE;
			}
			select.limit((int)limit);
		}

		return select;
	}

	/*
	 * public static Update buildSingleUpdate(String keySpace, String table,
	 * Map<String, Object> keyValueMap, Map<String, Object> whereClause) { if
	 * (keyValueMap.isEmpty()) { log.info(" no any key value to update table " +
	 * keySpace + "." + table); return null; }
	 * 
	 * Update update =
	 * com.datastax.driver.core.querybuilder.QueryBuilder.update(
	 * keySpace.toLowerCase(), table.toLowerCase()); Assignments assignments =
	 * update.with(); Where where = update.where();
	 * 
	 * Set<Entry<String, Object>> keyValueSet = keyValueMap.entrySet();
	 * Iterator<Entry<String, Object>> iter = keyValueSet.iterator(); while
	 * (iter.hasNext()) { Entry<String, Object> entry = iter.next(); Assignment
	 * assignment = set(entry.getKey(), entry.getValue());
	 * assignments.and(assignment); }
	 * 
	 * Set<Entry<String, Object>> whereClauseSet = whereClause.entrySet(); iter
	 * = whereClauseSet.iterator(); while (iter.hasNext()) { Entry<String,
	 * Object> entry = iter.next(); Clause clause = eq(entry.getKey(),
	 * entry.getValue()); where.and(clause); } return update; }
	 */

	public static Update buildSingleUpdate(CQLQueryBuilder.CqlUpdate cqlUpdate) {
		String table = cqlUpdate.getTable();
		String keyspace = cqlUpdate.getKeyspace();
		List<ColumnValueMap> whereClause = cqlUpdate.getWhereClause();
		Map<String, Object> keyValueMap = cqlUpdate.getKeyValueMap();

		Update update = com.datastax.driver.core.querybuilder.QueryBuilder.update(keyspace, table);
		Assignments assignments = update.with();
		Where where = update.where();

		Set<Entry<String, Object>> keyValueSet = keyValueMap.entrySet();
		Iterator<Entry<String, Object>> iter = keyValueSet.iterator();
		while (iter.hasNext()) {
			Entry<String, Object> entry = iter.next();
			Assignment assignment = set(entry.getKey(), entry.getValue());
			assignments.and(assignment);
		}

		if(whereClause != null){
			for (ColumnValueMap cv : whereClause) {
				Clause clause = null;
				switch (cv.getOperator()) {
				case EQ:
					clause = eq(cv.getColumn(), cv.getValue());

					break;
				case LT:
					clause = lt(cv.getColumn(), cv.getValue());

					break;
				case GT:
					clause = gt(cv.getColumn(), cv.getValue());

					break;
				case GTE:
					clause = gte(cv.getColumn(), cv.getValue());

					break;
				case LTE:
					clause = lte(cv.getColumn(), cv.getValue());

					break;
				case IN:
					Object obj = cv.getValue();
					if(obj instanceof String[]){
						clause = in(cv.getColumn(), (String[])cv.getValue());
					}else if(obj instanceof long[]){
						clause = in(cv.getColumn(), (long[])cv.getValue());
					}else if(obj instanceof int[]){
						clause = in(cv.getColumn(), (int[])cv.getValue());
					}

					break;
				}

				if (clause != null) {
					where.and(clause);
				}

			}
		}
		

		return update;
	}

	public static Insert buildSingleInsert(String keySpace, String table,
			Map<String, Object> keyValueMap) {
		if (keyValueMap.isEmpty()) {
			log.info(" no any key value to insert into table " + keySpace + "." + table);
			return null;
		}
		Insert insert = insertInto(keySpace, table);

		Set<Entry<String, Object>> keyValueSet = keyValueMap.entrySet();
		Iterator<Entry<String, Object>> iter = keyValueSet.iterator();
		while (iter.hasNext()) {
			Entry<String, Object> entry = iter.next();
			insert.value(entry.getKey(), entry.getValue());
		}

		return insert;
	}

	public static Delete buildSingleDelete(CQLQueryBuilder.CqlDelete cqlDelete) {
		String table = cqlDelete.getTable();
		String keyspace = cqlDelete.getKeyspace();
		Set<String> columns = cqlDelete.getColumns();
		List<ColumnValueMap> whereClause = cqlDelete.getWhereClause();

		Delete.Selection selection = QueryBuilder.delete();
		if (columns == null || columns.isEmpty()) {
			selection.all();
		} else {
			for (String column : columns) {
				selection.column(column);
			}
		}

		Delete delete = selection.from(keyspace, table);
		com.datastax.driver.core.querybuilder.Delete.Where where = delete.where();

		if(whereClause != null){
			for (ColumnValueMap cv : whereClause) {
				Clause clause = null;
				switch (cv.getOperator()) {
				case EQ:
					clause = eq(cv.getColumn(), cv.getValue());

					break;
				case LT:
					clause = lt(cv.getColumn(), cv.getValue());

					break;
				case GT:
					clause = gt(cv.getColumn(), cv.getValue());

					break;
				case GTE:
					clause = gte(cv.getColumn(), cv.getValue());

					break;
				case LTE:
					clause = lte(cv.getColumn(), cv.getValue());

					break;
				case IN:
					Object obj = cv.getValue();
					if(obj instanceof String[]){
						clause = in(cv.getColumn(), (String[])cv.getValue());
					}else if(obj instanceof long[]){
						clause = in(cv.getColumn(), (long[])cv.getValue());
					}else if(obj instanceof int[]){
						clause = in(cv.getColumn(), (int[])cv.getValue());
					}

					break;
				}

				if (clause != null) {
					where.and(clause);
				}

			}
		}
		

		return delete;
	}

	public static class Table {

		private final Map<String, DataType> columnType = new HashMap<String, DataType>();
		private final Set<String> primaryKeys = new HashSet<String>();
		private final String storageType;
		private final String name;
		private final String keySpace;
        private final int gcGraceSeconds;

		public Table(String storageType, String name, String keySpace,int gcGraceSeconds) {
			super();
			this.storageType = storageType;
			this.name = name.toLowerCase();
			this.keySpace = keySpace.toLowerCase();
            this.gcGraceSeconds = gcGraceSeconds;
		}

		public Map<String, DataType> getColumnType() {
			return columnType;
		}

		public String getStorageType() {
			return storageType;
		}

		public Set<String> getPrimaryKeys() {
			return primaryKeys;
		}

		public String getName() {
			return name;
		}

		public String getKeySpace() {
			return keySpace;
		}

        public int getGcGraceSeconds(){
            return gcGraceSeconds;
        }

	}

	public static class CqlSelect {
		private Set<String> columns;
		private List<ColumnValueMap> whereClause;
		private long limit = 0;
		private boolean allowFilter = false;
		private String table;
		private Set<String> ascendingColumns;
		private Set<String> descendingColumns;

		public CqlSelect(String table, long limit, boolean allowFilter) {
			super();
			this.limit = limit;
			this.allowFilter = allowFilter;
			this.table = table.toLowerCase();
		}

		public CqlSelect addCondition(String name, Object value, WHERE_OPERATORS operator) {
			if (whereClause == null) {
				whereClause = new ArrayList<ColumnValueMap>();
			}
			whereClause.add(new ColumnValueMap(name, value, operator));
			return this;
		}

		public CqlSelect addColumn(String name) {
			if (columns == null) {
				columns = new HashSet<String>();
			}
			columns.add(name);
			return this;
		}

		public CqlSelect addAscending(String column) {
			if (ascendingColumns == null) {
				ascendingColumns = new HashSet<String>();
			}
			ascendingColumns.add(column);
			return this;
		}

		public CqlSelect addDescending(String column) {
			if (descendingColumns == null) {
				descendingColumns = new HashSet<String>();
			}
			descendingColumns.add(column);
			return this;
		}

		public Set<String> getColumns() {
			return columns;
		}

		public List<ColumnValueMap> getWhereClause() {
			return whereClause;
		}

		public long getLimit() {
			return limit;
		}

		public boolean isAllowFilter() {
			return allowFilter;
		}

		public String getTable() {
			return table;
		}

		public final Set<String> getAscendingColumns() {
			return ascendingColumns;
		}

		public final Set<String> getDescendingColumns() {
			return descendingColumns;
		}

	}

	public static class CqlDelete {
		private Set<String> columns;
		private List<ColumnValueMap> whereClause;
		private String keyspace;
		private String table;

		public CqlDelete(String keyspace, String table) {
			super();
			this.table = table.toLowerCase();
			this.keyspace = keyspace.toLowerCase();
		}

		public CqlDelete addCondition(String name, Object value, WHERE_OPERATORS operator) {
			if (whereClause == null) {
				whereClause = new ArrayList<ColumnValueMap>();
			}
			whereClause.add(new ColumnValueMap(name, value, operator));
			return this;
		}

		public CqlDelete addColumn(String name) {
			if (columns == null) {
				columns = new HashSet<String>();
			}
			columns.add(name);
			return this;
		}

		public Set<String> getColumns() {
			return columns;
		}

		public List<ColumnValueMap> getWhereClause() {
			return whereClause;
		}

		public String getTable() {
			return table;
		}

		public final String getKeyspace() {
			return keyspace;
		}

	}

	public static class CqlUpdate {
		private List<ColumnValueMap> whereClause;
		private Map<String, Object> keyValueMap;
		private Map<String, Object> counterColumnValue;
		private String keyspace;
		private String table;

		public CqlUpdate(String keyspace, String table) {
			super();
			this.table = table.toLowerCase();
			this.keyspace = keyspace.toLowerCase();
		}

		public CqlUpdate addCondition(String name, Object value, WHERE_OPERATORS operator) {
			if (whereClause == null) {
				whereClause = new ArrayList<ColumnValueMap>();
			}
			whereClause.add(new ColumnValueMap(name, value, operator));
			return this;
		}

		public CqlUpdate addColumnAndValue(String key, Object value) {
			if (keyValueMap == null) {
				keyValueMap = new HashMap<String, Object>();
			}
			keyValueMap.put(key, value);
			return this;
		}

		public CqlUpdate addCounterColumnAndValue(String key, Object value) {
			if (counterColumnValue == null) {
				counterColumnValue = new HashMap<String, Object>();
			}
			counterColumnValue.put(key, value);
			return this;
		}

		public List<ColumnValueMap> getWhereClause() {
			return whereClause;
		}

		public String getTable() {
			return table;
		}

		public final String getKeyspace() {
			return keyspace;
		}

		public final Map<String, Object> getKeyValueMap() {
			return keyValueMap;
		}

		public final Map<String, Object> getCounterColumnValue() {
			return counterColumnValue;
		}

	}

	public static byte[] convertToByteArray(Row row, String columnName) {
		java.nio.ByteBuffer bb = row.getBytes(columnName);
		byte[] actualData = new byte[bb.remaining()];
		bb.get(actualData);
		return actualData;
	}
}
