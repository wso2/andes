package org.wso2.andes.store.cassandra.dao;

public class CassandraHelper {
	
	public enum WHERE_OPERATORS{
		EQ,
		LT,
		LTE,
		GT,
		GTE,
		ASC,
		DESC,
		IN,FILTERING,LIMIT;
		
	}
	
	public static class ColumnValueMap{
		private final String column;
		private final Object value;
		private final WHERE_OPERATORS operator;
		
		public ColumnValueMap(String column, Object value, WHERE_OPERATORS operator) {
			super();
			this.column = column;
			this.value = value;
			this.operator = operator;
		}

		public String getColumn() {
			return column;
		}

		public Object getValue() {
			return value;
		}

		public WHERE_OPERATORS getOperator() {
			return operator;
		}
		
		
		
	}

}
