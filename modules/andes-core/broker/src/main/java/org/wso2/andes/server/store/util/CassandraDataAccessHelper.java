/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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
package org.wso2.andes.server.store.util;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.CounterSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceCounterQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.wso2.andes.server.store.CassandraConsistencyLevelPolicy;

/**
 * Class <code>CassandraDataAccessHelper</code> Encapsulate the Cassandra DataAccessLogic used in
 * CassandraMessageStore
 */
public class CassandraDataAccessHelper {


    private static final String USERNAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";


    /**
     * Serializes used for Cassandra data operations
     */
    private static StringSerializer stringSerializer = StringSerializer.get();

    private static LongSerializer longSerializer = LongSerializer.get();

    private static BytesArraySerializer bytesArraySerializer = BytesArraySerializer.get();

    private static IntegerSerializer integerSerializer = IntegerSerializer.get();

    private static ByteBufferSerializer byteBufferSerializer = ByteBufferSerializer.get();

/*    public static int safeLongToInt(long l) {
        if(l < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        } else if( l > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) l;
        }
    }*/

    /**
     * Create a Cassandra Cluster instance given the connection details
     * @param userName  userName to connect to the cassandra
     * @param password  password to connect to cassandra
     * @param clusterName Cluster name
     * @param connectionString cassandra connection string
     * @return  Cluster instance
     * @throws CassandraDataAccessException   In case of and error in accessing database or data error
     */
    public static Cluster createCluster(String userName, String password, String clusterName,
                                         String connectionString) throws CassandraDataAccessException {

        if(userName == null || password == null) {
            throw new CassandraDataAccessException("Can't create cluster with empty userName or Password");
        }

        if(clusterName == null) {
            throw new CassandraDataAccessException("Can't create cluster with empty cluster name");
        }

        if(connectionString == null) {
            throw new CassandraDataAccessException("Can't create cluster with empty connection string");
        }

        Map<String, String> credentials = new HashMap<String, String>();
        credentials.put(USERNAME_KEY, userName);
        credentials.put(PASSWORD_KEY, password);

        CassandraHostConfigurator hostConfigurator =
                new CassandraHostConfigurator(connectionString);
        hostConfigurator.setMaxActive(2000);
        Cluster cluster = HFactory.getCluster(clusterName);

        if (cluster == null) {
            cluster = HFactory.createCluster(clusterName, hostConfigurator, credentials);
        }
        return cluster;
    }

    /**
     * Create a keySpace in a given cluster
     * @param cluster Cluster where keySpace should be created
     * @param keySpace name of the KeySpace
     * @return  Keyspace
     */
    public static Keyspace createKeySpace(Cluster cluster , String keySpace, int replicationFactor,
                                          String strategyClass) {
        Keyspace keyspace;

        //Define the keySpace
        ThriftKsDef thriftKsDef = new ThriftKsDef(keySpace);
        thriftKsDef.setReplicationFactor(replicationFactor);
        if(strategyClass == null || strategyClass.isEmpty()){
            strategyClass = ThriftKsDef.DEF_STRATEGY_CLASS;
        }
        thriftKsDef.setStrategyClass(strategyClass);

        KeyspaceDefinition def = cluster.describeKeyspace(keySpace);
        if (def == null) {
            //Adding keySpace to the cluster
            cluster.addKeyspace(thriftKsDef,true);
        }

        keyspace = HFactory.createKeyspace(keySpace, cluster);
        CassandraConsistencyLevelPolicy policy = new CassandraConsistencyLevelPolicy();
        keyspace.setConsistencyLevelPolicy(policy);
        return keyspace;
    }

    /**
     * Create a Column family in a Given Cluster instance
     * @param name  ColumnFamily Name
     * @param keySpace KeySpace name
     * @param cluster   Cluster instance
     * @param comparatorType Comparator
     * @throws CassandraDataAccessException   In case of an Error accessing database or data error
     */
    public static void createColumnFamily(String name, String keySpace, Cluster cluster, String comparatorType) throws CassandraDataAccessException {

        KeyspaceDefinition ksDef = cluster.describeKeyspace(keySpace);

        if (ksDef == null) {
            throw new CassandraDataAccessException("Can't create Column family, keyspace " + keySpace +
                    " does not exist");
        }

        ColumnFamilyDefinition cfDef =
                new ThriftCfDef(keySpace, /*"Queue"*/name,
                        ComparatorType.getByClassName(comparatorType));

        List<ColumnFamilyDefinition> cfDefsList = ksDef.getCfDefs();
        HashSet<String> cfNames = new HashSet<String>();
        for (ColumnFamilyDefinition columnFamilyDefinition : cfDefsList) {
            cfNames.add(columnFamilyDefinition.getName());
        }
        if (!cfNames.contains(name)) {
            cluster.addColumnFamily(cfDef,true);
        }


    }

    /**
     * Create a Column family for cassandra counters in a given Cluster intance
     * @param name  ColumnFamily Name
     * @param keySpace  KeySpace name
     * @param cluster   Cluster instance
     * @throws CassandraDataAccessException  In case of an Error accessing database or data error
     */
    public static void createCounterColumnFamily(String name, String keySpace, Cluster cluster) throws CassandraDataAccessException{
        KeyspaceDefinition ksDef = cluster.describeKeyspace(keySpace);

        if (ksDef == null) {
            throw new CassandraDataAccessException("Can't create Column family, keyspace " + keySpace +
                    " does not exist");
        }
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keySpace,name, ComparatorType.COUNTERTYPE);
        cfDef.setComparatorType(ComparatorType.UTF8TYPE);
        cfDef.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName());
        cfDef.setColumnType(ColumnType.STANDARD);

        List<ColumnFamilyDefinition> cfDefsList = ksDef.getCfDefs();
        HashSet<String> cfNames = new HashSet<String>();
        for (ColumnFamilyDefinition columnFamilyDefinition : cfDefsList) {
            cfNames.add(columnFamilyDefinition.getName());
        }
        if (!cfNames.contains(name)) {
            cluster.addColumnFamily(cfDef,true);
        }
    }

    /**
     * Insert a raw-column for counting a property (row is property, column is item)
     * @param cfName column family name
     * @param counterRowName  name of row
     * @param queueColumn name of column
     * @param keyspace key space name
     * @throws CassandraDataAccessException
     */
    public static void insertCounterColumn(String cfName, String counterRowName,String queueColumn, Keyspace keyspace)
            throws CassandraDataAccessException {
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
            // inserting counter column
            mutator.insertCounter(counterRowName, cfName,HFactory.createCounterColumn(queueColumn, 0L,StringSerializer.get()));
            mutator.execute();
            CounterQuery<String, String> counter = new ThriftCounterColumnQuery<String, String>(
                    keyspace, stringSerializer, stringSerializer);

            counter.setColumnFamily(cfName).setKey(counterRowName).setName(queueColumn);
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while inserting data to:" + cfName ,e);
        }
    }

    /**
     * remove allocated raw-column space for counter
     * @param cfName  column family name
     * @param counterRowName name of row
     * @param queueColumn name of column
     * @param keyspace key space name
     * @throws CassandraDataAccessException
     */
    public static void removeCounterColumn(String cfName, String counterRowName,String queueColumn, Keyspace keyspace) throws CassandraDataAccessException {
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
            mutator.deleteCounter(counterRowName, cfName,queueColumn,stringSerializer);
            mutator.execute();
            CounterQuery<String, String> counter = new ThriftCounterColumnQuery<String, String>(
                    keyspace, stringSerializer, stringSerializer);

            counter.setColumnFamily(cfName).setKey(counterRowName).setName(queueColumn);
        } catch (Exception e) {
            if (e.getMessage().contains("All host pools marked down. Retry burden pushed out to client")) {
                throw new CassandraDataAccessException("Unable to remove counter column as cassandra connection is down");
            } else {
                throw new CassandraDataAccessException("Error while accessing:" + cfName ,e);
            }
        }
    }

    /**
     * Increment counter by given value
     * @param rawID  raw name
     * @param columnFamily  column family name
     * @param columnName name of column
     * @param keyspace keyspace
     * @param incrementBy value to increase by
     * @throws CassandraDataAccessException
     */
    public static void incrementCounter(String columnName ,String columnFamily, String rawID, Keyspace keyspace, long incrementBy)
            throws CassandraDataAccessException {
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,StringSerializer.get());
            mutator.incrementCounter(rawID, columnFamily, columnName, incrementBy);
            mutator.execute();
        }  catch (Exception e) {
            throw new CassandraDataAccessException("Error while accessing:" + columnFamily ,e);
        }
    }

    /**
     * Decrement counter by a given value
     * @param rawID  raw name
     * @param columnFamily  column family name
     * @param columnName  name of column
     * @param keyspace   keyspace
     * @throws CassandraDataAccessException
     */
    public static void decrementCounter(String columnName ,String columnFamily, String rawID, Keyspace keyspace, long decrementBy)
            throws CassandraDataAccessException {
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,StringSerializer.get());
            mutator.decrementCounter(rawID, columnFamily, columnName, decrementBy);
            mutator.execute();
        } catch (HectorException he) {
            if (he.getMessage().contains("All host pools marked down. Retry burden pushed out to client")) {
                throw new CassandraDataAccessException("Unable to remove active subscribers as cassandra connection is down");
            } else {
                throw new CassandraDataAccessException("Error while accessing:" + columnFamily ,he);
            }
        }
    }

    /**
     * Get the value of a counter
     * @param keyspace  name of key space
     * @param columnFamily column family name
     * @param key   key value (property)
     * @param cloumnName   column name (item)
     * @return long count value
     * @throws CassandraDataAccessException
     */
    public static long getCountValue(Keyspace keyspace, String columnFamily, String cloumnName, String key)
            throws CassandraDataAccessException {
        try {
            long count =0;
            CounterQuery<String, String> query = HFactory.createCounterColumnQuery(keyspace, stringSerializer,stringSerializer);
            query.setColumnFamily(columnFamily).setKey(key).setName(cloumnName);
            HCounterColumn<String> counter = query.execute().get();
            if(counter != null){
               count = counter.getValue();
            }
            return count;
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while accessing:" + columnFamily ,e);
        }
    }

    /**
     * Get a list of column names in a given row of a counter column family
     * @param columnFamilyName
     * @param rowName
     * @param keyspace
     * @return List<String> column names
     * @throws CassandraDataAccessException
     */
/*    public static List<String> getColumnNameListForCounterColumnFamily(String columnFamilyName, String rowName, Keyspace keyspace)
            throws CassandraDataAccessException {

        ArrayList<String> rowList = new ArrayList<String>();

        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if(columnFamilyName == null || rowName == null) {
            throw new CassandraDataAccessException("Can't access data with queueType =" + columnFamilyName +
                    " and rowName=" + rowName);
        }

        try {

            SliceCounterQuery<String, String> sliceQuery = HFactory.createCounterSliceQuery(keyspace, stringSerializer, stringSerializer);
            sliceQuery.setKey(rowName);
            sliceQuery.setColumnFamily(columnFamilyName);
            sliceQuery.setRange("", "", false, Integer.MAX_VALUE);

            QueryResult<CounterSlice<String>> result = sliceQuery.execute();
            CounterSlice<String> columnSlice = result.get();
            for (HCounterColumn<String> column : columnSlice.getColumns()) {
                rowList.add(column.getName());
            }
        } catch (Exception e) {
            if (e.getMessage().contains("All host pools marked down. Retry burden pushed out to client")) {
                throw new CassandraDataAccessException("Error while accessing data from :" + columnFamilyName + " as cassandra connection is down");
            }else {
                throw new CassandraDataAccessException("Error while accessing data from :" + columnFamilyName ,e);
            }

        }
        return rowList;
    }*/
    /**
     * Get list of column names in a Given row in a Cassandra Column Family (row,column and value are of STRING type)
     * @param columnFamilyName Name of the column Family
     * @param rowName  Row name
     * @param keyspace  keySpace
     * @return  List of string in that given row.
     * @throws CassandraDataAccessException   In case of database access error or data error
     */
/*    public static List<String> getColumnNameList(String columnFamilyName, String rowName, Keyspace keyspace) throws CassandraDataAccessException {
        ArrayList<String> columnNamesList = new ArrayList<String>();

        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if(columnFamilyName == null || rowName == null) {
            throw new CassandraDataAccessException("Can't access data with queueType =" + columnFamilyName +
                    " and rowName=" + rowName);
        }

        try {
            SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer,
                    stringSerializer, stringSerializer);
            sliceQuery.setKey(rowName);
            sliceQuery.setColumnFamily(columnFamilyName);
            sliceQuery.setRange("", "", false, 10000);

            QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
            ColumnSlice<String, String> columnSlice = result.get();
            for (HColumn<String, String> column : columnSlice.getColumns()) {
                columnNamesList.add(column.getName());
            }
        } catch (Exception e) {
            if (e.getMessage().contains("All host pools marked down. Retry burden pushed out to client")) {
                throw new CassandraDataAccessException("Error while accessing data from :" + columnFamilyName + " as cassandra connection is down");
            }else {
                throw new CassandraDataAccessException("Error while accessing data from :" + columnFamilyName ,e);
            }
        }
        return columnNamesList;
    }*/

      /**
     * Get the value of a given column in a given row in a Cassandra Column Family (row,column,value are of STRING type)
     * @param columnFamilyName Name of the column Family
     * @param rowName  Row name
     * @param keyspace  keySpace
     * @param columnName Name of the column
     * @return vlaue of the column in the row.
     * @throws CassandraDataAccessException   In case of database access error or data error
     */
/*    public static List<String> getColumnValuesOfRow(String columnFamilyName, String rowName,Keyspace keyspace, String columnName) throws CassandraDataAccessException {
        List<String> valueList = new ArrayList<String>();
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if(columnFamilyName == null || rowName == null) {
            throw new CassandraDataAccessException("Can't access data with queueType =" + columnFamilyName +
                    " and rowName=" + rowName);
        }

        try {
            SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer,
                    stringSerializer, stringSerializer);
            sliceQuery.setKey(rowName);
            sliceQuery.setColumnFamily(columnFamilyName);
            sliceQuery.setRange("", "", false, 10000);
            sliceQuery.setColumnNames(columnName);

            QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
            ColumnSlice<String, String> columnSlice = result.get();
            for (HColumn<String, String> column : columnSlice.getColumns()) {
               if(column.getName().equalsIgnoreCase(columnName)){
                  valueList.add(column.getValue());
               }
            }
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while accessing data from :" + columnFamilyName ,e);
        }
        return valueList;
    }*/


    public static byte[] getMessageMetaDataFromQueue(String columnFamilyName, Keyspace keyspace,
                                                     long messageId) throws CassandraDataAccessException {
        byte[] value = null;
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if(columnFamilyName == null ) {
            throw new CassandraDataAccessException("Can't access data with queueType = " + columnFamilyName);
        }

        try {
            RangeSlicesQuery<String, Long, byte[]> sliceQuery =
                    HFactory.createRangeSlicesQuery(keyspace, stringSerializer, longSerializer, bytesArraySerializer);
            sliceQuery.setRange(null,null,false,Integer.MAX_VALUE);
            sliceQuery.setKeys(null,null);
            sliceQuery.setColumnNames(messageId);
            sliceQuery.setColumnFamily(columnFamilyName);


            QueryResult<OrderedRows<String, Long, byte[]>> result = sliceQuery.execute();
            Row<String, Long, byte[]> rowSlice = result.get().peekLast();

            if (result == null || result.get().getList().size() == 0) {
                return null;
            }
            ColumnSlice<Long, byte[]> columnSlice = rowSlice.getColumnSlice();
            for (Object column : columnSlice.getColumns()) {
                if (column instanceof HColumn) {
                    value = ((HColumn<Long, byte[]>) column).getValue();
                }
            }
            return value;
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while getting data from " + columnFamilyName,e);
        }
    }

    /**
     * Get set of <ColumnName,ColumnValue> list in a column family with a offset and a maximum cont
     * (row-STRING,column-LONG,Value-ByteArray). Used to get messages
     * form cassandra
     * @param rowName name of row (queue name)
     * @param columnFamilyName ColumnFamilyName
     * @param keyspace  Cassandra KeySpace
     * @param lastProcessedId Last processed Message id to use as a off set
     * @param count  max message count limit
     * @return  ColumnSlice which contain the messages
     * @throws CassandraDataAccessException
     */
    public static ColumnSlice<Long, byte[]> getMessagesFromQueue(String rowName,
                                                            String columnFamilyName, Keyspace keyspace,
                                                            long lastProcessedId,int count) throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if(columnFamilyName == null || rowName == null) {
            throw new CassandraDataAccessException("Can't access data with queueType = " + columnFamilyName +
                    " and queueName=" + rowName);
        }

        try {
            SliceQuery<String, Long, byte[]> sliceQuery =
                    HFactory.createSliceQuery(keyspace, stringSerializer, longSerializer, bytesArraySerializer);
            sliceQuery.setKey(rowName);
            sliceQuery.setRange(lastProcessedId + 1 ,Long.MAX_VALUE,false, count);
            sliceQuery.setColumnFamily(columnFamilyName);


            QueryResult<ColumnSlice<Long, byte[]>> result = sliceQuery.execute();
            ColumnSlice<Long, byte[]> columnSlice = result.get();

            return columnSlice;
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while getting data from " + columnFamilyName,e);
        }
    }

    /**
     * Get set of Column slice <ColumnName,ColumnValue> list in a column family from beginning of row up to given count.
     * Used to get messages from beginning of a queue
     * @param rowName QueueName
     * @param columnFamilyName ColumnFamilyName
     * @param keyspace  Cassandra KeySpace
     * @param count  max message count limit
     * @return  ColumnSlice which contain the messages
     * @throws CassandraDataAccessException
     */
/*    public static ColumnSlice<Long, byte[]> getMessagesFromQueue(String rowName,
                                                            String columnFamilyName, Keyspace keyspace,
                                                            int count) throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if(columnFamilyName == null || rowName == null) {
            throw new CassandraDataAccessException("Can't access data with queueType = " + columnFamilyName +
                    " and queueName=" + rowName);
        }

        try {
            SliceQuery<String, Long, byte[]> sliceQuery =
                    HFactory.createSliceQuery(keyspace, stringSerializer, longSerializer, bytesArraySerializer);
            sliceQuery.setKey(rowName);
            sliceQuery.setRange((long)0,Long.MAX_VALUE,false, count);
            sliceQuery.setColumnFamily(columnFamilyName);


            QueryResult<ColumnSlice<Long, byte[]>> result = sliceQuery.execute();
            ColumnSlice<Long, byte[]> columnSlice = result.get();

            return columnSlice;
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while getting data from " + columnFamilyName,e);
        }
    }*/

    /**
     * get string type values for the given range
     * @param rowName name of row
     * @param columnFamilyName  column family name
     * @param keyspace key space
     * @param startProcessedId range starts from here
     * @param lastProcessedId range ends from here
     * @return  Column slice list
     * @throws CassandraDataAccessException
     */
/*    public static ColumnSlice<Long, String> _getStringTypeValuesForGivenRowWithColumnsFiltered(String rowName,
                                                                 String columnFamilyName, Keyspace keyspace,
                                                                 long startProcessedId, long  lastProcessedId) throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if(columnFamilyName == null || rowName == null) {
            throw new CassandraDataAccessException("Can't access data with queueType = " + columnFamilyName +
                    " and row=" + rowName);
        }

        try {
            SliceQuery<String, Long, String> sliceQuery =
                    HFactory.createSliceQuery(keyspace, stringSerializer, longSerializer, stringSerializer);
            sliceQuery.setKey(rowName);
            long messageCountAsLong = lastProcessedId - startProcessedId;
            int  messageCount = safeLongToInt(messageCountAsLong);
//            sliceQuery.setRange(startProcessedId + 1 ,lastProcessedId ,false, Integer.MAX_VALUE);
            sliceQuery.setRange(startProcessedId + 1 ,lastProcessedId ,false, 200);
            sliceQuery.setColumnFamily(columnFamilyName);

            QueryResult<ColumnSlice<Long, String>> result = sliceQuery.execute();
            ColumnSlice<Long, String> columnSlice = result.get();

            return columnSlice;
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while getting data from " + columnFamilyName,e);
        }
    }*/


    /**
     * Get Number of <String,String> type columns in a given row in a cassandra column family
     * @param rowName row Name we are querying for
     * @param columnFamilyName columnFamilName
     * @param keyspace
     * @param count number of columns the column slice should contain
     * @return
     */
    public static ColumnSlice<String, String> getStringTypeColumnsInARow(String rowName,
                                                                         String columnFamilyName,
                                                                         Keyspace keyspace,int count)
            throws CassandraDataAccessException {

        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if (columnFamilyName == null || rowName == null) {
            throw new CassandraDataAccessException("Can't access data with queueType = " + columnFamilyName +
                    " and rowName=" + rowName);
        }


        try {
            SliceQuery sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer,
                    stringSerializer, stringSerializer);
            sliceQuery.setKey(rowName);
            sliceQuery.setColumnFamily(columnFamilyName);
            sliceQuery.setRange("", "", false, count);

            QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
            ColumnSlice<String, String> columnSlice = result.get();

            return columnSlice;
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while getting data from : " + columnFamilyName,e);
        }
    }

    /**
     * Get a chunk of <Long,String> type columns in a given row in a cassandra column family
     * @param rowName  row name
     * @param columnFamilyName   name of column family
     * @param keyspace key space
     * @param count message count
     * @return Column slice
     * @throws CassandraDataAccessException
     */
/*    public static ColumnSlice<Long, String> getLongTypeColumnsInARow(String rowName,
                                                                         String columnFamilyName,
                                                                         Keyspace keyspace,int count)
            throws CassandraDataAccessException {

        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if (columnFamilyName == null || rowName == null) {
            throw new CassandraDataAccessException("Can't access data with queueType = " + columnFamilyName +
                    " and rowName=" + rowName);
        }


        try {
            SliceQuery sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer,
                    longSerializer, stringSerializer);
            sliceQuery.setKey(rowName);
            sliceQuery.setColumnFamily(columnFamilyName);
            sliceQuery.setRange(0L, Long.MAX_VALUE , false, count);

            QueryResult<ColumnSlice<Long, String>> result = sliceQuery.execute();
            ColumnSlice<Long, String> columnSlice = result.get();

            return columnSlice;
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while getting data from : " + columnFamilyName,e);
        }
    }*/
    /**
     * get <String,String> type columns from lastProcessedID string up to the specified count
     * @param rowName  row name we are querying for
     * @param columnFamilyName column family name
     * @param keyspace  key space involved
     * @param count count of records we wish to receive at maximum
     * @param lastProcessedId
     * @return  ColumnSlice<String, String>
     * @throws CassandraDataAccessException
     */
/*    public static ColumnSlice<String, String> getStringTypeColumnsInARowWithOffset(String rowName,String columnFamilyName,
                                 Keyspace keyspace,int count,String lastProcessedId) throws CassandraDataAccessException {

        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        try {

            SliceQuery sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer,
                    stringSerializer, stringSerializer);
            sliceQuery.setKey(rowName);
            sliceQuery.setColumnFamily(columnFamilyName);
            sliceQuery.setRange(lastProcessedId, "", false, count);

            QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
            ColumnSlice<String, String> columnSlice = result.get();

            return columnSlice;

        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while getting data from " + columnFamilyName,e);
        }
    }*/

    /**
     * get a chuck of <Long,String> type of column slice from lastProcessedID string up to the specified count
     * @param rowName row name
     * @param columnFamilyName column family name
     * @param keyspace key space
     * @param count  num of messages to receive
     * @param lastProcessedId id from which slice should have values
     * @return Column slice
     * @throws CassandraDataAccessException
     */
/*    public static ColumnSlice<Long, String> getLongTypeColumnsInARowWithOffset(String rowName,String columnFamilyName,
                               Keyspace keyspace,int count,long lastProcessedId) throws CassandraDataAccessException {

        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        try {

            SliceQuery sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer,
                    longSerializer, stringSerializer);
            sliceQuery.setKey(rowName);
            sliceQuery.setColumnFamily(columnFamilyName);
            sliceQuery.setRange(lastProcessedId, Long.MAX_VALUE , false, count);

            QueryResult<ColumnSlice<Long, String>> result = sliceQuery.execute();
            ColumnSlice<Long, String> columnSlice = result.get();

            return columnSlice;

        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while getting data from " + columnFamilyName,e);
        }
    }*/

    /**
     * Get a  HColumn<Long, byte[]> with a given key in a given row in a given column Family
     * @param rowName name of the row
     * @param columnFamily column Family name
     * @param key   long type key of the column we are looking for
     * @param keyspace cassandra keySpace instance
     * @return     query result as a cassandra column
     * @throws CassandraDataAccessException  in case of an Error when accessing data
     */
/*    public static HColumn<Long, byte[]> getLongByteArrayColumnInARow(String rowName,String columnFamily,
                                                                        long key,Keyspace keyspace)
            throws CassandraDataAccessException {

        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if (columnFamily == null || rowName == null) {
            throw new CassandraDataAccessException("Can't access data with queueType = " + columnFamily +
                    " and rowName=" + rowName);
        }

        try {
            ColumnQuery columnQuery = HFactory.createColumnQuery(keyspace,
                    stringSerializer, longSerializer, bytesArraySerializer);
            columnQuery.setColumnFamily(columnFamily);
            columnQuery.setKey(rowName);
            columnQuery.setName(key);

            QueryResult<HColumn<Long, byte[]>> result = columnQuery.execute();

            HColumn<Long, byte[]> column = result.get();
            return column;
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while executing quary for HColumn<Long, byte[]> with key =" +
                    key + " in column Family = " + columnFamily,e);
        }

    }*/

    /**
     * Add Message to a Given Queue in Cassandra
     * @param columnFamily ColumnFamily name
     * @param queue  queue name
     * @param messageId  Message id
     * @param message  message in bytes
     * @param keyspace  Cassandra KeySpace
     * @throws CassandraDataAccessException  In case of database access error
     */
/*    public static void addMessageToQueue(String columnFamily , String queue , long messageId ,
                                         byte []message , Keyspace keyspace) throws CassandraDataAccessException {

       if (keyspace == null) {
            throw new CassandraDataAccessException("Can't add Data , no mutator provided ");
        }

        if (columnFamily == null || queue == null || message == null) {
            throw new CassandraDataAccessException("Can't add data with queueType = " + columnFamily +
                    " and queue=" + queue + " message id  = " + messageId + " message = " + message);
        }

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,stringSerializer);
            mutator.addInsertion(queue.trim(), columnFamily,
                    HFactory.createColumn(messageId, message,longSerializer, bytesArraySerializer));
            mutator.execute();

        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while adding message to Queue" ,e);
        }
    }*/


    /**
     * add message to a queue
     * @param columnFamily column family name
     * @param queue name of queue (row name)
     * @param messageId message ID of the message (column name)
     * @param message message content
     * @param mutator mutator to execute the query
     * @param execute whether to execute the query
     * @throws CassandraDataAccessException
     */
    public static void addMessageToQueue(String columnFamily , String queue , long messageId ,
                                         byte []message , Mutator<String> mutator , boolean execute) throws CassandraDataAccessException {

       if (mutator == null) {
            throw new CassandraDataAccessException("Can't add Data , no mutator provided ");
        }

        if (columnFamily == null || queue == null || message == null) {
            throw new CassandraDataAccessException("Can't add data with queueType = " + columnFamily +
                    " and queue=" + queue + " message id  = " + messageId + " message = " + message);
        }

        try {
            mutator.addInsertion(queue.trim(), columnFamily,
                    HFactory.createColumn(messageId, message,longSerializer, bytesArraySerializer));

            if (execute) {
                mutator.execute();
            }

        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while adding message to Queue" ,e);
        }
    }

    /**
     * add <integer,byte[]> column to a given row. Used to write message content to Cassandra
     * @param columnFamily Name of Column Family
     * @param row name of the row
     * @param key column offset
     * @param value byte[] message content to be written
     * @param mutator mutator
     * @param execute whether to execte the mutator
     * @throws CassandraDataAccessException
     */
    public static void addIntegerByteArrayContentToRaw(String columnFamily,String row,int key,
                                                       byte[] value,Mutator<String> mutator,boolean execute)
            throws CassandraDataAccessException {

        if (mutator == null) {
            throw new CassandraDataAccessException("Can't add Data , no Mutator provided ");
        }

        if (columnFamily == null || row == null || value == null) {
            throw new CassandraDataAccessException("Can't add data with queueType = " + columnFamily +
                    " and row=" + row + " key  = " + key + " value = " + value);
        }

        mutator.addInsertion(
                row,
                columnFamily,
                HFactory.createColumn(key, value, integerSerializer, bytesArraySerializer));
        if(execute) {
            mutator.execute();
        }
    }

    /**
     * Add a new Column <long,byte[]> to a given row in a given column family
     * @param columnFamily  column family name
     * @param row  row name
     * @param key  long key value
     * @param value value as a byte array
     * @param keyspace  CassandraKeySpace
     * @throws CassandraDataAccessException
     */
/*    public static void addLongByteArrayColumnToRow(String columnFamily, String row, long key, byte[] value, Keyspace keyspace)
            throws CassandraDataAccessException {

        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't add Data , no keySpace provided ");
        }

        if (columnFamily == null || row == null || value == null) {
            throw new CassandraDataAccessException("Can't add data with queueType = " + columnFamily +
                    " and row=" + row + " key  = " + key + " value = " + value);
        }

        try {
            Mutator<String> messageContentMutator = HFactory.createMutator(keyspace,
                    stringSerializer);
            messageContentMutator.addInsertion(
                    row,
                    columnFamily,
                    HFactory.createColumn(key, value, longSerializer, bytesArraySerializer));
            messageContentMutator.execute();
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while adding new Column <int,byte[]> to cassandra store", e);
        }
    }*/

    /**
     * Add a new Column <int,byte[]> to a given row in a given column family
     * @param columnFamily   column Family name
     * @param row row name
     * @param key key of the column
     * @param value value of the column
     * @param keyspace cassandra KeySpace
     * @throws CassandraDataAccessException
     */
/*    public static void addIntegerByteArrayContentToRaw(String columnFamily,String row,int key,
                                                       byte[]value,
                                                       Keyspace keyspace) throws CassandraDataAccessException {
        if(keyspace == null) {
            throw new CassandraDataAccessException("Can't add Data , no keySpace provided ");
        }

        if(columnFamily == null || row == null || value == null) {
             throw new CassandraDataAccessException("Can't add data with queueType = " + columnFamily +
                    " and row=" + row + " key  = " + key + " value = " + value);
        }

        try {
            Mutator<String> messageContentMutator = HFactory.createMutator(keyspace,
                    stringSerializer);
            messageContentMutator.addInsertion(
                    row,
                    columnFamily,
                    HFactory.createColumn(key, value, integerSerializer, bytesArraySerializer));
            messageContentMutator.execute();
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while adding new Column <int,byte[]> to cassandra store" , e);
        }
    }*/

    /**
     * Add new Column<long,long> to a given row in a given cassandra column family
     * @param columnFamily column family name
     * @param row row name
     * @param key long key value of the column
     * @param value long value of the column
     * @param keyspace  Cassandra KeySpace
     * @throws CassandraDataAccessException
     */
/*    public static void addLongContentToRow(String columnFamily,String row,long key,
                                                       long value,
                                                       Keyspace keyspace) throws CassandraDataAccessException {

        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't add Data , no keySpace provided ");
        }

        if (columnFamily == null || row == null ) {
            throw new CassandraDataAccessException("Can't add data with queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + key);
        }

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            mutator.insert(row, columnFamily,
                    HFactory.createColumn(key, value, longSerializer, longSerializer));
            mutator.execute();
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while adding long content to a row",e);
        }
    }*/

    /**
     * Add string value under a String row and long column. Use mutator to add a batch
     * @param columnFamily name of column family
     * @param row  row key
     * @param cKey column key
     * @param cValue column value
     * @param mutator mutator to be used
     * @param execute whether to execute the mutator
     * @throws CassandraDataAccessException
     */
/*    public static void addStringContentToRow(String columnFamily, String row, long cKey, String cValue,
                                             Mutator<String> mutator, boolean execute) throws CassandraDataAccessException {

        if (mutator == null) {
            throw new CassandraDataAccessException("Can't add Data , no mutator provided ");
        }

        if (columnFamily == null || row == null) {
            throw new CassandraDataAccessException("Can't add data with queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + cKey);
        }

        try {
            mutator.addInsertion(row, columnFamily,
                    HFactory.createColumn(cKey, cValue.trim(), longSerializer, stringSerializer));

            if (execute) {
                mutator.execute();
            }
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error in adding string content to row ", e);
        }
    }*/

    /**
     * Add a <String,String> Mapping to a Given Row in cassandra column family.
     * Mappings are used as search indexes
     * @param columnFamily  columnFamilyName
     * @param row  row name
     * @param cKey  key name for the adding column
     * @param cValue  value for the adding column
     * @param keyspace Cassandra KeySpace
     * @throws CassandraDataAccessException  In case of database access error or data error
     */
    public static void addMappingToRaw(String columnFamily, String row, String cKey, String cValue,
                                       Keyspace keyspace) throws CassandraDataAccessException {

        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't add Data , no KeySpace provided ");
        }

        if (columnFamily == null || row == null || cKey == null) {
            throw new CassandraDataAccessException("Can't add data with queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + cKey);
        }

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            mutator.addInsertion(row, columnFamily,
                    HFactory.createColumn(cKey, cValue.trim(), stringSerializer, stringSerializer));
            mutator.execute();
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while adding a Mapping to row ", e);
        }
    }

    /**
     *  Add an Mapping Entry to a raw in a given column family
     * @param columnFamily ColumnFamily name
     * @param row  row name
     * @param cKey column key
     * @param cValue column value
     * @param mutator mutator
     * @param execute  should we execute the insertion. if false it will just and the insertion to the mutator
     * @throws CassandraDataAccessException  In case of database access error or data error
     */
/*    public static void addMappingToRaw(String columnFamily, String row, String cKey, String cValue,
                                       Mutator<String> mutator, boolean execute)
            throws CassandraDataAccessException {

        if (mutator == null) {
            throw new CassandraDataAccessException("Can't add Data , no mutator provided ");
        }

        if (columnFamily == null || row == null || cKey == null) {
            throw new CassandraDataAccessException("Can't add data with queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + cKey);
        }

        try {
            mutator.addInsertion(row, columnFamily,
                    HFactory.createColumn(cKey, cValue.trim(), stringSerializer, stringSerializer));

            if (execute) {
                mutator.execute();
            }
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while adding a Mapping to row ", e);
        }
    }*/

    /**
     * add a column having name of type Long to a given row
     * @param columnFamily name of column family
     * @param row  row name
     * @param cKey column name
     * @param cValue column value
     * @param mutator mutator
     * @param execute whether to execute
     * @throws CassandraDataAccessException
     */
/*    public static void addLongTypeMappingToRaw(String columnFamily, String row, long cKey, String cValue,
                                       Mutator<String> mutator, boolean execute)
            throws CassandraDataAccessException {

        if (mutator == null) {
            throw new CassandraDataAccessException("Can't add Data , no mutator provided ");
        }

        if (columnFamily == null || row == null) {
            throw new CassandraDataAccessException("Can't add data with queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + cKey);
        }

        try {
            mutator.addInsertion(row, columnFamily,
                    HFactory.createColumn(cKey, cValue.trim(), longSerializer, stringSerializer));

            if (execute) {
                mutator.execute();
            }
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while adding a Mapping to row ", e);
        }
    }*/

    /**
     * Delete a given string column in a raw in a column family
     * @param columnFamily  column family name
     * @param row  row name
     * @param key  key name
     * @param keyspace cassandra keySpace
     * @throws CassandraDataAccessException  In case of database access error or data error
     */
    public static void deleteStringColumnFromRaw(String columnFamily,String row, String key ,Keyspace keyspace)
            throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't delete Data , no keyspace provided ");
        }

        if (columnFamily == null || row == null || key == null) {
            throw new CassandraDataAccessException("Can't delete data in queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + key);
        }

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            mutator.addDeletion(row, columnFamily, key, stringSerializer);
            mutator.execute();
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while deleting " + key + " from " + columnFamily + " as cassandra connection is down");

        }
    }


    /**
     * Delete a given string column in a raw in a column family
     * @param columnFamily  column family name
     * @param row  row name
     * @param key  key name
     * @param keyspace cassandra keySpace
     * @throws CassandraDataAccessException  In case of database access error or data error
     */
/*    public static void deleteLongColumnFromRaw(String columnFamily,String row, long key ,Keyspace keyspace)
            throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't delete Data , no keyspace provided ");
        }

        if (columnFamily == null || row == null ) {
            throw new CassandraDataAccessException("Can't delete data in queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + key);
        }

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            mutator.addDeletion(row, columnFamily, key, longSerializer);
            mutator.execute();
        } catch (Exception e) {
           throw new CassandraDataAccessException("Error while deleting " + key + " from " + columnFamily);
        }
    }*/

    /**
     * Delete a given long column in a raw in a column family.
     * @param columnFamily name of column family
     * @param row name of row
     * @param key column key
     * @param mutator mutator
     * @param execute whether to execute the mutator
     * @throws CassandraDataAccessException
     */
     public static void deleteLongColumnFromRaw(String columnFamily, String row, long key,
                                               Mutator<String> mutator, boolean execute) throws CassandraDataAccessException {


        if (mutator == null) {
            throw new CassandraDataAccessException("Can't delete Data , no mutator provided ");
        }

        if (columnFamily == null || row == null) {
            throw new CassandraDataAccessException("Can't delete data in queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + key);
        }

        try {
            mutator.addDeletion(row, columnFamily, key, longSerializer);

            if (execute) {
                mutator.execute();
            }

        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while deleting " + key + " from " + columnFamily);
        }

    }




    /**
     * Delete a given string column in a raw in a column family
     * @param columnFamily  ColumnFamily Name
     * @param row row name
     * @param key string key to of the column
     * @param mutator Mutator reference
     * @param execute execute the deletion ?
     * @throws CassandraDataAccessException
     */
/*    public static void deleteStringColumnFromRaw(String columnFamily,String row, String key ,Mutator<String> mutator,
                                                 boolean execute)
            throws CassandraDataAccessException {

        if (mutator == null) {
            throw new CassandraDataAccessException("Can't delete Data , no mutator provided ");
        }

        if (columnFamily == null || row == null || key == null) {
            throw new CassandraDataAccessException("Can't delete data in queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + key);
        }

         try {
            mutator.addDeletion(row, columnFamily, key, stringSerializer);
             if (execute) {
                 mutator.execute();
             }
        } catch (Exception e) {
           throw new CassandraDataAccessException("Error while deleting " + key + " from " + columnFamily);
        }
    }*/


    /**
     * Delete an integer column from row
     * @param columnFamily  name of column family
     * @param row name of row
     * @param key column key value to delete
     * @param mutator mutator
     * @param execute  whether to execute the mutator
     * @throws CassandraDataAccessException
     */
/*    public static void deleteIntegerColumnFromRow(String columnFamily, String row, int key,
                                               Mutator<String> mutator, boolean execute) throws CassandraDataAccessException {


        if (mutator == null) {
            throw new CassandraDataAccessException("Can't delete Data , no mutator provided ");
        }

        if (columnFamily == null || row == null) {
            throw new CassandraDataAccessException("Can't delete data in queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + key);
        }

        try {
            mutator.addDeletion(row, columnFamily, key, integerSerializer);

            if (execute) {
                mutator.execute();
            }

        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while deleting " + key + " from " + columnFamily);
        }

    }*/

    /**
     * Delete an integer column from row
     * @param columnFamily   name of column family
     * @param row name of row
     * @param key column key to delete
     * @param keyspace keyspace
     * @throws CassandraDataAccessException
     */
/*    public static void deleteIntegerColumnFromRow(String columnFamily, String row, Integer key,
                                                  Keyspace keyspace) throws CassandraDataAccessException {


        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't delete Data , no keyspace provided ");
        }

        if (columnFamily == null || row == null) {
            throw new CassandraDataAccessException("Can't delete data in queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + key);
        }


        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    stringSerializer);
            mutator.addDeletion(row, columnFamily, key, integerSerializer);
            mutator.execute();
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while deleting data",e);
        }
    }*/


    /**
     * delete a list of integer rows from a column family
     * @param columnFamily name of column family
     * @param rows list of rows to be removed
     * @param keyspace keyspace
     * @throws CassandraDataAccessException
     */
    public static void deleteIntegerRowListFromColumnFamily(String columnFamily, List<String> rows, Keyspace keyspace) throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't delete Data , no keyspace provided ");
        }
        
        if (columnFamily == null || rows == null) {
            throw new CassandraDataAccessException("Can't delete data in queueType = " + columnFamily +
            " and rowName=" + rows );
        }
        
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    stringSerializer);
            for(String row: rows){
                mutator.addDeletion(row, columnFamily);
            }
            mutator.execute();
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while deleting data",e);
        }
    }

/*    public static void deleteLongColumnListFromColumnFamily(String columnFamily, Keyspace keyspace, String row, List<Long> columns) throws CassandraDataAccessException{
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't delete Data , no keyspace provided ");
        }

        if (columnFamily == null || columns == null) {
            throw new CassandraDataAccessException("Can't delete data in queueType = " + columnFamily +
                    " and rowName=" + columns );
        }

        if(columns.isEmpty()) {
            return;
        }

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    stringSerializer);
            for(long column: columns){
                mutator.addDeletion(row, columnFamily, column, longSerializer);
            }
            mutator.execute();
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while deleting data",e);
        }
    }*/

    /**
     * delete a whole row from a given colum family
     * @param columnFamily column family the row to be deleted is in
     * @param keyspace  key space involved
     * @param row  name of row to be removed
     * @throws CassandraDataAccessException
     */
/*    public static void deleteWholeRowFromColumnFamily(String columnFamily,Keyspace keyspace, String row) throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't delete Data , no keySpace provided ");
        }
        if (columnFamily == null || keyspace == null) {
            throw new CassandraDataAccessException("Can't delete data in queueType = " + columnFamily +
                    " and keySpace=" + keyspace );
        }
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    stringSerializer);
            mutator.addDeletion(row, columnFamily);
            mutator.execute();
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while deleting data",e);
        }
    }*/

    /**
     * Remove <raw key, column key> list from a keyspace
     * @param columnFamily column falimiy to delete row/columns from
     * @param keyspace key space
     * @param inputMap a Map containing (row)/(column) to delete
     * @throws CassandraDataAccessException
     */
/*    public static void deleteStringColumnSpecifiedInRowAsBatch(String columnFamily, Keyspace keyspace, Map<String,String> inputMap) throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't delete Data , no keyspace provided ");
        }

        if (columnFamily == null || inputMap == null) {
            throw new CassandraDataAccessException("Can't delete data in queueType = " + columnFamily);
        }

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    stringSerializer);
            Set<String> rowNames = inputMap.keySet();
            for(String row: rowNames){
                String columnName = inputMap.get(row);
                mutator.addDeletion(row, columnFamily, columnName , stringSerializer);
            }
            mutator.execute();
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while deleting data",e);
        }
    }*/

/*    public static void deleteLongColumnSpecifiedInRowAsBatch(String columnFamily, Keyspace keyspace, Map<Long,String> inputMap) throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't delete Data , no keyspace provided ");
        }

        if (columnFamily == null || inputMap == null) {
            throw new CassandraDataAccessException("Can't delete data in queueType = " + columnFamily);
        }

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    stringSerializer);
            Set<Long> messageIdsToDelete = inputMap.keySet();
            for(Long messageID: messageIdsToDelete){
                String rowName = inputMap.get(messageID);
                mutator.addDeletion(rowName, columnFamily, messageID , longSerializer);
            }
            mutator.execute();
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while deleting data",e);
        }
    }*/
    
    public static Map<String, List<String>> listAllStringRows(String columnFamilyName, Keyspace keyspace){
    	Map<String, List<String>> results = new HashMap<String, List<String>>();
    	
        RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
        .createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
        
        rangeSlicesQuery.setColumnFamily(columnFamilyName);
        rangeSlicesQuery.setRange("", "", false, 1000);
        QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery
                .execute();
        for (Row<String, String, String> row : result.get().getList()) {
            List<String> list = new ArrayList<String>(); 
            String rowkey = new String(row.getKey().getBytes()); 
            for (HColumn<String, String> hc : row.getColumnSlice().getColumns()) {
            	list.add(hc.getValue());
            }
            results.put(rowkey, list);
        }
        return results;
    }
}
