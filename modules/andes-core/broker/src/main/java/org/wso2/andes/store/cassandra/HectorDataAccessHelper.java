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
package org.wso2.andes.store.cassandra;


import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HUnavailableException;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.commons.lang.StringUtils;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.MessageExpirationWorker;
import org.wso2.andes.store.AndesStoreUnavailableException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Class <code>HectorDataAccessHelper</code> Encapsulate the Cassandra DataAccessLogic used in
 * org.wso2.andes.store.cassandra.HectorBasedAndesContextStoreImpl and org.wso2.andes
 * .store.cassandra.hector.HectorBasedMessageStoreImpl
 */
public class HectorDataAccessHelper {

    /** The error message emitted hector client library when it loose connectivity to all known cassandra hosts */
    private static final String HECTOR_CONNECTION_ERROR_MESSAGE = 
                                                    "All host pools marked down. Retry burden pushed out to client";
    
    public static final int STANDARD_PAGE_SIZE = 1000;
    /**
     * Serializes used for Cassandra data operations
     */
    private static StringSerializer stringSerializer = StringSerializer.get();

    private static LongSerializer longSerializer = LongSerializer.get();

    private static BytesArraySerializer bytesArraySerializer = BytesArraySerializer.get();

    private static IntegerSerializer integerSerializer = IntegerSerializer.get();

    private static ByteBufferSerializer byteBufferSerializer = ByteBufferSerializer.get();

    /**
     * The number of records read in "listAllStringRows" method.
     */
    private final static int MAX_NUMBER_OF_ROWS_TO_READ = 1000;

    /**
     * Create a keySpace in a given cluster
     *
     * @param cluster      Cluster where Keyspace should be created
     * @param keySpaceName name of the Cassandra KeySpace
     * @return Keyspace
     */
    public static Keyspace createKeySpace(Cluster cluster, String keySpaceName, int replicationFactor,
                                          String strategyClass, ConfigurableConsistencyLevel policy) {
        //Define the keySpaceName
        ThriftKsDef thriftKsDef = new ThriftKsDef(keySpaceName);
        thriftKsDef.setReplicationFactor(replicationFactor);
        if (strategyClass == null || strategyClass.isEmpty()) {
            strategyClass = ThriftKsDef.DEF_STRATEGY_CLASS;
        }
        thriftKsDef.setStrategyClass(strategyClass);

        KeyspaceDefinition definition = cluster.describeKeyspace(keySpaceName);
        if (definition == null) {
            //Adding keySpaceName to the cluster
            cluster.addKeyspace(thriftKsDef, true);
        }

        Keyspace keyspace = HFactory.createKeyspace(keySpaceName, cluster);
        // Setting consistency policy
        keyspace.setConsistencyLevelPolicy(policy);
        return keyspace;
    }

    /**
     * Create a Column family in a Given Cluster instance
     *
     * @param name           ColumnFamily Name
     * @param keySpace       KeySpace name
     * @param cluster        Cluster instance
     * @param comparatorType Comparator
     * @param gcGraceSeconds gc_grace_second option in cassandra
     * @throws CassandraDataAccessException In case of an Error accessing database or data error
     */
    public static void createColumnFamily(String name, String keySpace, Cluster cluster,
                                          String comparatorType, int gcGraceSeconds)
                                                                                    throws AndesStoreUnavailableException,
                                                                                    CassandraDataAccessException {

        try {

            KeyspaceDefinition keyspaceDefinition = cluster.describeKeyspace(keySpace);

            if (keyspaceDefinition == null) {
                throw new IllegalStateException("Can't create Column family, keyspace " +
                                                keySpace + " does not exist");
            }

            ColumnFamilyDefinition columnFamilyDefinition =
                                                            new ThriftCfDef(
                                                                            keySpace,
                                                                            name,
                                                                            ComparatorType.getByClassName(comparatorType));
            
            columnFamilyDefinition.setGcGraceSeconds(gcGraceSeconds);
            List<ColumnFamilyDefinition> cfDefinitionList = keyspaceDefinition.getCfDefs();
            HashSet<String> columnFamilyNames = new HashSet<String>();

            for (ColumnFamilyDefinition definition : cfDefinitionList) {
                columnFamilyNames.add(definition.getName());
            }
            if (!columnFamilyNames.contains(name)) {
                cluster.addColumnFamily(columnFamilyDefinition, true);
            }
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("Unable to create the column family " + name +
                                                     " consistency level was not met",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Unable to create keyspace " + name +
                                                         "cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while creating:" + name, e);
            }
        } 
    }

    /**
     * Create a Column family for cassandra counters in a given Cluster instance
     *
     * @param name     ColumnFamily Name
     * @param cluster  Cluster instance
     * @throws CassandraDataAccessException In case of an Error accessing database or data error
     */
    public static void createCounterColumnFamily(String name, String keySpace, Cluster cluster,
                                                 int gcGraceSeconds) throws CassandraDataAccessException,
                                                                    AndesStoreUnavailableException
    {
        try {
            KeyspaceDefinition ksDef = cluster.describeKeyspace(keySpace);

            if (ksDef == null) {
                throw new IllegalStateException("Can't create Column family, keyspace " +
                                                keySpace + " does not exist");
            }

            ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keySpace, name,
                                                                                 ComparatorType.COUNTERTYPE);
            cfDef.setGcGraceSeconds(gcGraceSeconds);
            cfDef.setComparatorType(ComparatorType.UTF8TYPE);
            cfDef.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName());
            cfDef.setColumnType(ColumnType.STANDARD);

            List<ColumnFamilyDefinition> cfDefsList = ksDef.getCfDefs();
            HashSet<String> cfNames = new HashSet<String>();
            for (ColumnFamilyDefinition columnFamilyDefinition : cfDefsList) {
                cfNames.add(columnFamilyDefinition.getName());
            }
            if (!cfNames.contains(name)) {
                cluster.addColumnFamily(cfDef, true);
            }
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("Unable to create the column family " + name +
                                                     " consistency level was not met",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Unable to create keyspace " + name +
                                                         "cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while creating:" + name, e);
            }
        } 
    }

    /**
     * Method to initialize the MESSAGES_FOR_EXPIRY_COLUMN_FAMILY Column Family in cql style
     *
     * @param name
     * @param keySpace
     * @param cluster
     * @throws CassandraDataAccessException
     */
    public static void createMessageExpiryColumnFamily(String name, String keySpace,
                                                       Cluster cluster, String comparatorType,
                                                       int gcGraceSeconds) throws CassandraDataAccessException,
                                                                          AndesStoreUnavailableException
    {
        try {
            KeyspaceDefinition ksDef = cluster.describeKeyspace(keySpace);

            if (ksDef == null) {
                throw new IllegalStateException("Can't create Column family, keyspace " +
                                                keySpace + " does not exist");
            }

            ColumnFamilyDefinition cfDef =
                                           new ThriftCfDef(keySpace, /* "Queue" */name,
                                                           ComparatorType.getByClassName(comparatorType));
            cfDef.setGcGraceSeconds(gcGraceSeconds);
            List<ColumnFamilyDefinition> cfDefsList = ksDef.getCfDefs();
            HashSet<String> cfNames = new HashSet<String>();
            for (ColumnFamilyDefinition columnFamilyDefinition : cfDefsList) {
                cfNames.add(columnFamilyDefinition.getName());
            }
            if (!cfNames.contains(name)) {
                cluster.addColumnFamily(cfDef, true);
            }

        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("Unable to create the column family " + name +
                                                     " consistency level was not met",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Unable to create keyspace " + name +
                                                         "cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while creating:" + name, e);
            }
        }
    }

    /**
     * Insert a raw-column for counting a property (row is property, column is item)
     *
     * @param cfName         column family name
     * @param counterRowName name of row
     * @param queueColumn    name of column
     * @param keyspace       key space name
     * @throws CassandraDataAccessException
     */
    public static void insertCounterColumn(String cfName, String counterRowName, String queueColumn,
                                           Keyspace keyspace)
            throws AndesStoreUnavailableException, CassandraDataAccessException {
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
            // inserting counter column
            mutator.insertCounter(counterRowName, cfName, HFactory.createCounterColumn
                    (queueColumn, 0L, StringSerializer.get()));
            mutator.execute();
            CounterQuery<String, String> counter = new ThriftCounterColumnQuery<String, String>(
                    keyspace, stringSerializer, stringSerializer);

            counter.setColumnFamily(cfName).setKey(counterRowName).setName(queueColumn);
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Unable to insert data since " +
                        "cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while inserting data to:" + cfName, e);
            }
        }
    }

    /**
     * Remove allocated raw-column space for counter
     *
     * @param cfName         column family name
     * @param counterRowName name of row
     * @param queueColumn    name of column
     * @param keyspace       key space name
     * @throws CassandraDataAccessException
     */
    public static void removeCounterColumn(String cfName, String counterRowName,
                                           String queueColumn, Keyspace keyspace)
            throws AndesStoreUnavailableException, CassandraDataAccessException {

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
            mutator.deleteCounter(counterRowName, cfName, queueColumn, stringSerializer);
            mutator.execute();
            CounterQuery<String, String> counter = new ThriftCounterColumnQuery<String, String>(
                    keyspace, stringSerializer, stringSerializer);

            counter.setColumnFamily(cfName).setKey(counterRowName).setName(queueColumn);
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Unable to access data as " +
                        "cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while accessing:" + cfName, e);
            }
        }
    }

    /**
     * Increment counter by given value
     *
     * @param rawID        raw name
     * @param columnFamily column family name
     * @param columnName   name of column
     * @param keyspace     keyspace
     * @param incrementBy  value to increase by
     * @throws CassandraDataAccessException
     */
    public static void incrementCounter(String columnName, String columnFamily, String rawID,
                                        Keyspace keyspace, long incrementBy)
            throws AndesStoreUnavailableException, CassandraDataAccessException {
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
            mutator.incrementCounter(rawID, columnFamily, columnName, incrementBy);
            mutator.execute();
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while accessing " + columnFamily +
                        " since cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while accessing:" + columnFamily, e);
            }
        }
    }

    /**
     * Decrement counter by a given value
     *
     * @param rawID        raw name
     * @param columnFamily column family name
     * @param columnName   name of column
     * @param keyspace     keyspace
     * @throws CassandraDataAccessException
     */
    public static void decrementCounter(String columnName, String columnFamily, String rawID,
                                        Keyspace keyspace, long decrementBy)
            throws AndesStoreUnavailableException, CassandraDataAccessException {
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
            mutator.decrementCounter(rawID, columnFamily, columnName, decrementBy);
            mutator.execute();
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException he) {
            if (he.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while accessing " + columnFamily +
                        " since cassandra connection is down", he);
            } else {
                throw new CassandraDataAccessException("Error while accessing:" + columnFamily, he);
            }
        }
    }

    /**
     * Get the value of a counter
     *
     * @param keyspace     name of key space
     * @param columnFamily column family name
     * @param key          key value (property)
     * @param cloumnName   column name (item)
     * @return long count value
     * @throws CassandraDataAccessException
     */
    public static long getCountValue(Keyspace keyspace, String columnFamily, String cloumnName,
                                     String key)
            throws AndesStoreUnavailableException, CassandraDataAccessException {
        try {
            long count = 0;
            CounterQuery<String, String> query = HFactory.createCounterColumnQuery(keyspace,
                    stringSerializer, stringSerializer);
            query.setColumnFamily(columnFamily).setKey(key).setName(cloumnName);
            HCounterColumn<String> counter = query.execute().get();
            if (counter != null) {
                count = counter.getValue();
            }
            return count;
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while accessing " + columnFamily +
                        " since cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while accessing:" + columnFamily, e);
            }
        }
    }

    /**
     * Get the meta data stored for a given message ID
     *
     * @param columnFamilyName name of the column family
     * @param keyspace         Cassandra Keyspace
     * @param messageId        ID of the message
     * @return the metadata stored in Cassandra as a byte array
     * @throws CassandraDataAccessException
     */
    public static byte[] getMessageMetaDataOfMessage(String columnFamilyName, Keyspace keyspace,
                                                     long messageId) throws
                                                     AndesStoreUnavailableException, CassandraDataAccessException {
        byte[] value = null;
        if (keyspace == null) {
            throw new IllegalArgumentException("Can't access Data , no keyspace provided ");
        }

        if (columnFamilyName == null) {
            throw new IllegalArgumentException("Can't access data with queueType = " +
                    columnFamilyName);
        }

        try {
            RangeSlicesQuery<String, Long, byte[]> sliceQuery =
                    HFactory.createRangeSlicesQuery(keyspace, stringSerializer, longSerializer,
                            bytesArraySerializer);
            sliceQuery.setRange(null, null, false, Integer.MAX_VALUE);
            sliceQuery.setKeys(null, null);
            sliceQuery.setColumnNames(messageId);
            sliceQuery.setColumnFamily(columnFamilyName);

            QueryResult<OrderedRows<String, Long, byte[]>> result = sliceQuery.execute();

            if (result == null || result.get().getList().size() == 0) {
                value = null;
            } else {
                Row<String, Long, byte[]> rowSlice = result.get().peekLast();
                ColumnSlice<Long, byte[]> columnSlice = rowSlice.getColumnSlice();
                for (Object column : columnSlice.getColumns()) {
                    if (column instanceof HColumn) {
                        value = ((HColumn<Long, byte[]>) column).getValue();
                    }
                }
            }
            return value;
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while getting data from " +
                        columnFamilyName + " since cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while getting data from " +
                        columnFamilyName, e);
            }
        }
    }

    /**
     * Get set of <ColumnName,ColumnValue> list in a column family with a offset and a maximum cont
     * (row-STRING,column-LONG,Value-ByteArray). Used to get messages
     * form cassandra
     *
     * @param rowName          name of row (queue name)
     * @param columnFamilyName ColumnFamilyName
     * @param keyspace         Cassandra KeySpace
     * @param lastProcessedId  Last processed Message id to use as a offset
     * @param count            max message count limit
     * @throws CassandraDataAccessException
     */
    public static List<AndesMessageMetadata> getMessagesFromQueue(String rowName,
                                                                  String columnFamilyName,
                                                                  Keyspace keyspace,
                                                                  long firstID,
                                                                  long lastProcessedId,
                                                                  int count,
                                                                  boolean parse
    ) throws AndesStoreUnavailableException, CassandraDataAccessException {
        if (keyspace == null) {
            throw new IllegalArgumentException("Can't access Data , no keyspace provided ");
        }

        if (columnFamilyName == null || rowName == null) {
            throw new IllegalArgumentException("Can't access data with queueType = " +
                    columnFamilyName + " and queueName=" + rowName);
        }

        try {
            SliceQuery<String, Long, byte[]> sliceQuery =
                    HFactory.createSliceQuery(keyspace, stringSerializer, longSerializer,
                            bytesArraySerializer);
            sliceQuery.setKey(rowName);
            sliceQuery.setRange(firstID, lastProcessedId, false, count);

            sliceQuery.setColumnFamily(columnFamilyName);
            QueryResult<ColumnSlice<Long, byte[]>> result = sliceQuery.execute();
            ColumnSlice<Long, byte[]> columnSlice = result.get();
            List<AndesMessageMetadata> metadataList = new ArrayList<AndesMessageMetadata>();

            for (Object column : columnSlice.getColumns()) {
                if (column instanceof HColumn) {
                    AndesMessageMetadata metadata = new AndesMessageMetadata(((HColumn<Long,
                            byte[]>) column).getName(), ((HColumn<Long,
                            byte[]>) column).getValue(), parse);
                    if (!MessageExpirationWorker.isExpired(metadata.getExpirationTime())) {
                        metadataList.add(metadata);
                    }
                }
            }

            return metadataList;
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while getting data from " +
                        columnFamilyName + " since cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while getting data from " +
                        columnFamilyName, e);
            }
        }
    }

    /**
     * Get all columns in a given row of a cassandra column family
     *
     * @param rowName          row Name we are querying for
     * @param columnFamilyName column family name
     * @param keyspace         Cassandra Keyspace
     * @param count            number of columns the column slice should contain
     * @return
     */
    public static ColumnSlice<String, String> getStringTypeColumnsInARow(String rowName,
                                                                         String columnFamilyName,
                                                                         Keyspace keyspace, int count)
            throws AndesStoreUnavailableException, CassandraDataAccessException {

        if (keyspace == null) {
            throw new IllegalArgumentException("Can't access Data , no keyspace provided");
        }

        if (StringUtils.isBlank(columnFamilyName) || StringUtils.isBlank(rowName)) {
            throw new IllegalArgumentException("Can't access data with queueType = " +
                    columnFamilyName + " and rowName=" + rowName);
        }

        try {
            SliceQuery<String,String,String> sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer,
                    stringSerializer, stringSerializer);
            sliceQuery.setKey(rowName);
            sliceQuery.setColumnFamily(columnFamilyName);
            sliceQuery.setRange("", "", false, count);

            QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
            ColumnSlice<String, String> columnSlice = result.get();

            return columnSlice;
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while getting data from " +
                        columnFamilyName + " since cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while getting data from : " +
                        columnFamilyName, e);
            }
        }
    }

    /**
     * Method used to retrieve <long> list of a column keys from a given row. (in our case, message ID)
     * @param keyspace keyspace reference
     * @param columnFamilyName name of the column family
     * @param rowKey row key of which the column keys are required
     * @param count number of column keys to fetch
     * @param lastProcessedID last processed column key ID, so that this request can continue from that point.
     * @return List<Long>
     * @throws CassandraDataAccessException
     */
    public static List<Long> getNumericColumnKeysOfRow(Keyspace keyspace, String columnFamilyName, String rowKey,
                                                       int count, Long lastProcessedID)
                                                                                       throws AndesStoreUnavailableException,
                                                                                       CassandraDataAccessException {

        if (keyspace == null) {
            throw new IllegalArgumentException("Can't access Data , no keyspace provided");
        }

        if (StringUtils.isBlank(columnFamilyName) || StringUtils.isBlank(rowKey)) {
            throw new IllegalArgumentException("Can't access data with queueType = " + columnFamilyName +
                                                   " and row key =" + rowKey);
        }

        List<Long> messageIDs = new ArrayList<Long>();

        try {
            SliceQuery<String, Long, byte[]> sliceQuery =
                    HFactory.createSliceQuery(keyspace, stringSerializer, longSerializer,
                            bytesArraySerializer);
            sliceQuery.setKey(rowKey);
            sliceQuery.setRange(lastProcessedID, null, false, count);
            
            sliceQuery.setColumnFamily(columnFamilyName);
            QueryResult<ColumnSlice<Long, byte[]>> result = sliceQuery.execute();
            ColumnSlice<Long, byte[]> columnSlice = result.get();

            for (Object column : columnSlice.getColumns()) {
                if (column instanceof HColumn) {
                    messageIDs.add(((HColumn<Long, byte[]>) column).getName());
                }
            }

            return messageIDs;
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new CassandraDataAccessException("Error while getting data from " +
                        columnFamilyName + " since cassandra connection is down", e);
            } else {
                throw new AndesStoreUnavailableException("Error while getting data from " +
                        columnFamilyName, e);
            }
        }
    }

    /**
     * Add message to a queue
     *
     * @param columnFamily column family name
     * @param queue        name of queue (row name)
     * @param key          message ID of the message (column name)
     * @param message      message content
     * @param mutator      mutator to execute the query
     * @param execute      whether to execute the query
     * @throws CassandraDataAccessException
     */
    public static void addMessageToQueue(String columnFamily, String queue, int key,
                                         byte[] message, Mutator<String> mutator, boolean execute)
            throws AndesStoreUnavailableException, CassandraDataAccessException {

        if (mutator == null) {
            throw new IllegalArgumentException("Can't add Data , no mutator provided");
        }

        if (StringUtils.isBlank(columnFamily) || StringUtils.isBlank(queue) || message == null) {
            throw new IllegalArgumentException("Can't add data with queueType = " +
                    columnFamily + " and queue=" + queue + " offset = " + key + " message = " +
                    message);
        }

        try {
            mutator.addInsertion(queue.trim(), columnFamily,
                    HFactory.createColumn(key, message, integerSerializer,
                            BytesArraySerializer.get()));
            if (execute) {
                mutator.execute();
            }

        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while adding message to Queue " +
                        queue + " since cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while adding message to Queue", e);
            }
        }
    }


    /**
     * Add message to a queue
     *
     * @param columnFamily column family name
     * @param queue        name of queue (row name)
     * @param messageId    message ID of the message (column name)
     * @param message      message content
     * @param mutator      mutator to execute the query
     * @param execute      whether to execute the query
     * @throws CassandraDataAccessException
     */
    public static void addMessageToQueue(String columnFamily, String queue, long messageId,
                                         byte[] message, Mutator<String> mutator, boolean execute)
            throws AndesStoreUnavailableException, CassandraDataAccessException {

        if (mutator == null) {
            throw new IllegalArgumentException("Can't add Data , no mutator provided ");
        }

        if (columnFamily == null || queue == null || message == null) {
            throw new IllegalArgumentException("Can't add data with queueType = " +
                    columnFamily + " and queue=" + queue + " message id  = " + messageId + " " +
                    "message = " + message);
        }

        try {
            mutator.addInsertion(queue.trim(), columnFamily,
                    HFactory.createColumn(messageId, message, LongSerializer.get(),
                            BytesArraySerializer.get()));
            if (execute) {
                mutator.execute();
            }

        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while adding message to Queue " +
                        queue + " since cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while adding message to Queue", e);
            }
        }
    }

    /**
     * Add a <String,String> Mapping to a Given Row in cassandra column family.
     * Mappings are used as search indexes
     *
     * @param columnFamily columnFamilyName
     * @param row          row name
     * @param cKey         key name for the adding column
     * @param cValue       value for the adding column
     * @param keyspace     Cassandra KeySpace
     * @throws CassandraDataAccessException In case of database access error or data error
     */
    public static void addMappingToRaw(String columnFamily, String row, String cKey, String cValue,
                                       Keyspace keyspace) throws AndesStoreUnavailableException, CassandraDataAccessException {

        if (keyspace == null) {
            throw new IllegalArgumentException("Can't add Data , no KeySpace provided ");
        }

        if (columnFamily == null || row == null || cKey == null) {
            throw new IllegalArgumentException("Can't add data with queueType = " +
                    columnFamily + " and rowName=" + row + " key = " + cKey);
        }

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            mutator.addInsertion(row, columnFamily,
                    HFactory.createColumn(cKey, cValue.trim(), stringSerializer, stringSerializer));
            mutator.execute();
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while adding a mapping to row " +
                        row + " since cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while adding a mapping to row ", e);
            }
        }
    }

    /**
     * Delete a given string column in a raw in a column family
     *
     * @param columnFamily column family name
     * @param row          row name
     * @param key          key name
     * @param keyspace     cassandra keySpace
     * @throws CassandraDataAccessException In case of database access error or data error
     */
    public static void deleteStringColumnFromRaw(String columnFamily, String row, String key,
                                                 Keyspace keyspace)
            throws AndesStoreUnavailableException, CassandraDataAccessException {
        if (keyspace == null) {
            throw new IllegalArgumentException("Can't delete Data , no keyspace provided ");
        }

        if (columnFamily == null || row == null || key == null) {
            throw new CassandraDataAccessException("Can't delete data in queueType = " +
                    columnFamily + " and rowName=" + row + " key = " + key);
        }

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            mutator.addDeletion(row, columnFamily, key, stringSerializer);
            mutator.execute();
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while deleting " + key + " from " +
                        columnFamily + " since cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while deleting " + key + " from " +
                        columnFamily);
            }
        }
    }

    /**
     * Delete a given long column in a raw in a column family.
     *
     * @param columnFamily name of column family
     * @param row          name of row
     * @param key          column key
     * @param mutator      mutator
     * @param execute      whether to execute the mutator
     * @throws CassandraDataAccessException
     */
    public static void deleteLongColumnFromRaw(String columnFamily, String row, long key,
                                               Mutator<String> mutator, boolean execute)
            throws AndesStoreUnavailableException, CassandraDataAccessException {


        if (mutator == null) {
            throw new IllegalArgumentException("Can't delete Data , no mutator provided ");
        }

        if (columnFamily == null || row == null) {
            throw new IllegalArgumentException("Can't delete data in queueType = " +
                    columnFamily + " and rowName=" + row + " key = " + key);
        }

        try {
            mutator.addDeletion(row, columnFamily, key, longSerializer);

            if (execute) {
                mutator.execute();
            }

        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while deleting " + key + " from " +
                                                         row + " since cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while deleting " + key + " from " +
                                                       row);
            }
        }
    }

    /**
     * Delete a list of integer rows from a column family
     *
     * @param columnFamily name of column family
     * @param rows         list of rows to be removed
     * @param mutator      mutator
     * @throws CassandraDataAccessException
     */
    public static void deleteIntegerRowListFromColumnFamily(String columnFamily, List<String> rows,
                                                            Mutator<String> mutator, boolean execute)
            throws AndesStoreUnavailableException, CassandraDataAccessException {
        if (mutator == null) {
            throw new IllegalArgumentException("Can't delete Data , no mutator provided ");
        }

        if (columnFamily == null || rows == null) {
            throw new IllegalArgumentException("Can't delete data in queueType = " +
                    columnFamily + " and rowName=" + rows);
        }

        try {
            for (String row : rows) {
                mutator.addDeletion(row, columnFamily);
            }

            if (execute) {
                mutator.execute();
            }
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while deleting data since " +
                                                         "cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while deleting data", e);
            }
        }
    }

    /**
     * List all string rows
     *
     * @param columnFamilyName name of the column family
     * @param keyspace         Cassandra Keyspace
     * @return Map of Rows
     */
    public static Map<String, List<String>> listAllStringRows(String columnFamilyName,
                                                              Keyspace keyspace) throws AndesException
                                                                                , CassandraDataAccessException {
        
    try{
            Map<String, List<String>> results = new HashMap<String, List<String>>();

            RangeSlicesQuery<String, String, String> rangeSlicesQuery =
                                                                        HFactory
                                                                                .createRangeSlicesQuery(keyspace,
                                                                                                        stringSerializer,
                                                                                                        stringSerializer,
                                                                                                        stringSerializer);

            rangeSlicesQuery.setColumnFamily(columnFamilyName);
            rangeSlicesQuery.setRange("", "", false, MAX_NUMBER_OF_ROWS_TO_READ);
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

        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while deleting data since " +
                                                         "cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while deleting data", e);
            }
        }

    }

    /**
     * Read message content as a batch in cassandra
     *
     * @param columnFamily name of column family
     * @param keyspace     name of key space
     * @param messageId    list of message IDs
     * @return List of content List<AndesMessagePart>
     * @throws CassandraDataAccessException
     */
    public static Map<Long, List<AndesMessagePart>> getMessageContentBatch(
            String columnFamily,
            Keyspace keyspace, List<Long> messageId) throws
            AndesStoreUnavailableException, CassandraDataAccessException {

        //Holds the messages that will be sent back to the API for delivery
        Map<Long, List<AndesMessagePart>> messageContentBatch = new HashMap<Long, List<AndesMessagePart>>();

        try {
            //Specify the range of offsets in a message
            Integer startOffSet = 0;
            Integer maxOffset = Integer.MAX_VALUE;

            //TODO remove this once the mid is taken out from hector
            //TODO need to verify the reason for appending mid for the hector case
            List<String> sanitizedMessageID = new ArrayList<String>();
            //Need to prefix the message id since in the DB the message comes sanitized
            for (int msgIDs = 0; msgIDs < messageId.size(); msgIDs++) {
                sanitizedMessageID.add(HectorBasedMessageStoreImpl.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX +
                        messageId.get(msgIDs));
            }

            //Create the query to get the list of messages for the specified list of ids
            //SELECT * FROM MB_KEYSPACE.MB_CONTENT WHERE "QpidKeySpace"."MessageContent"
            Collection<String> messageIDs = sanitizedMessageID;
            MultigetSliceQuery<String, Integer, ByteBuffer> getMessageListQuery = HFactory
                    .createMultigetSliceQuery(keyspace, stringSerializer, integerSerializer, byteBufferSerializer);
            getMessageListQuery.setColumnFamily(columnFamily);
            getMessageListQuery.setRange(startOffSet, maxOffset, false, maxOffset);
            getMessageListQuery.setKeys(messageIDs);

            //The data structure holds message represented as ids in each row, messageID ---> offsets (1..*)
            // Each chunk will be defined as an offset, which which will be the column
            // i.e messageID   offset 0 offset 1
            //First for the given list of ids all the messages will be fetched from the database

            //Will return each row represented through the list of provided message ids
            QueryResult<Rows<String, Integer, ByteBuffer>> results = getMessageListQuery.execute();

            //Will go through the list of rows
            for (Row<String, Integer, ByteBuffer> messageIDRows : results.get()) {

                //Each column will represent an offset that will hold the message chunk
                List<HColumn<Integer, ByteBuffer>> columnSliced = messageIDRows.getColumnSlice().getColumns();

                //This will be the id of the message

                Long columnKey = Long.valueOf(messageIDRows.getKey().
                        substring(HectorBasedMessageStoreImpl.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX.length()));

                //Will iterate through each slice, each slice will represent an offset
                for (HColumn<Integer, ByteBuffer> column : columnSliced) {
                    if (column != null) {
                        int offset = column.getName();
                        byte[] content = bytesArraySerializer.fromByteBuffer(column
                                .getValue());

                        List<AndesMessagePart> andesMessagePart = messageContentBatch.get(columnKey);

                        if (null == andesMessagePart) {
                            andesMessagePart = new ArrayList<AndesMessagePart>();
                            messageContentBatch.put(columnKey, andesMessagePart);
                        }

                        AndesMessagePart msgPart = new AndesMessagePart();
                        msgPart.setMessageID(columnKey);
                        msgPart.setDataLength(content.length);
                        msgPart.setOffSet(offset);
                        msgPart.setData(content);
                        andesMessagePart.add(msgPart);
                    } else {
                        throw new RuntimeException("Unexpected Error , content already deleted for " +
                                "message id :" + messageId);
                    }
                }
            }
            
        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        } catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while getting message content " +
                        "since cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while getting message content", e);
            }
        }

        return messageContentBatch;


    }

    /**
     * Read message content from Cassandra
     *
     * @param rowKey       row name
     * @param columnFamily name of the column family
     * @param keyspace     Cassandra Keyspace
     * @param messageId    ID of the message
     * @param offsetValue  the offset of the message part
     * @return AndesMessagePart
     * @throws CassandraDataAccessException
     */
    public static AndesMessagePart getMessageContent(String rowKey,
                                                     String columnFamily,
                                                     Keyspace keyspace, long messageId,
                                                     int offsetValue) throws
                                                     AndesStoreUnavailableException, CassandraDataAccessException {
        AndesMessagePart messagePart = new AndesMessagePart();

        try {
            ColumnQuery<String, Integer, ByteBuffer> columnQuery = HFactory.createColumnQuery(keyspace,
                    stringSerializer, integerSerializer, byteBufferSerializer);

            columnQuery.setColumnFamily(columnFamily);
            columnQuery.setKey(rowKey.trim());
            columnQuery.setName(offsetValue);

            QueryResult<HColumn<Integer, ByteBuffer>> result = columnQuery.execute();
            HColumn<Integer, ByteBuffer> column = result.get();
            if (column != null) {
                int offset = column.getName();
                byte[] content = bytesArraySerializer.fromByteBuffer(column
                        .getValue());

                messagePart.setData(content);
                messagePart.setMessageID(messageId);
                messagePart.setDataLength(content.length);
                messagePart.setOffSet(offset);
            } else {
                throw new RuntimeException("Unexpected Error , content already deleted for " +
                        "message id :" + messageId);
            }

        } catch (HUnavailableException unavailEx) {
            throw new AndesStoreUnavailableException("unable to meet required consitancy level for the operation",
                                                     unavailEx);
        }catch (HectorException e) {
            if (e.getMessage().contains(HECTOR_CONNECTION_ERROR_MESSAGE)) {
                throw new AndesStoreUnavailableException("Error while getting message content " +
                        "since cassandra connection is down", e);
            } else {
                throw new CassandraDataAccessException("Error while getting message content", e);
            }
        }

        return messagePart;
    }
}
