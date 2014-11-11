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
package org.wso2.andes.server.store.util;


import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.server.cassandra.MessageExpirationWorker;
import org.wso2.andes.store.cassandra.cql.dao.CQLQueryBuilder;
import org.wso2.andes.store.cassandra.cql.dao.CQLQueryBuilder.Table;
import org.wso2.andes.store.cassandra.cql.dao.CassandraHelper.WHERE_OPERATORS;
import org.wso2.andes.store.cassandra.cql.dao.GenericCQLDAO;

import java.util.*;

import static org.wso2.andes.store.cassandra.CassandraConstants.*;
import static org.wso2.andes.store.cassandra.cql.dao.GenericCQLDAO.CLUSTER_SESSION;


/**
 * Class <code>CassandraDataAccessHelper</code> Encapsulate the Cassandra DataAccessLogic used in
 * CassandraMessageStore
 */
public class CQLDataAccessHelper {

    private static Log log = LogFactory.getLog(CQLDataAccessHelper.class);

    public static final String USERNAME_KEY = "username";
    public static final String PASSWORD_KEY = "password";
    public static final String MSG_COUNTER_COLUMN = "counter_value";
    public static final String MSG_COUNTER_QUEUE = "queue_name";
    public static final String MSG_COUNTER_ROW = "counter_row_id";

    public static final Integer STANDARD_PAGE_SIZE = 10000; // Standard row count retrieved in one call during a paginated data retrieval.

    //cql table column which store slice column key (compound primary key)
    public static final String MSG_KEY = "message_key";

    // cql table column which store slice column value
    public static final String MSG_VALUE = "message_value";

    //cql table column which store row id (compound primary key)
    public static final String MSG_ROW_ID = "message_row_id";

    //Columns needed for MESSAGES_FOR_EXPIRY_COLUMN_FAMILY Queue
    public static final String MESSAGE_ID = "message_id";
    public static final String MESSAGE_EXPIRATION_TIME = "expiration_time";
    public static final String MESSAGE_DESTINATION = "destination";
    public static final String MESSAGE_IS_FOR_TOPIC = "is_for_topic";

    public static final class ClusterConfiguration {

        private final String userName;
        private final String password;
        private final String clusterName;
        private final List<String> connections;
        private final int port;

        public ClusterConfiguration(String userName, String password, String clusterName,
                                    List<String> connections, int port) {
            super();
            this.userName = userName;
            this.password = password;
            this.clusterName = clusterName;
            this.connections = connections;
            this.port = port;
        }

        public String getUserName() {
            return userName;
        }

        public String getPassword() {
            return password;
        }

        public String getClusterName() {
            return clusterName;
        }

        public List<String> getConnections() {
            return connections;
        }

        public int getPort() {
            return port;
        }


    }

    /**
     * Create a Cassandra Cluster instance given the connection details
     *
     * @param config cluster config
     * @return Cluster
     * @throws CassandraDataAccessException
     */
    public static Cluster createCluster(ClusterConfiguration config)
            throws CassandraDataAccessException {

        String userName = config.getUserName();
        String password = config.getPassword();
        List<String> connections = config.getConnections();
        String clusterName = config.getClusterName();
        int port = config.getPort();

        if (userName == null || password == null) {
            throw new CassandraDataAccessException(
                    "Can't create cluster with empty userName or Password");
        }

        if (clusterName == null) {
            throw new CassandraDataAccessException("Can't create cluster with empty cluster name");
        }

        if (connections == null || connections.isEmpty()) {
            throw new CassandraDataAccessException(
                    "Can't create cluster with empty connection string");
        }

        int maxConnections = 1000;
        int concurrency = 1000;
        boolean async = true;
        String compression = "";

        StringBuilder configProps = new StringBuilder();
        configProps.append("  concurrency:          " + concurrency)
                   .append("\n  mode:                 " + (async ? "asynchronous" : "blocking"))
                   .append("\n  per-host connections: " + maxConnections)
                   .append("\n  compression:          " + compression);

        System.out.println(configProps.toString());

        Cluster cluster = null;

        try {
            PoolingOptions pools = new PoolingOptions();
            pools.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, concurrency);
            pools.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE,
                    concurrency);
            pools.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL,
                    concurrency);
            pools.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE,
                    concurrency);
            pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
            pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
            pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
            pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);


            SocketOptions soketOptions = new SocketOptions();
            soketOptions.setConnectTimeoutMillis(10000);
            soketOptions.setReadTimeoutMillis(15000);
            soketOptions.setTcpNoDelay(true);
            soketOptions.setKeepAlive(true);
            // Create cluster

            Builder builder = Cluster.builder();
            for (String con : connections) {
                builder.addContactPoints(con);
            }
            builder.withPoolingOptions(pools)
                   .withSocketOptions(soketOptions)
                   .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                   .withReconnectionPolicy(new ConstantReconnectionPolicy(200L)).withPort(port)
                   .withCredentials(userName, password);

            cluster = builder.build();

            if (compression.trim().length() > 0) {
                cluster.getConfiguration().getProtocolOptions()
                       .setCompression(ProtocolOptions.Compression.SNAPPY);
            }

            // validate cluster information after connect
            StringBuilder metaDataAfterConnect = new StringBuilder();
            Set<Host> allHosts = cluster.getMetadata().getAllHosts();
            for (Host h : allHosts) {
                metaDataAfterConnect.append("[");
                metaDataAfterConnect.append(h.getDatacenter());
                metaDataAfterConnect.append("-");
                metaDataAfterConnect.append(h.getRack());
                metaDataAfterConnect.append("-");
                metaDataAfterConnect.append(h.getAddress());
                metaDataAfterConnect.append("]\n");
            }
            System.out.println("Cassandra Cluster: " + metaDataAfterConnect.toString());

        } catch (NoHostAvailableException ex) {
            throw new CassandraDataAccessException(" No Host available to access ", ex);
        } catch (Exception ex) {
            log.error(ex);
            throw new CassandraDataAccessException(" Can not create cluster ", ex);
        }

        return cluster;
    }

    public static Session createSession(Cluster cluster) {
        return cluster.connect();
    }


    /**
     * Create a keySpace in a given cluster
     *
     * @param cluster Cluster where keySpace should be created
     * @param keySpace name of the KeySpace
     * @throws CassandraDataAccessException
     */
    /**
     *
     * @param cluster
     * @param clusterSession
     * @param keySpace
     * @param replicationFactor
     * @param strategyClass
     * @throws CassandraDataAccessException
     */
    public static void createKeySpace(Cluster cluster, String clusterSession, String keySpace,
                                      int replicationFactor,
                                      String strategyClass) throws CassandraDataAccessException {

        boolean isKeysapceExist = isKeySpaceExist(keySpace);
        if (isKeysapceExist) {
            return;
        }

        String sql = "CREATE KEYSPACE " + keySpace + " WITH replication " + "= {'class':'" +
                     strategyClass + "', 'replication_factor':" + replicationFactor + "};";

        GenericCQLDAO.execute(CLUSTER_SESSION, sql);

    }


    /**
     * Create a Column family in a Given Cluster instance
     * @param name  ColumnFamily Name
     * @param keySpace KeySpace name
     * @param cluster   Cluster instance
     * @param comparatorType Comparator
     * @throws CassandraDataAccessException   In case of an Error accessing database or data error
     */

    /**
     * Create a Column family in a Given Cluster instance
     *
     * @param keySpace
     *         KeySpace name
     * @param table
     * @return
     * @throws CassandraDataAccessException
     */
    public static boolean isTableExist(String keySpace, String table)
            throws CassandraDataAccessException {
        Set<String> tableNames = new HashSet<String>();
        String query = "SELECT columnfamily_name FROM System.schema_columnfamilies WHERE " +
                       "keyspace_name='" + keySpace
                .toLowerCase() + "' and columnfamily_name='" + table.toLowerCase() + "';";
        ResultSet result = GenericCQLDAO.execute(GenericCQLDAO.CLUSTER_SESSION, query);
        List<Row> rows = result.all();
        String name = null;
        if (rows != null && !rows.isEmpty()) {
            name = rows.iterator().next().getString("columnfamily_name");
        }

        return (name == null || name.trim().length() == 0) ? false : true;
    }

    public static boolean isKeySpaceExist(String keySpace) throws CassandraDataAccessException {
        Set<String> tableNames = new HashSet<String>();
        String query = "select keyspace_name from system.schema_keyspaces WHERE keyspace_name='"
                       + keySpace
                .toLowerCase() + "';";
        ResultSet result = GenericCQLDAO.execute(GenericCQLDAO.CLUSTER_SESSION, query);
        List<Row> rows = result.all();
        String name = null;
        if (rows != null && !rows.isEmpty()) {
            name = rows.iterator().next().getString("keyspace_name");
        }

        return (name == null || name.trim().length() == 0) ? false : true;
    }

    /**
     * Create a Column family for cassandra counters in a given Cluster intance
     *
     * @param name
     *         ColumnFamily Name
     * @param keySpace
     *         KeySpace name
     * @param cluster
     *         Cluster instance
     * @throws CassandraDataAccessException
     *         In case of an Error accessing database or data error
     */
    public static void createCounterColumnFamily(String name, String keySpace, Cluster cluster,
                                                 int gcGraceSeconds)
            throws CassandraDataAccessException {
        boolean isKeysapceExist = isKeySpaceExist(keySpace);
        if (!isKeysapceExist) {
            throw new CassandraDataAccessException(
                    "Can't create Column family, keyspace " + keySpace +
                    " does not exist");
        }
        boolean isTableExist = isTableExist(keySpace, name);
        if (!isTableExist) {
            Table table = new Table(null, name, keySpace, gcGraceSeconds);
            table.getColumnType().put(MSG_COUNTER_QUEUE, DataType.text());
            table.getColumnType().put(MSG_COUNTER_ROW, DataType.text());
            table.getColumnType().put(MSG_COUNTER_COLUMN, DataType.counter());
            table.getPrimaryKeys().add(MSG_COUNTER_ROW);
            table.getPrimaryKeys().add(MSG_COUNTER_QUEUE);
            String query = CQLQueryBuilder.buildTableQuery(table);
            GenericCQLDAO.execute(keySpace, query);
        }

    }

    /**
     * Insert a raw-column for counting a property (row is property, column is item)
     *
     * @param cfName
     *         column family name
     * @param counterRowName
     *         name of row
     * @param queueColumn
     *         name of column
     * @param keyspace
     *         key space name
     * @throws CassandraDataAccessException
     */
    public static void insertCounterColumn(String cfName, String counterRowName, String queueColumn,
                                           String keyspace)
            throws CassandraDataAccessException {
        try {
            CQLQueryBuilder.CqlUpdate update = new CQLQueryBuilder.CqlUpdate(keyspace, cfName);
            update.addColumnAndValue(MSG_COUNTER_COLUMN, MSG_COUNTER_COLUMN);
            update.addCounterColumnAndValue(MSG_COUNTER_COLUMN, 0);
            update.addCondition(MSG_COUNTER_ROW, counterRowName, WHERE_OPERATORS.EQ);
            update.addCondition(MSG_COUNTER_QUEUE, queueColumn, WHERE_OPERATORS.EQ);
            GenericCQLDAO.update(keyspace, update);
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while inserting data to:" + cfName, e);
        }
    }

    /**
     * remove allocated raw-column space for counter
     *
     * @param cfName
     *         column family name
     * @param counterRowName
     *         name of row
     * @param queueColumn
     *         name of column
     * @param keyspace
     *         key space name
     * @throws CassandraDataAccessException
     */
    public static void removeCounterColumn(String cfName, String counterRowName, String queueColumn,
                                           String keyspace) throws CassandraDataAccessException {
        try {

            CQLQueryBuilder.CqlDelete cqlDelete = new CQLQueryBuilder.CqlDelete(keyspace, cfName);
            cqlDelete.addCondition(MSG_COUNTER_QUEUE, queueColumn, WHERE_OPERATORS.EQ);
            cqlDelete.addCondition(MSG_COUNTER_ROW, counterRowName, WHERE_OPERATORS.EQ);
            Delete delete = CQLQueryBuilder.buildSingleDelete(cqlDelete);
            GenericCQLDAO.execute(keyspace, delete.getQueryString());
        } catch (Exception e) {
            if (e.getMessage()
                 .contains("All host pools marked down. Retry burden pushed out to client")) {
                throw new CassandraDataAccessException(
                        "Unable to remove counter column as cassandra connection is down");
            } else {
                throw new CassandraDataAccessException("Error while accessing:" + cfName, e);
            }
        }
    }

    /**
     * Increment counter by given value
     *
     * @param rawID
     *         raw name
     * @param columnFamily
     *         column family name
     * @param columnName
     *         name of column
     * @param keyspace
     *         keyspace
     * @param incrementBy
     *         value to increase by
     * @throws CassandraDataAccessException
     */
    public static void incrementCounter(String columnName, String columnFamily, String rawID,
                                        String keyspace, long incrementBy)
            throws CassandraDataAccessException {
        try {

            CQLQueryBuilder.CqlUpdate update = new CQLQueryBuilder.CqlUpdate(keyspace,
                                                                             columnFamily);
            update.addColumnAndValue(MSG_COUNTER_COLUMN, MSG_COUNTER_COLUMN);
            update.addCounterColumnAndValue(MSG_COUNTER_COLUMN, +incrementBy);
            update.addCondition(MSG_COUNTER_ROW, rawID, WHERE_OPERATORS.EQ);
            update.addCondition(MSG_COUNTER_QUEUE, columnName, WHERE_OPERATORS.EQ);
            GenericCQLDAO.update(keyspace, update);
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while accessing:" + columnFamily, e);
        }
    }

    /**
     * Decrement counter by a given value
     *
     * @param rawID
     *         raw name
     * @param columnFamily
     *         column family name
     * @param columnName
     *         name of column
     * @param keyspace
     *         keyspace
     * @throws CassandraDataAccessException
     */
    public static void decrementCounter(String columnName, String columnFamily, String rawID,
                                        String keyspace, long decrementBy)
            throws CassandraDataAccessException {
        try {
            CQLQueryBuilder.CqlUpdate update = new CQLQueryBuilder.CqlUpdate(keyspace,
                                                                             columnFamily);
            update.addColumnAndValue(MSG_COUNTER_COLUMN, MSG_COUNTER_COLUMN);
            update.addCounterColumnAndValue(MSG_COUNTER_COLUMN, -decrementBy);
            update.addCondition(MSG_COUNTER_ROW, rawID, WHERE_OPERATORS.EQ);
            update.addCondition(MSG_COUNTER_QUEUE, columnName, WHERE_OPERATORS.EQ);
            GenericCQLDAO.update(keyspace, update);
        } catch (Exception he) {
            if (he.getMessage()
                  .contains("All host pools marked down. Retry burden pushed out to client")) {
                throw new CassandraDataAccessException(
                        "Unable to remove active subscribers as cassandra connection is down");
            } else {
                throw new CassandraDataAccessException("Error while accessing:" + columnFamily, he);
            }
        }
    }

    /**
     * Get the value of a counter
     *
     * @param keyspace
     *         name of key space
     * @param columnFamily
     *         column family name
     * @param key
     *         key value (property)
     * @param cloumnName
     *         column name (item)
     * @return long count value
     * @throws CassandraDataAccessException
     */
    public static long getCountValue(String keyspace, String columnFamily, String cloumnName,
                                     String key)
            throws CassandraDataAccessException {
        try {
            long count = 0;
            //sql = "select value from complex.MB_COUNTER where message_id='rocky'  ALLOW
            // FILTERING ;";
            CQLQueryBuilder.CqlSelect cqlSelect = new CQLQueryBuilder.CqlSelect(columnFamily, 0,
                                                                                true);
            cqlSelect.addColumn(MSG_COUNTER_COLUMN);
            cqlSelect.addCondition(MSG_COUNTER_QUEUE, cloumnName, WHERE_OPERATORS.EQ);
            cqlSelect.addCondition(MSG_COUNTER_ROW, key, WHERE_OPERATORS.EQ);
            Select select = CQLQueryBuilder.buildSelect(cqlSelect);
            if (log.isDebugEnabled()) {
                log.debug(" getMessageMetaDataOfMessage : " + select.toString());
            }

            ResultSet result = GenericCQLDAO.execute(keyspace, select.getQueryString());
            List<Row> rows = result.all();
            Iterator<Row> iter = rows.iterator();
            if (iter.hasNext()) {
                count = iter.next().getLong(MSG_COUNTER_COLUMN);
            }

            return count;
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while accessing:" + columnFamily, e);
        }
    }

    /**
     * Get a list of column names in a given row of a counter column family
     *
     * @param name
     *         column family name
     * @param keySpace
     *         name of key space
     * @param cluster
     *         cluster object
     * @param comparatorType
     *         data type of the key value.
     * @param valueType
     *         DataType
     * @param gcGraceSeconds
     *         gc grace seconds
     * @throws CassandraDataAccessException
     */
    public static void createColumnFamily(String name, String keySpace, Cluster cluster,
                                          String comparatorType, DataType valueType,
                                          int gcGraceSeconds) throws CassandraDataAccessException {
        DataType keyType = DataType.cint();
        if (INTEGER_TYPE.equalsIgnoreCase(comparatorType)) {
            keyType = DataType.cint();
        } else if (LONG_TYPE.equalsIgnoreCase(comparatorType)) {
            keyType = DataType.bigint();
        } else if (STRING_TYPE.equalsIgnoreCase(comparatorType)) {
            keyType = DataType.text();
        }

        boolean isKeysapceExist = isKeySpaceExist(keySpace);
        if (!isKeysapceExist) {
            throw new CassandraDataAccessException(
                    "Can't create Column family, keyspace " + keySpace +
                    " does not exist");
        }
        boolean isTableExist = isTableExist(keySpace, name);
        if (!isTableExist) {
            Table table = new Table(null, name, keySpace, gcGraceSeconds);
            table.getColumnType().put(MSG_ROW_ID, DataType.text());
            table.getColumnType().put(MSG_KEY, keyType);
            table.getColumnType().put(MSG_VALUE, valueType);
            table.getPrimaryKeys().add(MSG_ROW_ID);
            table.getPrimaryKeys().add(MSG_KEY);
            String query = CQLQueryBuilder.buildTableQuery(table);
            GenericCQLDAO.execute(keySpace, query);
        }
    }


    public static byte[] getMessageMetaDataFromQueue(String columnFamilyName, String keyspace,
                                                     long messageId)
            throws CassandraDataAccessException {
        byte[] value = null;
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if (columnFamilyName == null) {
            throw new CassandraDataAccessException(
                    "Can't access data with queueType = " + columnFamilyName);
        }

        try {

            CQLQueryBuilder.CqlSelect cqlSelect = new CQLQueryBuilder.CqlSelect(columnFamilyName, 0,
                                                                                true);
            cqlSelect.addColumn(MSG_VALUE);
            cqlSelect.addCondition(MSG_KEY, messageId, WHERE_OPERATORS.GT);
            cqlSelect.addCondition(MSG_KEY, Long.MAX_VALUE, WHERE_OPERATORS.LTE);
            Select select = CQLQueryBuilder.buildSelect(cqlSelect);
            if (log.isDebugEnabled()) {
                log.debug(" getMessageMetaDataOfMessage : " + select.toString());
            }

            ResultSet result = GenericCQLDAO.execute(keyspace, select.getQueryString());
            List<Row> rows = result.all();
            Iterator<Row> iter = rows.iterator();
            if (iter.hasNext()) {
                Row row = iter.next();
                value = CQLQueryBuilder.convertToByteArray(row, MSG_VALUE);
            }
            //TODO: should we set storage queue name as well?
            return value;
        } catch (Exception e) {
            throw new CassandraDataAccessException(
                    "Error while getting data from " + columnFamilyName, e);
        }
    }

    /**
     * Get set of <ColumnName,ColumnValue> list in a column family with a offset and a maximum cont
     * (row-STRING,column-LONG,Value-ByteArray). Used to get messages form cassandra
     *
     * @param rowName
     *         name of row (queue name)
     * @param columnFamilyName
     *         ColumnFamilyName
     * @param keyspace
     *         Cassandra KeySpace
     * @param lastProcessedId
     *         Last processed Message id to use as a off set
     * @param count
     *         max message count limit
     * @return ColumnSlice which contain the messages
     * @throws CassandraDataAccessException
     */
    public static List<AndesMessageMetadata> getMessagesFromQueue(String rowName,
                                                                  String columnFamilyName,
                                                                  String keyspace,
                                                                  long lastProcessedId,
                                                                  long rangeEnd, long count,
                                                                  boolean isRange, boolean parse)
            throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keySpace provided ");
        }

        if (columnFamilyName == null || rowName == null) {
            throw new CassandraDataAccessException(
                    "Can't access data with queueType = " + columnFamilyName +
                    " and queueName=" + rowName);
        }
        try {

            List<AndesMessageMetadata> metadataList = new ArrayList<AndesMessageMetadata>();
            CQLQueryBuilder.CqlSelect cqlSelect = new CQLQueryBuilder.CqlSelect(columnFamilyName,
                                                                                count,
                                                                                true);
            cqlSelect.addColumn(MSG_VALUE);
            cqlSelect.addColumn(MSG_KEY);
            if (isRange) {
                cqlSelect.addCondition(MSG_KEY, lastProcessedId, WHERE_OPERATORS.GTE);
                cqlSelect.addCondition(MSG_KEY, rangeEnd, WHERE_OPERATORS.LTE);
            } else {
                cqlSelect.addCondition(MSG_KEY, lastProcessedId, WHERE_OPERATORS.EQ);
            }

            cqlSelect.addCondition(MSG_ROW_ID, rowName, WHERE_OPERATORS.EQ);
            Select select = CQLQueryBuilder.buildSelect(cqlSelect);
            if (log.isDebugEnabled()) {
                log.debug(" getMessageMetaDataOfMessage : " + select.toString());
            }

            ResultSet result = GenericCQLDAO.execute(keyspace, select.getQueryString());
            List<Row> rows = result.all();
            Iterator<Row> iter = rows.iterator();
            while (iter.hasNext()) {
                Row row = iter.next();
                byte[] value = CQLQueryBuilder.convertToByteArray(row, MSG_VALUE);
                long msgId = row.getLong(MSG_KEY);
                if (value != null && value.length > 0) {
                    AndesMessageMetadata tmpEntry = new AndesMessageMetadata(msgId, value, parse);
                    //set back storage queue name
                    tmpEntry.setStorageQueueName(rowName);
                    if (!MessageExpirationWorker.isExpired(tmpEntry.getExpirationTime())) {
                        metadataList.add(tmpEntry);
                    }
                }

            }

            return metadataList;
        } catch (Exception e) {
            throw new CassandraDataAccessException(
                    "Error while getting data from " + columnFamilyName, e);
        }
    }

    /**
     * Get a list of values of a single Long column (in our case ; the message ID) from the given
     * column family
     *
     * @param rowKey
     * @param columnFamilyName
     * @param columnKey
     * @param keyspace
     * @param lastProcessedId
     * @param limit
     * @return
     * @throws CassandraDataAccessException
     */
    public static List<Long> getColumnDataFromColumnFamily(String rowKey,
                                                           String columnFamilyName,
                                                           String columnKey,
                                                           String keyspace,
                                                           long lastProcessedId,
                                                           long limit)
            throws CassandraDataAccessException {

        if (keyspace == null || columnFamilyName == null) {
            throw new CassandraDataAccessException("Can't access Data. The input keyspace or " +
                    "column family is invalid.");
        }

        if (columnFamilyName.isEmpty() || rowKey.isEmpty()) {
            throw new CassandraDataAccessException(
                    "Can't access data with column family = " + columnFamilyName +
                            " and row key=" + rowKey);
        }

        try {

            List<Long> valueList = new ArrayList<Long>();
            CQLQueryBuilder.CqlSelect cqlSelect = new CQLQueryBuilder.CqlSelect(columnFamilyName,
                    limit,
                    true);
            cqlSelect.addColumn(columnKey);

            cqlSelect.addCondition(MSG_ROW_ID, rowKey, WHERE_OPERATORS.EQ);

            // Add this condition only if the request needs messages from a specific point
            if (lastProcessedId > 0) {
                cqlSelect.addCondition(MSG_KEY, lastProcessedId, WHERE_OPERATORS.GTE);
            }

            Select select = CQLQueryBuilder.buildSelect(cqlSelect);

            if (log.isDebugEnabled()) {
                log.debug(" getMessageMetaDataFromQueue : " + select.toString());
            }

            ResultSet result = GenericCQLDAO.execute(keyspace, select.getQueryString());

            List<Row> rows = result.all();

            Iterator<Row> iter = rows.iterator();
            while (iter.hasNext()) {
                Row row = iter.next();
                long msgId = row.getLong(MSG_KEY);
                valueList.add(msgId);
            }

            return valueList;
        } catch (Exception e) {
            throw new CassandraDataAccessException(
                    "Error while getting data from " + columnFamilyName, e);
        }
    }

    /**
     * Return a message part with the given offset for the given message Id.
     *
     * @param rowName
     *         The row key for the message part
     * @param columnFamilyName
     *         Name of the column Family
     * @param keyspace
     *         Cassandra KeySpace
     * @param messageId
     *         The message Id
     * @param offset
     *         Message part offset to be retrieved
     * @return The part with the given offset
     * @throws CassandraDataAccessException
     */
    public static AndesMessagePart getMessageContent(String rowName,
                                                     String columnFamilyName, String keyspace,
                                                     long messageId, int offset)
            throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided.");
        }

        if (columnFamilyName == null || rowName == null) {
            throw new CassandraDataAccessException(
                    "Can't access data with queueType = " + columnFamilyName +
                    " and message part name =" + rowName);
        }

        try {
            AndesMessagePart messagePart = new AndesMessagePart();
            CQLQueryBuilder.CqlSelect cqlSelect = new CQLQueryBuilder.CqlSelect(columnFamilyName, 1,
                                                                                true);
            cqlSelect.addColumn(MSG_VALUE);
            cqlSelect.addColumn(MSG_KEY);
            cqlSelect.addCondition(MSG_ROW_ID, rowName, WHERE_OPERATORS.EQ);
            cqlSelect.addCondition(MSG_KEY, offset, WHERE_OPERATORS.EQ);

            Select select = CQLQueryBuilder.buildSelect(cqlSelect);
            if (log.isDebugEnabled()) {
                log.debug(" getMessageContent : " + select.toString());
            }

            ResultSet result = GenericCQLDAO.execute(keyspace, select.getQueryString());
            List<Row> rows = result.all();
            Iterator<Row> iterator = rows.iterator();

            if (iterator.hasNext()) {
                Row row = iterator.next();
                byte[] value = CQLQueryBuilder.convertToByteArray(row, MSG_VALUE);

                if (value != null && value.length > 0) {
                    messagePart.setData(value);
                    messagePart.setMessageID(messageId);
                    messagePart.setDataLength(value.length);
                    messagePart.setOffSet(offset);
                } else {
                    throw new CassandraDataAccessException(
                            "Message part with offset " + offset + " for the message " +
                            messageId + " was not found.");
                }
            }


            return messagePart;
        } catch (Exception e) {
            throw new CassandraDataAccessException(
                    "Error while getting data from " + columnFamilyName, e);
        }
    }

    /**
     * Get Number of <String,String> type columns in a given row in a cassandra column family
     *
     * @param rowName
     *         row Name we are querying for
     * @param columnFamilyName
     *         columnFamilName
     * @param keyspace
     * @param count
     *         number of columns the column slice should contain
     * @return
     */
    public static List<Row> getStringTypeColumnsInARow(String rowName, String colmunName,
                                                       String columnFamilyName,
                                                       String keyspace, long count)
            throws CassandraDataAccessException {

        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if (columnFamilyName == null || rowName == null) {
            throw new CassandraDataAccessException(
                    "Can't access data with queueType = " + columnFamilyName +
                    " and rowName=" + rowName);
        }

        try {

            CQLQueryBuilder.CqlSelect cqlSelect = new CQLQueryBuilder.CqlSelect(columnFamilyName,
                                                                                count,
                                                                                true);
            cqlSelect.addColumn(MSG_VALUE);
            cqlSelect.addColumn(MSG_KEY);

            cqlSelect.addCondition(MSG_ROW_ID, rowName, WHERE_OPERATORS.EQ);
            if (colmunName != null) {
                cqlSelect.addCondition(MSG_KEY, colmunName, WHERE_OPERATORS.EQ);
            }
            Select select = CQLQueryBuilder.buildSelect(cqlSelect);
            if (log.isDebugEnabled()) {
                log.debug(" getMessageMetaDataOfMessage : " + select.toString());
            }

            ResultSet result = GenericCQLDAO.execute(keyspace, select.getQueryString());
            List<Row> rows = result.all();
            if (rows == null) {
                rows = Collections.EMPTY_LIST;
            }

            return rows;
        } catch (Exception e) {
            throw new CassandraDataAccessException(
                    "Error while getting data from : " + columnFamilyName, e);
        }
    }

    public static Insert addMessageToQueue(String keySpace, String columnFamily, String storageQueueName,
                                           long messageId,
                                           byte[] message, boolean execute)
            throws CassandraDataAccessException {

        if (columnFamily == null || storageQueueName == null || message == null) {
            throw new CassandraDataAccessException(
                    "Can't add data with queueType = " + columnFamily +
                    " and storageQueueName=" + storageQueueName + " message id  = " + messageId + " message = " +
                    message);
        }

        try {

            Map<String, Object> keyValueMap = new HashMap<String, Object>();
            keyValueMap.put(MSG_ROW_ID, storageQueueName);
            keyValueMap.put(MSG_KEY, messageId);
            keyValueMap.put(MSG_VALUE, java.nio.ByteBuffer.wrap(message));
            Insert insert = CQLQueryBuilder.buildSingleInsert(keySpace, columnFamily, keyValueMap);
            if (execute) {
                GenericCQLDAO.execute(keySpace, insert.getQueryString());
            }
            return insert;
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while adding message to Queue", e);
        }
    }

    /**
     * add <integer,byte[]> column to a given row. Used to write message content to Cassandra
     *
     * @param keySpace
     *         key space
     * @param columnFamily
     *         Name of Column Family
     * @param row
     *         name of the row
     * @param key
     *         column offset
     * @param value
     *         byte[] message content to be written
     * @param execute
     *         whether to execte the mutator
     * @return
     * @throws CassandraDataAccessException
     */
    public static Insert addMessageToQueue(String keySpace, String columnFamily, String row,
                                           int key,
                                           byte[] value/*, Mutator<String> mutator*/,
                                           boolean execute)
            throws CassandraDataAccessException {

        if (columnFamily == null || row == null || value == null) {
            throw new CassandraDataAccessException(
                    "Can't add data with queueType = " + columnFamily +
                    " and row=" + row + " key  = " + key + " value = " + value);
        }
        Map<String, Object> keyValueMap = new HashMap<String, Object>();
        keyValueMap.put(MSG_ROW_ID, row);
        keyValueMap.put(MSG_KEY, key);
        keyValueMap.put(MSG_VALUE, java.nio.ByteBuffer.wrap(value));
        Insert insert = CQLQueryBuilder.buildSingleInsert(keySpace, columnFamily, keyValueMap);
        if (execute) {
            GenericCQLDAO.execute(keySpace, insert.getQueryString());
        }

        return insert;
    }

    /**
     * Add a <String,String> Mapping to a Given Row in cassandra column family. Mappings are used as
     * search indexes
     *
     * @param keySpace
     *         Cassandra KeySpace
     * @param columnFamily
     *         columnFamilyName
     * @param row
     *         row name
     * @param cKey
     *         key name for the adding column
     * @param cValue
     *         value for the adding column
     * @param execute
     * @return
     * @throws CassandraDataAccessException
     */
    public static Insert addCellToRow(String keySpace, String columnFamily, String row, String cKey,
                                      String cValue, boolean execute)
            throws CassandraDataAccessException {

        if (columnFamily == null || row == null || cKey == null) {
            throw new CassandraDataAccessException(
                    "Can't add data with queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + cKey);
        }
        try {
            Map<String, Object> keyValueMap = new HashMap<String, Object>();
            keyValueMap.put(MSG_ROW_ID, row);
            keyValueMap.put(MSG_KEY, cKey);
            keyValueMap.put(MSG_VALUE, cValue);
            Insert insert = CQLQueryBuilder.buildSingleInsert(keySpace, columnFamily, keyValueMap);

            if (execute) {
                GenericCQLDAO.execute(keySpace, insert.getQueryString());
            }
            return insert;
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while adding a Mapping to row ", e);
        }
    }

    /**
     * Add an Mapping Entry to a raw in a given column family
     *
     * @param columnFamily
     *         ColumnFamily name
     * @param row
     *         row name
     * @param cKey
     *         column key
     * @param cValue
     *         column value
     * @param execute
     *         should we execute the insertion. if false it will just and the insertion to the
     *         mutator
     * @throws CassandraDataAccessException
     *         In case of database access error or data error
     */
    public static void addMappingToRaw(String keySpace, String columnFamily, String row,
                                       String cKey, String cValue, boolean execute)
            throws CassandraDataAccessException {

        if (columnFamily == null || row == null || cKey == null) {
            throw new CassandraDataAccessException(
                    "Can't add data with queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + cKey);
        }
        try {
            addCellToRow(keySpace, columnFamily, row, cKey, cValue, execute);
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while adding a Mapping to row ", e);
        }
    }

    /**
     * Delete a given string column in a raw in a column family
     *
     * @param columnFamily
     *         column family name
     * @param row
     *         row name
     * @param key
     *         key name
     * @param keyspace
     *         cassandra keySpace
     * @throws CassandraDataAccessException
     *         In case of database access error or data error
     */
    public static void deleteStringColumnFromRaw(String columnFamily, String row, String key,
                                                 String keyspace)
            throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't delete Data , no keyspace provided ");
        }

        if (columnFamily == null || row == null || key == null) {
            throw new CassandraDataAccessException(
                    "Can't delete data in queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + key);
        }

        try {
            CQLQueryBuilder.CqlDelete cqlDelete = new CQLQueryBuilder.CqlDelete(keyspace,
                                                                                columnFamily);
            cqlDelete.addCondition(MSG_ROW_ID, row, WHERE_OPERATORS.EQ);
            cqlDelete.addCondition(MSG_KEY, key, WHERE_OPERATORS.EQ);
            Delete delete = CQLQueryBuilder.buildSingleDelete(cqlDelete);
            GenericCQLDAO.executeAsync(keyspace, delete.getQueryString());
        } catch (Exception e) {
            throw new CassandraDataAccessException(
                    "Error while deleting " + key + " from " + columnFamily + " as cassandra " +
                    "connection is down");

        }
    }

    /**
     * Delete a given long column in a raw in a column family.
     *
     * @param columnFamily
     *         name of column family
     * @param row
     *         name of row
     * @param key
     *         column key
     * @param execute
     *         whether to execute the mutator
     * @throws CassandraDataAccessException
     */
    public static Delete deleteLongColumnFromRaw(String keyspace, String columnFamily, String row,
                                                 long key,
                                                 boolean execute)
            throws CassandraDataAccessException {

        if (columnFamily == null || row == null) {
            throw new CassandraDataAccessException(
                    "Can't delete data in queueType = " + columnFamily +
                    " and rowName=" + row + " key = " + key);
        }

        try {

            CQLQueryBuilder.CqlDelete cqlDelete = new CQLQueryBuilder.CqlDelete(keyspace,
                                                                                columnFamily);
            cqlDelete.addCondition(MSG_ROW_ID, row, WHERE_OPERATORS.EQ);
            cqlDelete.addCondition(MSG_KEY, key, WHERE_OPERATORS.EQ);
            Delete delete = CQLQueryBuilder.buildSingleDelete(cqlDelete);
            return delete;

        } catch (Exception e) {
            throw new CassandraDataAccessException(
                    "Error while deleting " + key + " from " + columnFamily);
        }

    }



    /**
     * delete a list of integer rows from a column family
     *
     * @param columnFamily
     *         name of column family
     * @param rows
     *         list of rows to be removed
     * @param keyspace
     *         keyspace
     * @throws CassandraDataAccessException
     */
    public static void deleteIntegerRowListFromColumnFamily(String columnFamily, List<String> rows,
                                                            String keyspace)
            throws CassandraDataAccessException {
        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't delete Data , no keyspace provided ");
        }

        if (columnFamily == null || rows == null) {
            throw new CassandraDataAccessException(
                    "Can't delete data in queueType = " + columnFamily +
                    " and rowName=" + rows);
        }

        try {

            CQLQueryBuilder.CqlDelete cqlDelete = new CQLQueryBuilder.CqlDelete(keyspace,
                                                                                columnFamily);
            cqlDelete.addCondition(MSG_ROW_ID, rows.toArray(new String[rows.size()]),
                                   WHERE_OPERATORS.IN);
            Delete delete = CQLQueryBuilder.buildSingleDelete(cqlDelete);
            GenericCQLDAO.execute(keyspace, delete.getQueryString());
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while deleting data", e);
        }
    }

    /**
     * Delete a single row from given column family
     *
     * @param columnFamily
     * @param rowKey
     * @param keyspace
     */
    public static void deleteRowFromColumnFamily(String columnFamily, String rowKey,
                                                 String keyspace) throws
            CassandraDataAccessException {

        if (keyspace == null) {
            throw new CassandraDataAccessException("Cannot delete data, no keyspace provided");
        }

        try {

            CQLQueryBuilder.CqlDelete cqlDelete = new CQLQueryBuilder.CqlDelete(keyspace,
                    columnFamily);

            cqlDelete.addCondition(MSG_ROW_ID, rowKey, WHERE_OPERATORS.EQ);
            Delete delete = CQLQueryBuilder.buildSingleDelete(cqlDelete);
            GenericCQLDAO.execute(keyspace, delete.getQueryString());
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while deleting data", e);
        }


    }

    public static Map<String, List<String>> listAllStringRows(String columnFamilyName,
                                                              String keyspace)
            throws CassandraDataAccessException {
        Map<String, List<String>> results = new HashMap<String, List<String>>();

        CQLQueryBuilder.CqlSelect cqlSelect = new CQLQueryBuilder.CqlSelect(columnFamilyName, 0,
                                                                            false);
        cqlSelect.addColumn(MSG_ROW_ID);
        cqlSelect.addColumn(MSG_KEY);
        cqlSelect.addColumn(MSG_VALUE);

        Select select = CQLQueryBuilder.buildSelect(cqlSelect);

        ResultSet result = GenericCQLDAO.execute(keyspace, select.getQueryString());
        List<Row> rows = result.all();
        Iterator<Row> iter = rows.iterator();
        while (iter.hasNext()) {
            Row row = iter.next();
            String rowKey = row.getString(MSG_ROW_ID);
            String key = row.getString(MSG_KEY);
            String value = row.getString(MSG_VALUE);

            if (!results.containsKey(rowKey)) {
                results.put(rowKey, new ArrayList<String>());
            }
            results.get(rowKey).add(value);
        }

        return results;
    }

    /**
     * Get Top @param limit messages having expiration times < current timestamp if limit <= 0,
     * fetches all entries matching criteria.
     *
     * @param limit
     * @param columnFamilyName
     * @param keyspace
     * @return
     */
    public static List<AndesRemovableMetadata> getExpiredMessages(int limit,
                                                                  String columnFamilyName,
                                                                  String keyspace)
            throws CassandraDataAccessException {

        if (keyspace == null) {
            throw new CassandraDataAccessException("Can't access Data , no keyspace provided ");
        }

        if (columnFamilyName == null) {
            throw new CassandraDataAccessException(
                    "Can't access data with queueType = ");
        }

        try {

            List<AndesRemovableMetadata> expiredMessages = new ArrayList<AndesRemovableMetadata>();

            Long currentTimestamp = System.currentTimeMillis();

            CQLQueryBuilder.CqlSelect cqlSelect = new CQLQueryBuilder.CqlSelect(columnFamilyName,
                                                                                limit, true);
            cqlSelect.addColumn(MESSAGE_ID);
            cqlSelect.addColumn(MESSAGE_DESTINATION);
            cqlSelect.addColumn(MESSAGE_IS_FOR_TOPIC);
            cqlSelect.addColumn(MESSAGE_EXPIRATION_TIME);

            cqlSelect.addCondition(MESSAGE_EXPIRATION_TIME, currentTimestamp, WHERE_OPERATORS.LTE);

            Select select = CQLQueryBuilder.buildSelect(cqlSelect);

            if (log.isDebugEnabled()) {
                log.debug(" getExpiredMessages : " + select.toString());
            }

            ResultSet result = GenericCQLDAO.execute(keyspace, select.getQueryString());
            List<Row> rows = result.all();

            for (Row row : rows) {
                AndesRemovableMetadata arm = new AndesRemovableMetadata(row.getLong(MESSAGE_ID),
                                                                        row.getString(
                                                                                MESSAGE_DESTINATION),
                                                                        row.getString(
                                                                                MESSAGE_DESTINATION));
                arm.isForTopic = row.getBool(MESSAGE_IS_FOR_TOPIC);

                if (arm.messageID > 0) {
                    expiredMessages.add(arm);
                }
            }

            return expiredMessages;
        } catch (Exception e) {
            throw new CassandraDataAccessException(
                    "Error while getting data from " + columnFamilyName, e);
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
                                                       Cluster cluster, int gcGraceSeconds)
            throws CassandraDataAccessException {

        if (!isKeySpaceExist(keySpace)) {
            throw new CassandraDataAccessException(
                    "Can't create Column family, keyspace " + keySpace +
                    " does not exist");
        }
        boolean isTableExist = isTableExist(keySpace, name);
        if (!isTableExist) {
            Table table = new Table(null, name, keySpace, gcGraceSeconds);
            //table.getColumnType().put(MESSAGES_TO_EXPIRE_ROW_KEY,DataType.varchar());
            table.getColumnType().put(MESSAGE_ID, DataType.bigint());
            table.getColumnType().put(MESSAGE_EXPIRATION_TIME, DataType.bigint());
            table.getColumnType().put(MESSAGE_IS_FOR_TOPIC, DataType.cboolean());
            table.getColumnType().put(MESSAGE_DESTINATION, DataType.varchar());

            //TODO add a default row as the first primary key so it becomes the row id
            table.getPrimaryKeys().add(MESSAGE_ID);
            table.getPrimaryKeys().add(MESSAGE_EXPIRATION_TIME);
            String query = CQLQueryBuilder.buildTableQuery(table);
            GenericCQLDAO.execute(keySpace, query);
        }
    }
}
