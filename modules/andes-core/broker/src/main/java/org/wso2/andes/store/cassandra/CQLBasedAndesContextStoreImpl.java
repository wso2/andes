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

package org.wso2.andes.store.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.ConfigurationProperties;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.store.util.CQLDataAccessHelper;
import org.wso2.andes.server.store.util.CassandraDataAccessException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.wso2.andes.store.cassandra.CassandraConstants.*;

/**
 * CQL based Andes context store implementation.
 */
public class CQLBasedAndesContextStoreImpl implements AndesContextStore {

    /**
     * logger for CQLBasedAndesContextStoreImpl class
     */
    private static Log log = LogFactory.getLog(CQLBasedAndesContextStoreImpl.class);

    /**
     * DurableStoreConnection to connect to Cassandra database
     */
    private DurableStoreConnection connection;


    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection init(ConfigurationProperties connectionProperties) throws
            AndesException {

        try {
            CQLConnection cqlConnection = new CQLConnection();
            cqlConnection.initialize(connectionProperties);
            connection = cqlConnection;
            Cluster cluster = cqlConnection.getCluster();
            //create needed column families
            CQLDataAccessHelper.createColumnFamily(SUBSCRIPTIONS_COLUMN_FAMILY, KEYSPACE, cluster, CassandraConstants.STRING_TYPE, DataType.text(),
                    ((CQLConnection) connection.getConnection()).getGcGraceSeconds());
            CQLDataAccessHelper.createColumnFamily(EXCHANGE_COLUMN_FAMILY, KEYSPACE, cluster, CassandraConstants.STRING_TYPE, DataType.text(),
                    ((CQLConnection) connection.getConnection()).getGcGraceSeconds());
            CQLDataAccessHelper.createColumnFamily(QUEUE_COLUMN_FAMILY, KEYSPACE, cluster, CassandraConstants.STRING_TYPE, DataType.text(),
                    ((CQLConnection) connection.getConnection()).getGcGraceSeconds());
            CQLDataAccessHelper.createColumnFamily(BINDING_COLUMN_FAMILY, KEYSPACE, cluster, CassandraConstants.STRING_TYPE, DataType.text(),
                    ((CQLConnection) connection.getConnection()).getGcGraceSeconds());
            CQLDataAccessHelper.createColumnFamily(NODE_DETAIL_COLUMN_FAMILY, KEYSPACE, cluster, CassandraConstants.STRING_TYPE, DataType.text(),
                    ((CQLConnection) connection.getConnection()).getGcGraceSeconds());
            return cqlConnection;
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while creating column spaces during subscription " +
                    "store init. ", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, List<String>> getAllStoredDurableSubscriptions() throws AndesException {
        try {
            return CQLDataAccessHelper.listAllStringRows(SUBSCRIPTIONS_COLUMN_FAMILY, KEYSPACE);
        } catch (CassandraDataAccessException e) {
            log.error("error while getting durable subscriptions from cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeDurableSubscription(String destinationIdentifier, String subscriptionID, String subscriptionEncodeAsStr) throws AndesException {
        try {
            CQLDataAccessHelper.addMappingToRaw(KEYSPACE, SUBSCRIPTIONS_COLUMN_FAMILY, destinationIdentifier, subscriptionID, subscriptionEncodeAsStr, true);
        } catch (CassandraDataAccessException e) {
            log.error("error while storing durable subscriptions to cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDurableSubscription(String destinationIdentifier, String subscriptionID) throws AndesException {
        try {
            CQLDataAccessHelper.deleteStringColumnFromRaw(SUBSCRIPTIONS_COLUMN_FAMILY, destinationIdentifier, subscriptionID, KEYSPACE);
        } catch (CassandraDataAccessException e) {
            log.error("error while removing durable topic subscriptions", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNodeDetails(String nodeID, String data) throws AndesException {
        try {
            CQLDataAccessHelper.addMappingToRaw(KEYSPACE, NODE_DETAIL_COLUMN_FAMILY, NODE_DETAIL_ROW, nodeID, data, true);
        } catch (CassandraDataAccessException e) {
            log.error("error while storing node details", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getAllStoredNodeData() throws AndesException {
        try {
            Map<String, String> nodeDetails = new HashMap<String, String>();
            List<Row> values = CQLDataAccessHelper.getStringTypeColumnsInARow(NODE_DETAIL_ROW, null, NODE_DETAIL_COLUMN_FAMILY,
                    KEYSPACE, Long.MAX_VALUE);
            if (values != null) {
                for (Row row : values) {
                    String nodeID = row.getString(CQLDataAccessHelper.MSG_KEY);
                    String value = row.getString(CQLDataAccessHelper.MSG_VALUE);
                    nodeDetails.put(nodeID, value);
                }
            }


            return nodeDetails;
        } catch (CassandraDataAccessException e) {
            log.error("error while retrieving all node data", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNodeData(String nodeID) throws AndesException {
        try {
            CQLDataAccessHelper.deleteStringColumnFromRaw(NODE_DETAIL_COLUMN_FAMILY, NODE_DETAIL_ROW, nodeID, KEYSPACE);
        } catch (CassandraDataAccessException e) {
            log.error("error while removing node data", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageCounterForQueue(String destinationQueueName) throws AndesException {
        try {
            CQLDataAccessHelper.insertCounterColumn(MESSAGE_COUNTERS_COLUMN_FAMILY, MESSAGE_COUNTERS_RAW_NAME, destinationQueueName, KEYSPACE);
        } catch (CassandraDataAccessException e) {
            log.error("error while adding message counter to cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {
        long msgCount;
        try {
            msgCount = CQLDataAccessHelper.getCountValue(CassandraConstants.KEYSPACE, CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY, destinationQueueName,
                                                         CassandraConstants.MESSAGE_COUNTERS_RAW_NAME);
        } catch (Exception e) {
            log.error("Error while getting message count for queue " + destinationQueueName);
            throw new AndesException(e);
        }
        return msgCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeMessageCounterForQueue(String destinationQueueName) throws AndesException {
        try {
            CQLDataAccessHelper.removeCounterColumn(MESSAGE_COUNTERS_COLUMN_FAMILY, MESSAGE_COUNTERS_RAW_NAME, destinationQueueName, KEYSPACE);
        } catch (CassandraDataAccessException e) {
            log.error("error while removing message counter to cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void incrementMessageCountForQueue(String destinationQueueName, long incrementBy) throws AndesException {

        try {
            CQLDataAccessHelper.incrementCounter(destinationQueueName, CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY, CassandraConstants.MESSAGE_COUNTERS_RAW_NAME, CassandraConstants.KEYSPACE, incrementBy);
        } catch (CassandraDataAccessException e) {
            log.error("Error while incrementing message counter", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void decrementMessageCountForQueue(String destinationQueueName, long decrementBy) throws AndesException {

        try {
            CQLDataAccessHelper.decrementCounter(destinationQueueName, CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY, CassandraConstants.MESSAGE_COUNTERS_RAW_NAME,
                                                 CassandraConstants.KEYSPACE, decrementBy);
        } catch (CassandraDataAccessException e) {
            log.error("Error while decrementing message counter", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeExchangeInformation(String exchangeName, String exchangeInfo) throws AndesException {
        try {
            CQLDataAccessHelper.addMappingToRaw(KEYSPACE, EXCHANGE_COLUMN_FAMILY, EXCHANGE_ROW, exchangeName, exchangeInfo, true);
        } catch (CassandraDataAccessException e) {
            log.error("error while storing exchange information to cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesExchange> getAllExchangesStored() throws AndesException {
        try {
            List<AndesExchange> exchanges = new ArrayList<AndesExchange>();

            List<Row> rows = CQLDataAccessHelper.
                    getStringTypeColumnsInARow(EXCHANGE_ROW, null, EXCHANGE_COLUMN_FAMILY, KEYSPACE, Long.MAX_VALUE);
            for (Row row : rows) {
                String encodedStr = row.getString(CQLDataAccessHelper.MSG_VALUE);
                exchanges.add(new AndesExchange(encodedStr));
            }
            return exchanges;

        } catch (CassandraDataAccessException e) {
            log.error("error while reading exchange information to cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteExchangeInformation(String exchangeName) throws AndesException {
        try {
            CQLDataAccessHelper.deleteStringColumnFromRaw(EXCHANGE_COLUMN_FAMILY, EXCHANGE_ROW, exchangeName, KEYSPACE);
        } catch (CassandraDataAccessException e) {
            log.error("error while deleting exchange information to cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeQueueInformation(String queueName, String queueInfo) throws AndesException {
        try {
            CQLDataAccessHelper.addMappingToRaw(KEYSPACE, QUEUE_COLUMN_FAMILY, QUEUE_ROW, queueName, queueInfo, true);
        } catch (CassandraDataAccessException e) {
            log.error("error while storing queue information to cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesQueue> getAllQueuesStored() throws AndesException {
        try {
            List<AndesQueue> queues = new ArrayList<AndesQueue>();
            List<Row> rows = CQLDataAccessHelper.
                    getStringTypeColumnsInARow(QUEUE_ROW, null, QUEUE_COLUMN_FAMILY, KEYSPACE, Long.MAX_VALUE);
            for (Row row : rows) {
                String encodedStr = row.getString(CQLDataAccessHelper.MSG_VALUE);
                queues.add(new AndesQueue(encodedStr));
            }
            return queues;

        } catch (CassandraDataAccessException e) {
            log.error("error while reading queue information to cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteQueueInformation(String queueName) throws AndesException {
        try {
            CQLDataAccessHelper.deleteStringColumnFromRaw(QUEUE_COLUMN_FAMILY, QUEUE_ROW, queueName, KEYSPACE);
        } catch (CassandraDataAccessException e) {
            log.error("error while deleting queue information to cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeBindingInformation(String exchange, String boundQueueName, String bindingInfo) throws AndesException {
        try {
            CQLDataAccessHelper.addMappingToRaw(KEYSPACE, BINDING_COLUMN_FAMILY, exchange, boundQueueName, bindingInfo, true);
        } catch (CassandraDataAccessException e) {
            log.error("error while storing binding information to cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesBinding> getBindingsStoredForExchange(String exchangeName) throws AndesException {
        try {
            List<AndesBinding> bindings = new ArrayList<AndesBinding>();
            List<Row> rows = CQLDataAccessHelper.
                    getStringTypeColumnsInARow(exchangeName, null, BINDING_COLUMN_FAMILY, KEYSPACE, Long.MAX_VALUE);
            for (Row row : rows) {
                String encodedStr = row.getString(CQLDataAccessHelper.MSG_VALUE);
                bindings.add(new AndesBinding(encodedStr));
            }
            return bindings;

        } catch (CassandraDataAccessException e) {
            log.error("error while reading queue information to cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteBindingInformation(String exchangeName, String boundQueueName) throws AndesException {
        try {
            CQLDataAccessHelper.deleteStringColumnFromRaw(BINDING_COLUMN_FAMILY, exchangeName, boundQueueName, KEYSPACE);
        } catch (CassandraDataAccessException e) {
            log.error("error while deleting queue information to cassandra context store", e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        connection.close();
    }
}
