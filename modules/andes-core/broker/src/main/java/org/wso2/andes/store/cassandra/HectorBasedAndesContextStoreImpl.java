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

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.wso2.andes.store.cassandra.CassandraConstants.*;

/**
 * Hector based Andes context store implementation.
 */
public class HectorBasedAndesContextStoreImpl implements AndesContextStore {

    /**
     * DurableStoreConnection to connect to Cassandra database
     */
    private HectorConnection hectorConnection;

    /**
     * Cassandra Keyspace
     */
    private Keyspace keyspace;

    /**
     * Set HectorConnection
     */
    public void setHectorConnection(HectorConnection connection) {
        this.hectorConnection = connection;
    }

    public HectorBasedAndesContextStoreImpl() {
        CassandraConfig config = new CassandraConfig();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection init(ConfigurationProperties connectionProperties) throws
            AndesException {
        try {
            if (hectorConnection == null) {
                hectorConnection = new HectorConnection();
                hectorConnection.initialize(connectionProperties);
                keyspace = hectorConnection.getKeySpace();
            }

            int gcGraceSeconds = hectorConnection.getGcGraceSeconds();

            Cluster cluster = hectorConnection.getCluster();

            //Create needed column families
            HectorDataAccessHelper.createColumnFamily(SUBSCRIPTIONS_COLUMN_FAMILY, DEFAULT_KEYSPACE,
                    cluster, CassandraConstants.UTF8_TYPE, gcGraceSeconds);
            HectorDataAccessHelper.createColumnFamily(EXCHANGE_COLUMN_FAMILY, DEFAULT_KEYSPACE, cluster,
                    CassandraConstants.UTF8_TYPE, gcGraceSeconds);
            HectorDataAccessHelper.createColumnFamily(QUEUE_COLUMN_FAMILY, DEFAULT_KEYSPACE, cluster,
                    CassandraConstants.UTF8_TYPE, gcGraceSeconds);
            HectorDataAccessHelper.createColumnFamily(BINDING_COLUMN_FAMILY, DEFAULT_KEYSPACE, cluster,
                    CassandraConstants.UTF8_TYPE, gcGraceSeconds);
            HectorDataAccessHelper.createColumnFamily(NODE_DETAIL_COLUMN_FAMILY, DEFAULT_KEYSPACE, cluster,
                    CassandraConstants.UTF8_TYPE, gcGraceSeconds);

            return hectorConnection;
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
        return HectorDataAccessHelper.listAllStringRows(SUBSCRIPTIONS_COLUMN_FAMILY, keyspace);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeDurableSubscription(String destinationIdentifier, String subscriptionID,
                                         String subscriptionEncodeAsStr) throws AndesException {
        try {
            HectorDataAccessHelper.addMappingToRaw(SUBSCRIPTIONS_COLUMN_FAMILY,
                    destinationIdentifier, subscriptionID, subscriptionEncodeAsStr, keyspace);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while storing durable subscriptions to cassandra " +
                    "context store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDurableSubscription(String destinationIdentifier, String subscriptionID, String subscriptionEncodeAsStr) throws AndesException {
        // updating and inserting in Hector has the same effect
        storeDurableSubscription(destinationIdentifier, subscriptionID, subscriptionEncodeAsStr);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDurableSubscription(String destinationIdentifier,
                                          String subscriptionID) throws AndesException {
        try {
            HectorDataAccessHelper.deleteStringColumnFromRaw(SUBSCRIPTIONS_COLUMN_FAMILY,
                    destinationIdentifier, subscriptionID, keyspace);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while removing durable topic subscriptions", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNodeDetails(String nodeID, String data) throws AndesException {
        try {
            HectorDataAccessHelper.addMappingToRaw(NODE_DETAIL_COLUMN_FAMILY, NODE_DETAIL_ROW,
                    nodeID, data, keyspace);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while storing node details", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getAllStoredNodeData() throws AndesException {
        try {
            Map<String, String> nodeDetails = new HashMap<String, String>();
            ColumnSlice<String, String> values = HectorDataAccessHelper
                    .getStringTypeColumnsInARow(NODE_DETAIL_ROW, NODE_DETAIL_COLUMN_FAMILY,
                            keyspace, Integer.MAX_VALUE);

            if (values != null) {
                for (HColumn<String, String> column : values.getColumns()) {
                    String nodeID = column.getName();
                    String value = column.getValue();
                    nodeDetails.put(nodeID, value);
                }
            }

            return nodeDetails;
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while retrieving all node data", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNodeData(String nodeID) throws AndesException {
        try {
            HectorDataAccessHelper.deleteStringColumnFromRaw(NODE_DETAIL_COLUMN_FAMILY,
                    NODE_DETAIL_ROW, nodeID, keyspace);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while removing node data", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageCounterForQueue(String destinationQueueName) throws AndesException {
        try {
            HectorDataAccessHelper.insertCounterColumn(MESSAGE_COUNTERS_COLUMN_FAMILY,
                    MESSAGE_COUNTERS_RAW_NAME, destinationQueueName, keyspace);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while adding message counter to cassandra context " +
                    "store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {
        long messageCount;
        try {
            messageCount = HectorDataAccessHelper.getCountValue(keyspace,
                    CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY, destinationQueueName,
                    CassandraConstants.MESSAGE_COUNTERS_RAW_NAME);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while getting message count for queue " +
                    destinationQueueName, e);
        }
        return messageCount;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {
        // NOTE: Cassandra counters can't be reset. Deleting the counter and adding it again is not
        // supported by Cassandra
        long count = getMessageCountForQueue(storageQueueName);
        decrementMessageCountForQueue(storageQueueName, count);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeMessageCounterForQueue(String destinationQueueName) throws AndesException {
        try {
            HectorDataAccessHelper.removeCounterColumn(MESSAGE_COUNTERS_COLUMN_FAMILY,
                    MESSAGE_COUNTERS_RAW_NAME, destinationQueueName, keyspace);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while removing message counter to cassandra context " +
                    "store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementMessageCountForQueue(String destinationQueueName,
                                              long incrementBy) throws AndesException {
        try {
            HectorDataAccessHelper.incrementCounter(destinationQueueName,
                    CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY,
                    CassandraConstants.MESSAGE_COUNTERS_RAW_NAME, keyspace, incrementBy);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while incrementing message counter", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementMessageCountForQueue(String destinationQueueName,
                                              long decrementBy) throws AndesException {
        try {
            HectorDataAccessHelper.decrementCounter(destinationQueueName,
                    CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY,
                    CassandraConstants.MESSAGE_COUNTERS_RAW_NAME, keyspace, decrementBy);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while decrementing message counter", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeExchangeInformation(String exchangeName, String exchangeInfo) throws
            AndesException {
        try {
            HectorDataAccessHelper.addMappingToRaw(EXCHANGE_COLUMN_FAMILY, EXCHANGE_ROW,
                    exchangeName, exchangeInfo, keyspace);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while storing exchange information to cassandra " +
                    "context store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesExchange> getAllExchangesStored() throws AndesException {
        try {
            List<AndesExchange> exchanges = new ArrayList<AndesExchange>();

            ColumnSlice<String, String> columns = HectorDataAccessHelper.
                    getStringTypeColumnsInARow(EXCHANGE_ROW, EXCHANGE_COLUMN_FAMILY, keyspace,
                            Integer.MAX_VALUE);

            for (HColumn<String, String> column : columns.getColumns()) {
                String encodedString = column.getValue();
                exchanges.add(new AndesExchange(encodedString));
            }
            return exchanges;

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while reading exchange information to cassandra " +
                    "context store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteExchangeInformation(String exchangeName) throws AndesException {
        try {
            HectorDataAccessHelper.deleteStringColumnFromRaw(EXCHANGE_COLUMN_FAMILY,
                    EXCHANGE_ROW, exchangeName, keyspace);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while deleting exchange information to cassandra " +
                    "context store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeQueueInformation(String queueName, String queueInfo) throws AndesException {
        try {
            HectorDataAccessHelper.addMappingToRaw(QUEUE_COLUMN_FAMILY, QUEUE_ROW, queueName,
                    queueInfo, keyspace);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while storing queue information to cassandra context " +
                    "store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesQueue> getAllQueuesStored() throws AndesException {
        try {
            List<AndesQueue> queues = new ArrayList<AndesQueue>();
            ColumnSlice<String, String> columns = HectorDataAccessHelper.
                    getStringTypeColumnsInARow(QUEUE_ROW, QUEUE_COLUMN_FAMILY, keyspace,
                            Integer.MAX_VALUE);

            for (HColumn<String, String> column : columns.getColumns()) {
                String encodedString = column.getValue();
                queues.add(new AndesQueue(encodedString));
            }

            return queues;

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while reading queue information to cassandra context " +
                    "store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteQueueInformation(String queueName) throws AndesException {
        try {
            HectorDataAccessHelper.deleteStringColumnFromRaw(QUEUE_COLUMN_FAMILY, QUEUE_ROW,
                    queueName, keyspace);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while deleting queue information to cassandra " +
                    "context store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeBindingInformation(String exchange, String boundQueueName,
                                        String bindingInfo) throws AndesException {
        try {
            HectorDataAccessHelper.addMappingToRaw(BINDING_COLUMN_FAMILY, exchange, boundQueueName,
                    bindingInfo, keyspace);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while storing binding information to cassandra " +
                    "context store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesBinding> getBindingsStoredForExchange(String exchangeName) throws
            AndesException {
        try {
            List<AndesBinding> bindings = new ArrayList<AndesBinding>();
            ColumnSlice<String, String> columns = HectorDataAccessHelper.
                    getStringTypeColumnsInARow(exchangeName, BINDING_COLUMN_FAMILY, keyspace,
                            Integer.MAX_VALUE);

            for (HColumn<String, String> column : columns.getColumns()) {
                String encodedString = column.getValue();
                bindings.add(new AndesBinding(encodedString));
            }

            return bindings;

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while reading queue information to cassandra context " +
                    "store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteBindingInformation(String exchangeName, String boundQueueName) throws
            AndesException {
        try {
            HectorDataAccessHelper.deleteStringColumnFromRaw(BINDING_COLUMN_FAMILY, exchangeName,
                    boundQueueName, keyspace);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while deleting queue information to cassandra " +
                    "context store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        hectorConnection.close();
    }
}
