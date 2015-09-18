/*
 * Copyright (c) 2005-2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesExchange;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.kernel.slot.SlotState;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.wso2.andes.store.cassandra.HectorConstants.BINDING_COLUMN_FAMILY;
import static org.wso2.andes.store.cassandra.HectorConstants.EXCHANGE_COLUMN_FAMILY;
import static org.wso2.andes.store.cassandra.HectorConstants.EXCHANGE_ROW;
import static org.wso2.andes.store.cassandra.HectorConstants.MESSAGE_COUNTERS_COLUMN_FAMILY;
import static org.wso2.andes.store.cassandra.HectorConstants.MESSAGE_COUNTERS_RAW_NAME;
import static org.wso2.andes.store.cassandra.HectorConstants.NODE_DETAIL_COLUMN_FAMILY;
import static org.wso2.andes.store.cassandra.HectorConstants.NODE_DETAIL_ROW;
import static org.wso2.andes.store.cassandra.HectorConstants.QUEUE_COLUMN_FAMILY;
import static org.wso2.andes.store.cassandra.HectorConstants.QUEUE_ROW;
import static org.wso2.andes.store.cassandra.HectorConstants.SUBSCRIPTIONS_COLUMN_FAMILY;

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
     * Encapsulates functionality required to test connectivity to cluster
     */
    private HectorUtils hectorUtils;
    
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

            String keyspace = hectorConnection.getKeySpace().getKeyspaceName();

            //Create needed column families
            HectorDataAccessHelper.createColumnFamily(SUBSCRIPTIONS_COLUMN_FAMILY, keyspace,
                    cluster, HectorConstants.UTF8_TYPE, gcGraceSeconds);
            HectorDataAccessHelper.createColumnFamily(EXCHANGE_COLUMN_FAMILY, keyspace, cluster,
                    HectorConstants.UTF8_TYPE, gcGraceSeconds);
            HectorDataAccessHelper.createColumnFamily(QUEUE_COLUMN_FAMILY, keyspace, cluster,
                    HectorConstants.UTF8_TYPE, gcGraceSeconds);
            HectorDataAccessHelper.createColumnFamily(BINDING_COLUMN_FAMILY, keyspace, cluster,
                    HectorConstants.UTF8_TYPE, gcGraceSeconds);
            HectorDataAccessHelper.createColumnFamily(NODE_DETAIL_COLUMN_FAMILY, keyspace, cluster,
                    HectorConstants.UTF8_TYPE, gcGraceSeconds);

            HectorDataAccessHelper.createColumnFamily(HectorConstants.MESSAGE_STORE_STATUS_COLUMN_FAMILY,
                                                      keyspace, cluster,
                                                      HectorConstants.UTF8_TYPE,
                                                      gcGraceSeconds);

            
            hectorUtils = new HectorUtils();
            
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

        try {
            return HectorDataAccessHelper.listAllStringRows(SUBSCRIPTIONS_COLUMN_FAMILY, keyspace);

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while retrieving durable subscriptions to cassandra " +
                    "context store", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getAllDurableSubscriptionsByID() throws AndesException {
        throw new NotImplementedException();
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
    public void updateDurableSubscriptions(Map<String, String> subscriptions) throws AndesException {
        throw new NotImplementedException();
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
            Map<String, String> nodeDetails = new HashMap<>();
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
                    HectorConstants.MESSAGE_COUNTERS_COLUMN_FAMILY, destinationQueueName,
                    HectorConstants.MESSAGE_COUNTERS_RAW_NAME);
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
                    HectorConstants.MESSAGE_COUNTERS_COLUMN_FAMILY,
                    HectorConstants.MESSAGE_COUNTERS_RAW_NAME, keyspace, incrementBy);
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
                    HectorConstants.MESSAGE_COUNTERS_COLUMN_FAMILY,
                    HectorConstants.MESSAGE_COUNTERS_RAW_NAME, keyspace, decrementBy);
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
            List<AndesExchange> exchanges = new ArrayList<>();

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
            List<AndesQueue> queues = new ArrayList<>();
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
            List<AndesBinding> bindings = new ArrayList<>();
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
    public void createSlot(long startMessageId, long endMessageId,
                           String storageQueueName, String assignedNodeId) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createSlotAssignment(String nodeId, String queueId, long startMsgId,
                                               long endMsgId)
            throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Slot selectUnAssignedSlot(String queueName) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getQueueToLastAssignedId(String queueName) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQueueToLastAssignedId(String queueName, long messageId)
            throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getNodeToLastPublishedId(String nodeId) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNodeToLastPublishedId(String nodeId, long messageId)
            throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removePublisherNodeId(String nodeId) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<String> getMessagePublishedNodes() throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSlotAssignment(long startMessageId, long endMessageId)
            throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSlotAssignmentByQueueName(String nodeId, String queueName)
            throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSlotState(long startMessageId, long endMessageId, SlotState slotState)
            throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Slot getOverlappedSlot(String queueName) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageId(String queueName, long messageId) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<Long> getMessageIds(String queueName) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageId(long messageId) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSlot(long startMessageId, long endMessageId) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSlotsByQueueName(String queueName) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageIdsByQueueName(String queueName) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<Slot> getAssignedSlotsByNodeId(String nodeId) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<Slot> getAllSlotsByQueueName(String queueName) throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        hectorConnection.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllQueues() throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override public void clearSlotStorage() throws AndesException {
        throw new UnsupportedOperationException();
    }

    /**
     * TODO: implementation.
     */
    @Override
    public boolean isOperational(String testString, long testTime) {
        return hectorConnection.isReachable() &&
               hectorUtils.testInsert(hectorConnection, testString, testTime) &&
               hectorUtils.testRead(hectorConnection, testString, testTime) &&
               hectorUtils.testDelete(hectorConnection, testString, testTime);
    }
}
