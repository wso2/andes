/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.store.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.decr;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.incr;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

import java.util.*;

import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.kernel.slot.SlotState;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import org.apache.commons.lang.NotImplementedException;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * CQL 3 based AndesContextStore implementation. This is intended to support Cassandra 2.xx series upwards.
 */
public class CQLBasedAndesContextStoreImpl implements AndesContextStore {

    // Andes Context Store tables
    private static final String DURABLE_SUB_TABLE = "MB_DURABLE_SUBSCRIPTION";
    private static final String BINDINGS_TABLE = "MB_BINDING";
    private static final String QUEUE_COUNTER_TABLE = "MB_QUEUE_COUNTER";
    private static final String QUEUE_EXCHANGE_NODE_TABLE = "MB_QUEUE_EXCHANGE_NODE";

    // Andes Context Store table columns
    private static final String DURABLE_SUB_ID = "SUBSCRIPTION_ID";
    private static final String QUEUE_EXCHANGE_NODE_ROW = "ENTITY_TYPE";
    private static final String QUEUE_EXCHANGE_NODE_IDENTIFIER = "ENTITY_NAME";
    private static final String QUEUE_EXCHANGE_NODE_DETAIL = "ENTITY_DETAIL";
    private static final String QUEUE_ENTITY_TYPE = "MB_QUEUE";
    private static final String EXCHANGE_ENTITY_TYPE = "MB_EXCHANGE";
    private static final String NODE_ENTITY_TYPE = "MB_NODE";

    private static final String DESTINATION_IDENTIFIER = "DESTINATION_IDENTIFIER";
    private static final String DURABLE_SUB_DATA = "SUBSCRIPTION_DATA";
    private static final String BINDING_INFO = "BINDING_DETAIL";
    private static final String BINDING_QUEUE_NAME = "QUEUE_NAME";
    private static final String BINDING_EXCHANGE_NAME = "EXCHANGE_NAME";
    private static final String MESSAGE_COUNT = "MESSAGE_COUNT";
    private static final String QUEUE_NAME = "QUEUE_NAME";

    /**
     * Encapsulates connectivity related state.
     */
    private CQLConnection cqlConnection;
    
    /**
     * Cassandra related configurations.
     */
    private CassandraConfig config;

    /**
     * Helper class providing utility methods required for testing connection status
     */
    private CQLUtils cqlUtils;
    
    /**
     * Constructor.
     */
    public CQLBasedAndesContextStoreImpl() {
        config = new CassandraConfig();
        cqlUtils = new CQLUtils();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection init(ConfigurationProperties connectionProperties) throws AndesException {
        cqlConnection = new CQLConnection();
        cqlConnection.initialize(connectionProperties);
        config.parse(connectionProperties);
        createSchema();
        return cqlConnection;
    }

    /**
     * Creates the schema if not created for AndesContextStore within the given keyspace
     */
    private void createSchema() {
        Session session = cqlConnection.getSession();

        Statement statement = SchemaBuilder.createTable(config.getKeyspace(), QUEUE_EXCHANGE_NODE_TABLE).ifNotExists().
                addPartitionKey(QUEUE_EXCHANGE_NODE_ROW, DataType.text()).
                addClusteringColumn(QUEUE_EXCHANGE_NODE_IDENTIFIER, DataType.text()).
                addColumn(QUEUE_EXCHANGE_NODE_DETAIL, DataType.text()).
                withOptions().gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        session.execute(statement);

        // This is optimised to query from destination identifier. Similar destination identifier specific
        // subscriptions in one Cassandra node.
        // NOTE: To query specifically from SUB_ID there should be a different schema with partition key set to SUB_ID
        statement = SchemaBuilder.createTable(config.getKeyspace(), DURABLE_SUB_TABLE).ifNotExists().
                addPartitionKey(DESTINATION_IDENTIFIER, DataType.text()).
                addClusteringColumn(DURABLE_SUB_ID, DataType.text()).
                addColumn(DURABLE_SUB_DATA, DataType.text()).
                withOptions().gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        session.execute(statement);

        // Schema is optimised for queries related to a given exchange.
        // Bindings for the same exchange resides in a single node.
        statement = SchemaBuilder.createTable(config.getKeyspace(), BINDINGS_TABLE).ifNotExists().
                addPartitionKey(BINDING_EXCHANGE_NAME, DataType.text()).
                addClusteringColumn(BINDING_QUEUE_NAME, DataType.text()).
                addColumn(BINDING_INFO, DataType.text()).
                withOptions().gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        session.execute(statement);

        statement = SchemaBuilder.createTable(config.getKeyspace(), QUEUE_COUNTER_TABLE).ifNotExists().
                addPartitionKey(QUEUE_NAME, DataType.text()).
                addColumn(MESSAGE_COUNT, DataType.counter()).withOptions().
                gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        session.execute(statement);
        
        //Create table required for Cassandra availability tests.
        cqlUtils.createSchema(cqlConnection, config);
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, List<String>> getAllStoredDurableSubscriptions() throws AndesException {

        Statement statement = QueryBuilder.select().
                all().
                from(config.getKeyspace(), DURABLE_SUB_TABLE).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(statement, "retrieving all durable subscriptions");
        Map<String, List<String>> subscriberMap = new HashMap<String, List<String>>();
        for (Row row : resultSet) {
            String destinationId = row.getString(DESTINATION_IDENTIFIER);
            List<String> subscriberList = subscriberMap.get(destinationId);

            // if no entry in map create list and put into map
            if (subscriberList == null) {
                subscriberList = new ArrayList<String>();
                subscriberMap.put(destinationId, subscriberList);
            }
            // add subscriber data to list
            subscriberList.add(row.getString(DURABLE_SUB_DATA));
        }
        return subscriberMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeDurableSubscription(String destinationIdentifier, String subscriptionID, String subscriptionEncodeAsStr)
            throws AndesException {

        Statement statement = QueryBuilder.insertInto(config.getKeyspace(), DURABLE_SUB_TABLE).ifNotExists().
                value(DESTINATION_IDENTIFIER, destinationIdentifier).
                value(DURABLE_SUB_ID, subscriptionID).
                value(DURABLE_SUB_DATA, subscriptionEncodeAsStr).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "storing durable subscription for sub-id " + subscriptionID + " and destination identifier" +
                destinationIdentifier);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDurableSubscription(String destinationIdentifier, String subscriptionID, String subscriptionEncodeAsStr) throws AndesException {
        Statement statement = QueryBuilder.update(config.getKeyspace(), DURABLE_SUB_TABLE).
                with(set(DURABLE_SUB_DATA, subscriptionEncodeAsStr)).
                where(eq(DESTINATION_IDENTIFIER, destinationIdentifier)).
                and(eq(DURABLE_SUB_ID, subscriptionID)).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "Updating durable subscription for sub id " + subscriptionID + " and destination "
                + "identifier " + destinationIdentifier);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDurableSubscription(String destinationIdentifier, String subscriptionID) throws AndesException {

        Statement statement = QueryBuilder.delete().from(config.getKeyspace(), DURABLE_SUB_TABLE).
                where(eq(DESTINATION_IDENTIFIER, destinationIdentifier)).and(eq(DURABLE_SUB_ID, subscriptionID)).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "removing durable subscription with sub-id " + subscriptionID + " and destination " +
                "identifier " + destinationIdentifier);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNodeDetails(String nodeID, String data) throws AndesException {

        Statement statement = QueryBuilder.insertInto(config.getKeyspace(), QUEUE_EXCHANGE_NODE_TABLE).ifNotExists().
                value(QUEUE_EXCHANGE_NODE_ROW, NODE_ENTITY_TYPE).
                value(QUEUE_EXCHANGE_NODE_IDENTIFIER, nodeID).
                value(QUEUE_EXCHANGE_NODE_DETAIL, data).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "storing node information for node " + nodeID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getAllStoredNodeData() throws AndesException {

        Statement statement = QueryBuilder.select().
                column(QUEUE_EXCHANGE_NODE_IDENTIFIER).
                column(QUEUE_EXCHANGE_NODE_DETAIL).
                from(config.getKeyspace(), QUEUE_EXCHANGE_NODE_TABLE).
                where(eq(QUEUE_EXCHANGE_NODE_ROW, NODE_ENTITY_TYPE)).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(statement, "retrieving all stored node information");

        Map<String, String> nodeInfoMap = new HashMap<String, String>();
        for (Row row : resultSet) {
            nodeInfoMap.put(row.getString(QUEUE_EXCHANGE_NODE_IDENTIFIER),
                    row.getString(QUEUE_EXCHANGE_NODE_DETAIL));
        }

        return nodeInfoMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNodeData(String nodeID) throws AndesException {

        Statement statement = QueryBuilder.delete().
                from(config.getKeyspace(), QUEUE_EXCHANGE_NODE_TABLE).
                where(eq(QUEUE_EXCHANGE_NODE_ROW, NODE_ENTITY_TYPE)).
                and(eq(QUEUE_EXCHANGE_NODE_IDENTIFIER, nodeID)).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "removing node information for node id " + nodeID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageCounterForQueue(String destinationQueueName) throws AndesException {

        Statement statement = QueryBuilder.update(config.getKeyspace(), QUEUE_COUNTER_TABLE).
                with(incr(MESSAGE_COUNT, 0L)).
                where(eq(QUEUE_NAME, destinationQueueName)).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "adding message counter for queue " + destinationQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {

        Statement statement = QueryBuilder.select().column(MESSAGE_COUNT).
                from(config.getKeyspace(), QUEUE_COUNTER_TABLE).
                where(eq(QUEUE_NAME, destinationQueueName)).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(statement, "retrieving message count for queue " + destinationQueueName);

        Row row = resultSet.one();
        if (null != row) {
            return row.getLong(MESSAGE_COUNT);
        } else {
            return 0;
        }
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

        Statement statement = QueryBuilder.delete().from(config.getKeyspace(), QUEUE_COUNTER_TABLE).
                where(eq(QUEUE_NAME, destinationQueueName)).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "removing message counter for queue " + destinationQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementMessageCountForQueue(String destinationQueueName, long incrementBy) throws AndesException {

        Statement statement = QueryBuilder.update(config.getKeyspace(), QUEUE_COUNTER_TABLE).
                with(incr(MESSAGE_COUNT, incrementBy)).
                where(eq(QUEUE_NAME, destinationQueueName)).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "incrementing message count for queue " + destinationQueueName + " by " + incrementBy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementMessageCountForQueue(String destinationQueueName, long decrementBy) throws AndesException {

        Statement statement = QueryBuilder.update(config.getKeyspace(), QUEUE_COUNTER_TABLE).
                with(decr(MESSAGE_COUNT, decrementBy)).
                where(eq(QUEUE_NAME, destinationQueueName)).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "decrementing message count for queue " + destinationQueueName + " by " + decrementBy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeExchangeInformation(String exchangeName, String exchangeData) throws AndesException {

        Statement statement = QueryBuilder.insertInto(config.getKeyspace(), QUEUE_EXCHANGE_NODE_TABLE).ifNotExists().
                value(QUEUE_EXCHANGE_NODE_ROW, EXCHANGE_ENTITY_TYPE).
                value(QUEUE_EXCHANGE_NODE_IDENTIFIER, exchangeName).
                value(QUEUE_EXCHANGE_NODE_DETAIL, exchangeData).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "storing exchange information for exchange " + exchangeName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesExchange> getAllExchangesStored() throws AndesException {

        Statement statement = QueryBuilder.select().
                column(QUEUE_EXCHANGE_NODE_DETAIL).
                from(config.getKeyspace(), QUEUE_EXCHANGE_NODE_TABLE).
                where(eq(QUEUE_EXCHANGE_NODE_ROW, EXCHANGE_ENTITY_TYPE)).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(statement, "retrieving all exchange information");

        List<AndesExchange> exchangeList = new ArrayList<AndesExchange>();
        for (Row row : resultSet) {
            exchangeList.add(new AndesExchange(row.getString(QUEUE_EXCHANGE_NODE_DETAIL)));
        }
        return exchangeList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteExchangeInformation(String exchangeName) throws AndesException {

        Statement statement = QueryBuilder.delete().
                from(config.getKeyspace(), QUEUE_EXCHANGE_NODE_DETAIL).
                where(eq(QUEUE_EXCHANGE_NODE_ROW, EXCHANGE_ENTITY_TYPE)).
                and(eq(QUEUE_EXCHANGE_NODE_IDENTIFIER, exchangeName)).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "deleting exchange " + exchangeName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeQueueInformation(String queueName, String queueData) throws AndesException {

        Statement statement = QueryBuilder.insertInto(config.getKeyspace(), QUEUE_EXCHANGE_NODE_TABLE).ifNotExists().
                value(QUEUE_EXCHANGE_NODE_ROW, QUEUE_ENTITY_TYPE).
                value(QUEUE_EXCHANGE_NODE_IDENTIFIER, queueName).
                value(QUEUE_EXCHANGE_NODE_DETAIL, queueData).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "storing queue information for queue " + queueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesQueue> getAllQueuesStored() throws AndesException {

        Statement statement = QueryBuilder.select().
                column(QUEUE_EXCHANGE_NODE_DETAIL).
                from(config.getKeyspace(), QUEUE_EXCHANGE_NODE_TABLE).
                where(eq(QUEUE_EXCHANGE_NODE_ROW, QUEUE_ENTITY_TYPE)).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(statement, "retrieving all queues stored");

        List<AndesQueue> andesQueueList = new ArrayList<AndesQueue>();
        for (Row row : resultSet) {
            andesQueueList.add(new AndesQueue(row.getString(QUEUE_EXCHANGE_NODE_DETAIL)));
        }
        return andesQueueList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteQueueInformation(String queueName) throws AndesException {

        Statement statement = QueryBuilder.delete().
                from(config.getKeyspace(), QUEUE_EXCHANGE_NODE_TABLE).
                where(eq(QUEUE_EXCHANGE_NODE_ROW, QUEUE_ENTITY_TYPE)).
                and(eq(QUEUE_EXCHANGE_NODE_IDENTIFIER, queueName)).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "deleting queue information for queue " + queueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeBindingInformation(String exchange, String boundQueueName, String bindingInfo) throws AndesException {

        Statement statement = QueryBuilder.insertInto(config.getKeyspace(), BINDINGS_TABLE).ifNotExists().
                value(BINDING_EXCHANGE_NAME, exchange).
                value(BINDING_QUEUE_NAME, boundQueueName).
                value(BINDING_INFO, bindingInfo).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "storing binding information for exchange " + exchange +
                " and bound queue " + boundQueueName);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesBinding> getBindingsStoredForExchange(String exchangeName) throws AndesException {

        Statement statement = QueryBuilder.select().
                column(BINDING_INFO).
                from(config.getKeyspace(), BINDINGS_TABLE).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(statement, "retrieving binding list for exchange " + exchangeName);

        List<AndesBinding> andesBindingList = new ArrayList<AndesBinding>();
        for (Row row : resultSet) {
            andesBindingList.add(new AndesBinding(row.getString(BINDING_INFO)));
        }

        return andesBindingList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteBindingInformation(String exchangeName, String boundQueueName) throws AndesException {

        Statement statement = QueryBuilder.delete().from(config.getKeyspace(), BINDINGS_TABLE).
                where(eq(BINDING_EXCHANGE_NAME, exchangeName)).
                and(eq(BINDING_QUEUE_NAME, boundQueueName)).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "deleting binding information for exchange " + exchangeName +
                " bound queue " + boundQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        cqlConnection.close();
    }

    /**
     * Executes the statement using the given session and returns the resultSet.
     * Additionally this catches any NoHostAvailableException or QueryExecutionException thrown by CQL driver and
     * rethrows an AndesException
     *
     * @param statement statement to be executed
     * @param task      description of the task that is done by the statement
     * @return ResultSet
     * @throws AndesException
     */
    private ResultSet execute(Statement statement, String task) throws AndesException {
        try {
            return cqlConnection.getSession().execute(statement);
        } catch (NoHostAvailableException e) {
            throw new AndesException("Error occurred while " + task, e);
        } catch (QueryExecutionException e) {
            throw new AndesException("Error occurred while " + task, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createSlot(long startMessageId, long endMessageId,
                           String storageQueueName, String assignedNodeId) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createSlotAssignment(String nodeId, String queueName, long startMsgId,
                                               long endMsgId) {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Slot selectUnAssignedSlot(String queueName) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getQueueToLastAssignedId(String queueName) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQueueToLastAssignedId(String queueName, long messageId)
            throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getNodeToLastPublishedId(String nodeId) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNodeToLastPublishedId(String nodeId, long messageId)
            throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<String> getMessagePublishedNodes() throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSlotAssignment(long startMessageId, long endMessageId)
            throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSlotAssignmentByQueueName(String nodeId, String queueName)
            throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSlotState(long startMessageId, long endMessageId, SlotState slotState)
            throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Slot getOverlappedSlot(String queueName) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageId(String queueName, long messageId) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<Long> getMessageIds(String queueName) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageId(long messageId) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSlot(long startMessageId, long endMessageId) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSlotsByQueueName(String queueName) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageIdsByQueueName(String queueName) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<Slot> getAssignedSlotsByNodeId(String nodeId) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<Slot> getAllSlotsByQueueName(String queueName) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllQueues() throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOperational(String testString, long testTime) {
        return cqlUtils.isReachable(cqlConnection) && 
               cqlUtils.testInsert(cqlConnection, config, testString, testTime) && 
               cqlUtils.testRead(cqlConnection, config, testString, testTime) &&
               cqlUtils.testDelete(cqlConnection, config, testString, testTime);
        
    }
}
