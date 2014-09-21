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

package org.wso2.andes.store.jdbc;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class H2BasedAndesContextStoreImpl implements AndesContextStore {

    private static final Logger logger = Logger.getLogger(H2BasedAndesContextStoreImpl.class);

    private DataSource datasource;
    private final boolean isInMemoryMode;

    H2BasedAndesContextStoreImpl(boolean isInMemoryMode) {
        this.isInMemoryMode = isInMemoryMode;
    }

    @Override
    public void init(DurableStoreConnection connection) throws AndesException {
        JDBCConnection JDBCConnection = new JDBCConnection(isInMemoryMode);
        JDBCConnection.initialize(null);
        datasource = JDBCConnection.getDatasource();
    }

    @Override
    public Map<String, List<String>> getAllStoredDurableSubscriptions() throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Map<String, List<String>> subscriberMap = new HashMap<String, List<String>>();
        try {

            connection = getConnection();
            preparedStatement = connection.prepareStatement(JDBCConstants
                    .PS_SELECT_ALL_DURABLE_SUBSCRIPTIONS);
            resultSet = preparedStatement.executeQuery();

            // create Subscriber Map
            while (resultSet.next()) {
                String destinationId = resultSet.getString(JDBCConstants.DESTINATION_IDENTIFIER);
                List<String> subscriberList = subscriberMap.get(destinationId);

                // if no entry in map create list and put into map
                if (subscriberList == null) {
                    subscriberList = new ArrayList<String>();
                    subscriberMap.put(destinationId, subscriberList);
                }
                // add subscriber data to list
                subscriberList.add(resultSet.getString(JDBCConstants.DURABLE_SUB_DATA));
            }
            return subscriberMap;

        } catch (SQLException e) {
            throw new AndesException(
                    "Error occurred while " + JDBCConstants.TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION);
        } finally {
            close(resultSet, JDBCConstants.TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION);
            close(preparedStatement, JDBCConstants.TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION);
            close(connection, JDBCConstants.TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION);
        }
    }

    @Override
    public void storeDurableSubscription(String destinationIdentifier, String subscriptionID,
                                         String subscriptionEncodeAsStr) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(
                    JDBCConstants.PS_INSERT_DURABLE_SUBSCRIPTION);

            preparedStatement.setString(1, destinationIdentifier);
            preparedStatement.setString(2, subscriptionID);
            preparedStatement.setString(3, subscriptionEncodeAsStr);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_STORING_DURABLE_SUBSCRIPTION);
            throw new AndesException("Error occurred while storing durable subscription. sub id: "
                    + subscriptionID + " destination identifier: " + destinationIdentifier, e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_STORING_DURABLE_SUBSCRIPTION);
            close(connection, JDBCConstants.TASK_STORING_DURABLE_SUBSCRIPTION);
        }
    }

    @Override
    public void removeDurableSubscription(String destinationIdentifier, String subscriptionID) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String task = JDBCConstants.TASK_REMOVING_DURABLE_SUBSCRIPTION + "destination: " +
                destinationIdentifier + " sub id: " + subscriptionID;
        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants
                    .PS_DELETE_DURABLE_SUBSCRIPTION);

            preparedStatement.setString(1, destinationIdentifier);
            preparedStatement.setString(2, subscriptionID);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLException e) {
            rollback(connection, task);
            throw new AndesException("error occurred while " + task, e);
        } finally {

            close(preparedStatement, task);
            close(connection, task);
        }
    }

    @Override
    public void storeNodeDetails(String nodeID, String data) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        // task in progress that's logged on an exception
        String task = JDBCConstants.TASK_STORING_NODE_INFORMATION + "node id: " + nodeID;
        try {
            // done as a transaction
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_INSERT_NODE_INFO);
            preparedStatement.setString(1, nodeID);
            preparedStatement.setString(2, data);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLException e) {
            rollback(connection, task);
            throw new AndesException("Error occurred while " + task);
        } finally {
            close(preparedStatement, task);
            close(connection, task);
        }
    }

    @Override
    public Map<String, String> getAllStoredNodeData() throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Map<String, String> nodeInfoMap = new HashMap<String, String>();
        try {

            connection = getConnection();
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_SELECT_ALL_NODE_INFO);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                nodeInfoMap.put(
                        resultSet.getString(JDBCConstants.NODE_ID),
                        resultSet.getString(JDBCConstants.NODE_INFO)
                );
            }

            return nodeInfoMap;
        } catch (SQLException e) {
            throw new AndesException("Error occurred while " + JDBCConstants
                    .TASK_RETRIEVING_ALL_NODE_DETAILS);
        } finally {
            close(resultSet, JDBCConstants.TASK_RETRIEVING_ALL_NODE_DETAILS);
            close(preparedStatement, JDBCConstants.TASK_RETRIEVING_ALL_NODE_DETAILS);
            close(connection, JDBCConstants.TASK_RETRIEVING_ALL_NODE_DETAILS);
        }
    }

    @Override
    public void removeNodeData(String nodeID) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String task = JDBCConstants.TASK_REMOVING_NODE_INFORMATION + " node id: " + nodeID;
        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_DELETE_NODE_INFO);
            preparedStatement.setString(1, nodeID);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLException e) {
            rollback(connection, task);
            throw new AndesException("Error occurred while " + task, e);
        } finally {
            close(preparedStatement, task);
            close(connection, task);
        }
    }

    @Override
    public void addMessageCounterForQueue(String destinationQueueName) throws AndesException {
        // todo: no counters needed in JDBC.
        // NOTE: In a case of a JDBC context store and a cassandra message store how to update
        // message count for queues?

    }

    @Override
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {
        // todo: no counters needed in JDBC
        // NOTE: In a case of a JDBC context store and a cassandra message store how to update
        // message count for queues? Separate call for each count update?
        return 0;
    }

    @Override
    public void removeMessageCounterForQueue(String destinationQueueName) throws AndesException {
        // todo: no counters needed in JDBC
        // NOTE: In a case of a JDBC context store and a cassandra message store how to update
        // message count for queues?
    }

    @Override
    public void storeExchangeInformation(String exchangeName, String exchangeInfo) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_STORE_EXCHANGE_INFO);
            preparedStatement.setString(1, exchangeName);
            preparedStatement.setString(2, exchangeInfo);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_STORING_EXCHANGE_INFORMATION);
            throw new AndesException("Error occurred while " + JDBCConstants
                    .TASK_STORING_EXCHANGE_INFORMATION + " exchange: " + exchangeName, e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_STORING_EXCHANGE_INFORMATION);
            close(connection, JDBCConstants.TASK_STORING_EXCHANGE_INFORMATION);
        }
    }

    @Override
    public List<AndesExchange> getAllExchangesStored() throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            List<AndesExchange> exchangeList = new ArrayList<AndesExchange>();

            connection = getConnection();
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_SELECT_ALL_EXCHANGE_INFO);
            resultSet = preparedStatement.executeQuery();

            // traverse the result set and add it to exchange list and return the list
            while (resultSet.next()) {
                AndesExchange andesExchange = new AndesExchange(
                        resultSet.getString(JDBCConstants.EXCHANGE_DATA)
                );
                exchangeList.add(andesExchange);
            }
            return exchangeList;
        } catch (SQLException e) {
            throw new AndesException("Error occurred while " + JDBCConstants
                    .TASK_RETRIEVING_ALL_EXCHANGE_INFO, e);
        } finally {
            close(resultSet, JDBCConstants.TASK_RETRIEVING_ALL_EXCHANGE_INFO);
            close(preparedStatement, JDBCConstants.TASK_RETRIEVING_ALL_EXCHANGE_INFO);
            close(connection, JDBCConstants.TASK_RETRIEVING_ALL_EXCHANGE_INFO);
        }
    }

    @Override
    public void deleteExchangeInformation(String exchangeName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_DELETE_EXCHANGE);
            preparedStatement.setString(1, exchangeName);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLException e) {
            String errMsg = JDBCConstants.TASK_DELETING_EXCHANGE + " exchange: " + exchangeName;
            rollback(connection, errMsg);
            throw new AndesException(errMsg, e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_DELETING_EXCHANGE);
            close(connection, JDBCConstants.TASK_DELETING_EXCHANGE);
        }

    }

    @Override
    public void storeQueueInformation(String queueName, String queueInfo) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_INSERT_QUEUE_INFO);
            preparedStatement.setString(1, queueName);
            preparedStatement.setString(2, queueInfo);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            String errMsg = JDBCConstants.TASK_STORING_QUEUE_INFO + " queue name:" + queueName;
            rollback(connection, errMsg);
            throw new AndesException("Error occurred while " + errMsg, e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_STORING_QUEUE_INFO);
            close(connection, JDBCConstants.TASK_STORING_QUEUE_INFO);
        }
    }

    @Override
    public List<AndesQueue> getAllQueuesStored() throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_SELECT_ALL_QUEUE_INFO);
            resultSet = preparedStatement.executeQuery();

            List<AndesQueue> queueList = new ArrayList<AndesQueue>();
            // iterate through the result set and add to queue list
            while (resultSet.next()) {
                AndesQueue andesQueue = new AndesQueue(
                        resultSet.getString(JDBCConstants.QUEUE_INFO)
                );
                queueList.add(andesQueue);
            }

            return queueList;
        } catch (SQLException e) {
            throw new AndesException("Error occurred while " + JDBCConstants.TASK_RETRIEVING_ALL_QUEUE_INFO);
        } finally {
            close(resultSet, JDBCConstants.TASK_RETRIEVING_ALL_QUEUE_INFO);
            close(preparedStatement, JDBCConstants.TASK_RETRIEVING_ALL_QUEUE_INFO);
            close(connection, JDBCConstants.TASK_RETRIEVING_ALL_QUEUE_INFO);
        }
    }

    @Override
    public void deleteQueueInformation(String queueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_DELETE_QUEUE_INFO);
            preparedStatement.setString(1, queueName);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLException e) {
            String errMsg = JDBCConstants.TASK_DELETING_QUEUE_INFO + "queue name: " + queueName;
            rollback(connection, errMsg);
            throw new AndesException("Error occurred while " + errMsg, e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_DELETING_QUEUE_INFO);
            close(connection, JDBCConstants.TASK_DELETING_QUEUE_INFO);
        }
    }

    @Override
    public void storeBindingInformation(String exchange, String boundQueueName, String routingKey) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_INSERT_BINDING);
            preparedStatement.setString(1, exchange);
            preparedStatement.setString(2, boundQueueName);
            preparedStatement.setString(3, routingKey);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLException e) {
            String errMsg = JDBCConstants.TASK_STORING_BINDING + " exchange: " + exchange +
                    " queue: " + boundQueueName + " routing key: " + routingKey;
            rollback(connection, errMsg);
            throw new AndesException("Error occurred while " + errMsg, e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_STORING_BINDING);
            close(connection, JDBCConstants.TASK_STORING_BINDING);
        }
    }

    @Override
    public List<AndesBinding> getBindingsStoredForExchange(String exchangeName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            List<AndesBinding> bindingList = new ArrayList<AndesBinding>();
            connection = getConnection();

            // todo test is this join a performance hit?
            preparedStatement = connection.prepareStatement(JDBCConstants
                    .PS_SELECT_BINDINGS_JOIN_QUEUE_INFO);
            preparedStatement.setString(1, exchangeName);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                AndesBinding andesBinding = new AndesBinding(
                        exchangeName,
                        new AndesQueue(resultSet.getString(JDBCConstants.QUEUE_INFO)),
                        resultSet.getString(JDBCConstants.ROUTING_KEY)
                );
                bindingList.add(andesBinding);
            }

            return bindingList;
        } catch (SQLException e) {
            throw new AndesException("Error occurred while " + JDBCConstants.TASK_RETRIEVING_BINDING_INFO, e);
        } finally {
            close(resultSet, JDBCConstants.TASK_RETRIEVING_BINDING_INFO);
            close(preparedStatement, JDBCConstants.TASK_RETRIEVING_BINDING_INFO);
            close(connection, JDBCConstants.TASK_RETRIEVING_BINDING_INFO);
        }
    }

    @Override
    public void deleteBindingInformation(String exchangeName, String boundQueueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_DELETE_BINDING);
            preparedStatement.setString(1, exchangeName);
            preparedStatement.setString(2, boundQueueName);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            String errMsg = JDBCConstants.TASK_DELETING_BINDING + " exchange: " + exchangeName +
                    " bound queue: " + boundQueueName;
            rollback(connection, errMsg);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_DELETING_BINDING);
            close(connection, JDBCConstants.TASK_DELETING_BINDING);
        }
    }

    @Override
    public void close() {
        // nothing to do here.
    }

    private Connection getConnection() throws SQLException {
        return datasource.getConnection();
    }

    /**
     * Closes the provided connection. on failure log the error;
     *
     * @param connection Connection
     */
    private void close(Connection connection, String task) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("Failed to close connection after " + task);
            }
        }
    }

    private void rollback(Connection connection, String task) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
                logger.warn("Rollback failed on " + task);
            }
        }
    }

    private void close(PreparedStatement preparedStatement, String task) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                logger.error("Closing prepared statement failed after " + task);
            }
        }
    }

    private void close(ResultSet resultSet, String task) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                logger.error("Closing result set failed after " + task);
            }
        }
    }
}
