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
 * under the License. and limitations under the License.
 */

package org.wso2.andes.store.jdbc;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.ConfigurationProperties;
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

/**
 * ANSI SQL based Andes Context Store implementation. This is used to persist information of
 * current durable subscription, exchanges, queues and bindings
 */
public class JDBCAndesContextStoreImpl implements AndesContextStore {

    private static final Logger logger = Logger.getLogger(JDBCAndesContextStoreImpl.class);

    /**
     * Connection pooled sql data source object. Used to create connections in method scope
     */
    private DataSource datasource;

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection init(ConfigurationProperties connectionProperties) throws
            AndesException {

        JDBCConnection jdbcConnection = new JDBCConnection();
        jdbcConnection.initialize(connectionProperties);

        datasource = jdbcConnection.getDataSource();
        return jdbcConnection;
    }

    /**
     * {@inheritDoc}
     */
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
                    "Error occurred while " + JDBCConstants
                            .TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION);
        } finally {
            close(resultSet, JDBCConstants.TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION);
            close(preparedStatement, JDBCConstants.TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION);
            close(connection, JDBCConstants.TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION);
        }
    }

    /**
     * {@inheritDoc}
     */
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
                    + subscriptionID + " destination identifier: " +
                    destinationIdentifier,
                    e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_STORING_DURABLE_SUBSCRIPTION);
            close(connection, JDBCConstants.TASK_STORING_DURABLE_SUBSCRIPTION);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDurableSubscription(String destinationIdentifier, String subscriptionID)
            throws AndesException {
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageCounterForQueue(String destinationQueueName) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();

            if (!isCounter4QueueExist(connection, destinationQueueName)) {
                // if queue counter does not exist
                connection.setAutoCommit(false);

                preparedStatement = connection.prepareStatement(JDBCConstants
                        .PS_INSERT_QUEUE_COUNTER);

                preparedStatement.setString(1, destinationQueueName);
                preparedStatement.setLong(2, 0); // initial count is set to zero for parameter two
                preparedStatement.executeUpdate();
                connection.commit();
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("counter for queue: " + destinationQueueName + " already exists.");
                }
            }

        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_ADDING_QUEUE_COUNTER);
            throw new AndesException("Error occurred while " + JDBCConstants
                    .TASK_ADDING_QUEUE_COUNTER);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_ADDING_QUEUE_COUNTER);
            close(connection, JDBCConstants.TASK_ADDING_QUEUE_COUNTER);
        }
    }

    /**
     * Check whether the queue counter already exists. Provided connection is not closed
     *
     * @param connection SQL Connection
     * @param queueName  queue name
     * @return returns true if the queue counter exists
     * @throws AndesException
     */
    private boolean isCounter4QueueExist(Connection connection,
                                         String queueName) throws AndesException {

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            // check if queue already exist
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_SELECT_QUEUE_COUNT);
            preparedStatement.setString(1, queueName);
            resultSet = preparedStatement.executeQuery();

            return resultSet.first();
        } catch (SQLException e) {
            throw new AndesException("Error occurred while " + JDBCConstants
                    .TASK_ADDING_QUEUE_COUNTER);
        } finally {
            close(resultSet, JDBCConstants.TASK_CHECK_QUEUE_COUNTER_EXIST);
            close(preparedStatement, JDBCConstants.TASK_CHECK_QUEUE_COUNTER_EXIST);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_SELECT_QUEUE_COUNT);
            preparedStatement.setString(1, destinationQueueName);

            resultSet = preparedStatement.executeQuery();

            long count = 0;
            if (resultSet.first()) {
                count = resultSet.getLong(JDBCConstants.QUEUE_COUNT);
            }
            return count;
        } catch (SQLException e) {
            throw new AndesException("Error occurred while " + JDBCConstants
                    .TASK_RETRIEVING_QUEUE_COUNT);
        } finally {
            close(resultSet, JDBCConstants.TASK_RETRIEVING_QUEUE_COUNT);
            close(preparedStatement, JDBCConstants.TASK_RETRIEVING_QUEUE_COUNT);
            close(connection, JDBCConstants.TASK_RETRIEVING_QUEUE_COUNT);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeMessageCounterForQueue(String destinationQueueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_DELETE_QUEUE_COUNTER);
            preparedStatement.setString(1, destinationQueueName);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_DELETING_QUEUE_COUNTER);
            throw new AndesException("Error occurred while " + JDBCConstants
                    .TASK_DELETING_QUEUE_COUNTER + " queue: " + destinationQueueName);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_DELETING_QUEUE_COUNTER);
            close(connection, JDBCConstants.TASK_DELETING_QUEUE_COUNTER);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementMessageCountForQueue(String destinationQueueName, long incrementBy)
            throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_INCREMENT_QUEUE_COUNT);
            preparedStatement.setLong(1, incrementBy);
            preparedStatement.setString(2, destinationQueueName);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_INCREMENTING_QUEUE_COUNT);
            throw new AndesException("Error occurred while " + JDBCConstants
                    .TASK_INCREMENTING_QUEUE_COUNT + " queue name: " + destinationQueueName);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_INCREMENTING_QUEUE_COUNT);
            close(connection, JDBCConstants.TASK_INCREMENTING_QUEUE_COUNT);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementMessageCountForQueue(String destinationQueueName, long decrementBy)
            throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_DECREMENT_QUEUE_COUNT);
            preparedStatement.setLong(1, decrementBy);
            preparedStatement.setString(2, destinationQueueName);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_DECREMENTING_QUEUE_COUNT);
            throw new AndesException("Error occurred while " + JDBCConstants
                    .TASK_DECREMENTING_QUEUE_COUNT + " queue name: " + destinationQueueName);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_DECREMENTING_QUEUE_COUNT);
            close(connection, JDBCConstants.TASK_DECREMENTING_QUEUE_COUNT);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeExchangeInformation(String exchangeName, String exchangeInfo)
            throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {

            connection = getConnection();
            // If exchange doesn't exist in DB create exchange
            // NOTE: Qpid tries to create default exchanges at startup. If this
            // is not a vanilla setup DB already have the created exchanges. hence need to check
            // for existence before insertion.
            // This check is done here rather than inside Qpid code that will be updated in
            // future.
            if (!isExchangeExist(connection, exchangeName)) {
                connection.setAutoCommit(false);

                preparedStatement = connection
                        .prepareStatement(JDBCConstants.PS_STORE_EXCHANGE_INFO);
                preparedStatement.setString(1, exchangeName);
                preparedStatement.setString(2, exchangeInfo);
                preparedStatement.executeUpdate();

                connection.commit();
            }
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_STORING_EXCHANGE_INFORMATION);
            throw new AndesException("Error occurred while " + JDBCConstants
                    .TASK_STORING_EXCHANGE_INFORMATION + " exchange: " + exchangeName, e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_STORING_EXCHANGE_INFORMATION);
            close(connection, JDBCConstants.TASK_STORING_EXCHANGE_INFORMATION);
        }
    }

    /**
     * Helper method to check the existence of an exchange in database
     *
     * @param connection   SQL Connection
     * @param exchangeName exchange name to be checked
     * @return return true if exist and wise versa
     * @throws AndesException
     */
    private boolean isExchangeExist(Connection connection, String exchangeName)
            throws AndesException {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(JDBCConstants
                    .PS_SELECT_EXCHANGE);

            preparedStatement.setString(1, exchangeName);
            resultSet = preparedStatement.executeQuery();
            return resultSet.first(); // if present true
        } catch (SQLException e) {
            throw new AndesException("Error occurred retrieving exchange information for" +
                    " exchange: " + exchangeName, e);
        } finally {
            close(resultSet, JDBCConstants.TASK_IS_EXCHANGE_EXIST);
            close(preparedStatement, JDBCConstants.TASK_IS_EXCHANGE_EXIST);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesExchange> getAllExchangesStored() throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            List<AndesExchange> exchangeList = new ArrayList<AndesExchange>();

            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(JDBCConstants.PS_SELECT_ALL_EXCHANGE_INFO);
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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
            throw new AndesException(
                    "Error occurred while " + JDBCConstants.TASK_RETRIEVING_ALL_QUEUE_INFO);
        } finally {
            close(resultSet, JDBCConstants.TASK_RETRIEVING_ALL_QUEUE_INFO);
            close(preparedStatement, JDBCConstants.TASK_RETRIEVING_ALL_QUEUE_INFO);
            close(connection, JDBCConstants.TASK_RETRIEVING_ALL_QUEUE_INFO);
        }
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeBindingInformation(String exchange, String boundQueueName, String bindingInfo)
            throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_INSERT_BINDING);
            preparedStatement.setString(1, exchange);
            preparedStatement.setString(2, boundQueueName);
            preparedStatement.setString(3, bindingInfo);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLException e) {
            String errMsg = JDBCConstants.TASK_STORING_BINDING + " exchange: " + exchange +
                    " queue: " + boundQueueName + " routing key: " + bindingInfo;
            rollback(connection, errMsg);
            throw new AndesException("Error occurred while " + errMsg, e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_STORING_BINDING);
            close(connection, JDBCConstants.TASK_STORING_BINDING);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesBinding> getBindingsStoredForExchange(String exchangeName)
            throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            List<AndesBinding> bindingList = new ArrayList<AndesBinding>();
            connection = getConnection();

            preparedStatement = connection.prepareStatement(JDBCConstants
                    .PS_SELECT_BINDINGS_FOR_EXCHANGE);
            preparedStatement.setString(1, exchangeName);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                AndesBinding andesBinding = new AndesBinding(
                        resultSet.getString(JDBCConstants.BINDING_INFO)
                );
                bindingList.add(andesBinding);
            }

            return bindingList;
        } catch (SQLException e) {
            throw new AndesException(
                    "Error occurred while " + JDBCConstants.TASK_RETRIEVING_BINDING_INFO, e);
        } finally {
            close(resultSet, JDBCConstants.TASK_RETRIEVING_BINDING_INFO);
            close(preparedStatement, JDBCConstants.TASK_RETRIEVING_BINDING_INFO);
            close(connection, JDBCConstants.TASK_RETRIEVING_BINDING_INFO);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteBindingInformation(String exchangeName, String boundQueueName)
            throws AndesException {
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // nothing to do here.
    }

    /**
     * Creates a connection using a thread pooled data source object and returns the connection
     *
     * @return Connection
     * @throws SQLException
     */
    protected Connection getConnection() throws SQLException {
        return datasource.getConnection();
    }

    /**
     * Closes the provided connection. on failure log the error;
     *
     * @param connection Connection
     * @param task       task that was done before closing
     */
    protected void close(Connection connection, String task) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("Failed to close connection after " + task);
            }
        }
    }

    /**
     * On database update failure tries to rollback
     *
     * @param connection database connection
     * @param task       explanation of the task done when the rollback was triggered
     */
    protected void rollback(Connection connection, String task) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
                logger.warn("Rollback failed on " + task);
            }
        }
    }

    /**
     * close the prepared statement resource
     *
     * @param preparedStatement PreparedStatement
     * @param task              task that was done by the closed prepared statement.
     */
    protected void close(PreparedStatement preparedStatement, String task) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                logger.error("Closing prepared statement failed after " + task);
            }
        }
    }

    /**
     * closes the result set resources
     *
     * @param resultSet ResultSet
     * @param task      task that was done by the closed result set.
     */
    protected void close(ResultSet resultSet, String task) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                logger.error("Closing result set failed after " + task);
            }
        }
    }
}
