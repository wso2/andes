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

package org.wso2.andes.store.rdbms;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.*;
import org.wso2.andes.matrics.DataAccessMetricsManager;
import org.wso2.andes.matrics.MetricsConstants;
import org.wso2.andes.store.AndesStoreUnavailableException;
import org.wso2.carbon.metrics.manager.Timer.Context;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ANSI SQL based Andes Context Store implementation. This is used to persist information of
 * current durable subscription, exchanges, queues and bindings
 */
public class RDBMSAndesContextStoreImpl implements AndesContextStore {

    private static final Logger logger = Logger.getLogger(RDBMSAndesContextStoreImpl.class);

    /**
     * Connection pooled sql data source object. Used to create connections in method scope
     */
    private DataSource datasource;

    
    /**
     * Contains utils methods related to connection health tests
     */
    private RDBMSStoreUtils rdbmsStoreUtils;
    
    
    public RDBMSAndesContextStoreImpl() {
        rdbmsStoreUtils = new RDBMSStoreUtils();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection init(ConfigurationProperties connectionProperties) throws
            AndesException {

        RDBMSConnection rdbmsConnection = new RDBMSConnection();
        rdbmsConnection.initialize(connectionProperties);

        datasource = rdbmsConnection.getDataSource();
        logger.info("Andes Context Store initialised");
        return rdbmsConnection;
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
        Context contextRead = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_READ, this.getClass()).start();

        try {

            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants
                    .PS_SELECT_ALL_DURABLE_SUBSCRIPTIONS);
            resultSet = preparedStatement.executeQuery();

            // create Subscriber Map
            while (resultSet.next()) {
                String destinationId = resultSet.getString(RDBMSConstants.DESTINATION_IDENTIFIER);
                List<String> subscriberList = subscriberMap.get(destinationId);

                // if no entry in map create list and put into map
                if (subscriberList == null) {
                    subscriberList = new ArrayList<String>();
                    subscriberMap.put(destinationId, subscriberList);
                }
                // add subscriber data to list
                subscriberList.add(resultSet.getString(RDBMSConstants.DURABLE_SUB_DATA));
            }
            return subscriberMap;

        } catch (SQLNonTransientConnectionException sqlConEx) {
            throw new AndesStoreUnavailableException("Error occurred while " +
                                                RDBMSConstants.TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION,
                                                sqlConEx.getSQLState(), sqlConEx);

        } catch (SQLException e) {
            throw new AndesException("Error occurred while " + RDBMSConstants
                    .TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION, e);
        } finally {
            contextRead.stop();
            close(resultSet, RDBMSConstants.TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION);
            close(preparedStatement, RDBMSConstants.TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION);
            close(connection, RDBMSConstants.TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION);
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
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();
        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(
                    RDBMSConstants.PS_INSERT_DURABLE_SUBSCRIPTION);

            preparedStatement.setString(1, destinationIdentifier);
            preparedStatement.setString(2, subscriptionID);
            preparedStatement.setString(3, subscriptionEncodeAsStr);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLNonTransientConnectionException sqlConEx) {
            rollback(connection, RDBMSConstants.TASK_STORING_DURABLE_SUBSCRIPTION);
            throw new AndesStoreUnavailableException("Error occurred while storing durable subscription. sub id: "
                    + subscriptionID + " destination identifier: " + destinationIdentifier,
                                                sqlConEx.getSQLState(), sqlConEx);

        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_STORING_DURABLE_SUBSCRIPTION);
            throw new AndesException("Error occurred while storing durable subscription. sub id: "
                    + subscriptionID + " destination identifier: " + destinationIdentifier, e);
        } finally {
            contextWrite.stop();
            close(preparedStatement, RDBMSConstants.TASK_STORING_DURABLE_SUBSCRIPTION);
            close(connection, RDBMSConstants.TASK_STORING_DURABLE_SUBSCRIPTION);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDurableSubscription(String destinationIdentifier, String subscriptionID, String subscriptionEncodeAsStr) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();
        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(
                    RDBMSConstants.PS_UPDATE_DURABLE_SUBSCRIPTION);

            preparedStatement.setString(1, subscriptionEncodeAsStr);
            preparedStatement.setString(2, destinationIdentifier);
            preparedStatement.setString(3, subscriptionID);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLNonTransientConnectionException sqlConEx) {
            rollback(connection, RDBMSConstants.TASK_UPDATING_DURABLE_SUBSCRIPTION);
            throw new AndesStoreUnavailableException("Error occurred while updating durable subscription. sub id: " +
                                                subscriptionID + " destination identifier: " + destinationIdentifier,
                                                sqlConEx.getSQLState(), sqlConEx);

        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_UPDATING_DURABLE_SUBSCRIPTION);
            throw new AndesException("Error occurred while updating durable subscription. sub id: "
                    + subscriptionID + " destination identifier: " + destinationIdentifier, e);
        } finally {
            contextWrite.stop();
            close(preparedStatement, RDBMSConstants.TASK_UPDATING_DURABLE_SUBSCRIPTION);
            close(connection, RDBMSConstants.TASK_UPDATING_DURABLE_SUBSCRIPTION);
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
        String task = RDBMSConstants.TASK_REMOVING_DURABLE_SUBSCRIPTION + "destination: " +
                destinationIdentifier + " sub id: " + subscriptionID;
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();

        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants
                    .PS_DELETE_DURABLE_SUBSCRIPTION);

            preparedStatement.setString(1, destinationIdentifier);
            preparedStatement.setString(2, subscriptionID);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLNonTransientConnectionException sqlConEx) {
            rollback(connection, task);
            throw new AndesStoreUnavailableException("error occurred while " + task, sqlConEx.getSQLState(), sqlConEx);
        }  catch (SQLException e) {
            rollback(connection, task);
            throw new AndesException("error occurred while " + task, e);
        } finally {
            contextWrite.stop();
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
        String task = RDBMSConstants.TASK_STORING_NODE_INFORMATION + "node id: " + nodeID;

        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();

        try {
            // done as a transaction
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INSERT_NODE_INFO);
            preparedStatement.setString(1, nodeID);
            preparedStatement.setString(2, data);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLNonTransientConnectionException sqlConEx) {
            rollback(connection, task);
            throw new AndesStoreUnavailableException("error occurred while " + task, sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            rollback(connection, task);
            throw new AndesException("Error occurred while " + task, e);
        } finally {
            contextWrite.stop();
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
        Context contextRead = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_READ, this.getClass()).start();

        try {

            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_ALL_NODE_INFO);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                nodeInfoMap.put(
                        resultSet.getString(RDBMSConstants.NODE_ID),
                        resultSet.getString(RDBMSConstants.NODE_INFO)
                );
            }

            return nodeInfoMap;
        } catch (SQLNonTransientConnectionException sqlConEx) {
            throw new AndesStoreUnavailableException("Error occurred while " +
                                                RDBMSConstants.TASK_RETRIEVING_ALL_NODE_DETAILS,
                                                sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            throw new AndesException("Error occurred while " + RDBMSConstants.TASK_RETRIEVING_ALL_NODE_DETAILS, e);
        } finally {
            contextRead.stop();
            close(resultSet, RDBMSConstants.TASK_RETRIEVING_ALL_NODE_DETAILS);
            close(preparedStatement, RDBMSConstants.TASK_RETRIEVING_ALL_NODE_DETAILS);
            close(connection, RDBMSConstants.TASK_RETRIEVING_ALL_NODE_DETAILS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNodeData(String nodeID) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String task = RDBMSConstants.TASK_REMOVING_NODE_INFORMATION + " node id: " + nodeID;
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();

        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_DELETE_NODE_INFO);
            preparedStatement.setString(1, nodeID);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLNonTransientConnectionException sqlConEx) {
            rollback(connection, task);
            throw new AndesStoreUnavailableException("Error occurred while " + task,
                                                sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            rollback(connection, task);
            throw new AndesException("Error occurred while " + task, e);
        } finally {
            contextWrite.stop();
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
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();
        try {
            connection = getConnection();

            if (!isCounter4QueueExist(connection, destinationQueueName)) {
                // if queue counter does not exist
                connection.setAutoCommit(false);

                preparedStatement = connection.prepareStatement(RDBMSConstants
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

        } catch (SQLNonTransientConnectionException sqlConEx) {
            rollback(connection, RDBMSConstants.TASK_ADDING_QUEUE_COUNTER);
            throw new AndesStoreUnavailableException("Error occurred while " + RDBMSConstants.TASK_ADDING_QUEUE_COUNTER,
                                                sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_ADDING_QUEUE_COUNTER);
            throw new AndesException("Error occurred while " + RDBMSConstants
                    .TASK_ADDING_QUEUE_COUNTER, e);
        } finally {
            contextWrite.stop();
            close(preparedStatement, RDBMSConstants.TASK_ADDING_QUEUE_COUNTER);
            close(connection, RDBMSConstants.TASK_ADDING_QUEUE_COUNTER);
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
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_QUEUE_COUNT);
            preparedStatement.setString(1, queueName);
            resultSet = preparedStatement.executeQuery();

            return resultSet.next();
        } catch (SQLNonTransientConnectionException sqlConEx) {
            throw new AndesStoreUnavailableException("Error occurred while " + RDBMSConstants.TASK_ADDING_QUEUE_COUNTER,
                                                sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            throw new AndesException("Error occurred while " + RDBMSConstants
                    .TASK_ADDING_QUEUE_COUNTER, e);
        } finally {
            close(resultSet, RDBMSConstants.TASK_CHECK_QUEUE_COUNTER_EXIST);
            close(preparedStatement, RDBMSConstants.TASK_CHECK_QUEUE_COUNTER_EXIST);
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
        Context contextRead = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_READ, this.getClass()).start();

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_QUEUE_COUNT);
            preparedStatement.setString(1, destinationQueueName);

            resultSet = preparedStatement.executeQuery();

            long count = 0;
            if (resultSet.next()) {
                count = resultSet.getLong(RDBMSConstants.MESSAGE_COUNT);
            }
            return count;
        } catch (SQLNonTransientConnectionException sqlConEx) {
            throw new AndesStoreUnavailableException("Error occurred while " + RDBMSConstants
                                                .TASK_RETRIEVING_QUEUE_COUNT,
                                                sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            throw new AndesException("Error occurred while " + RDBMSConstants
                    .TASK_RETRIEVING_QUEUE_COUNT, e);
        } finally {
            contextRead.stop();
            close(resultSet, RDBMSConstants.TASK_RETRIEVING_QUEUE_COUNT);
            close(preparedStatement, RDBMSConstants.TASK_RETRIEVING_QUEUE_COUNT);
            close(connection, RDBMSConstants.TASK_RETRIEVING_QUEUE_COUNT);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            // RESET the queue counter to 0
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_RESET_QUEUE_COUNT);

            preparedStatement.setString(1, storageQueueName);

            preparedStatement.execute();
            connection.commit();

        } catch (SQLNonTransientConnectionException sqlConEx) {
            rollback(connection, RDBMSConstants.TASK_RESETTING_MESSAGE_COUNTER + storageQueueName);
            throw new AndesStoreUnavailableException("error occurred while resetting message count for queue :" +
                                                storageQueueName, sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_RESETTING_MESSAGE_COUNTER + storageQueueName);
            throw new AndesException("error occurred while resetting message count for queue :" +
                    storageQueueName,e);
        } finally {
            contextWrite.stop();
            String task = RDBMSConstants.TASK_RESETTING_MESSAGE_COUNTER + storageQueueName;
            close(preparedStatement, task);
            close(connection, task);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeMessageCounterForQueue(String destinationQueueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_DELETE_QUEUE_COUNTER);
            preparedStatement.setString(1, destinationQueueName);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLNonTransientConnectionException sqlConEx) {
            rollback(connection, RDBMSConstants.TASK_DELETING_QUEUE_COUNTER);
            throw new AndesStoreUnavailableException("Error occurred while " + RDBMSConstants.TASK_DELETING_QUEUE_COUNTER +
                                                " queue: " + destinationQueueName, sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_DELETING_QUEUE_COUNTER);
            throw new AndesException("Error occurred while " + RDBMSConstants
                    .TASK_DELETING_QUEUE_COUNTER + " queue: " + destinationQueueName, e);
        } finally {
            contextWrite.stop();
            close(preparedStatement, RDBMSConstants.TASK_DELETING_QUEUE_COUNTER);
            close(connection, RDBMSConstants.TASK_DELETING_QUEUE_COUNTER);
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
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INCREMENT_QUEUE_COUNT);
            preparedStatement.setLong(1, incrementBy);
            preparedStatement.setString(2, destinationQueueName);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLNonTransientConnectionException sqlConEx) {
            rollback(connection, RDBMSConstants.TASK_INCREMENTING_QUEUE_COUNT);
            throw new AndesStoreUnavailableException("Error occurred while " + RDBMSConstants.TASK_INCREMENTING_QUEUE_COUNT +
                                                " queue name: " + destinationQueueName, sqlConEx.getSQLState(),
                                                sqlConEx);
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_INCREMENTING_QUEUE_COUNT);
            throw new AndesException("Error occurred while " + RDBMSConstants
                    .TASK_INCREMENTING_QUEUE_COUNT + " queue name: " + destinationQueueName, e);
        } finally {
            contextWrite.stop();
            close(preparedStatement, RDBMSConstants.TASK_INCREMENTING_QUEUE_COUNT);
            close(connection, RDBMSConstants.TASK_INCREMENTING_QUEUE_COUNT);
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
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_DECREMENT_QUEUE_COUNT);
            preparedStatement.setLong(1, decrementBy);
            preparedStatement.setString(2, destinationQueueName);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLNonTransientConnectionException sqlConEx) {
            rollback(connection, RDBMSConstants.TASK_DECREMENTING_QUEUE_COUNT);
            throw new AndesStoreUnavailableException("Error occurred while " + RDBMSConstants.TASK_DECREMENTING_QUEUE_COUNT +
                                                " queue name: " + destinationQueueName, sqlConEx.getSQLState(),
                                                sqlConEx);
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_DECREMENTING_QUEUE_COUNT);
            throw new AndesException("Error occurred while " + RDBMSConstants
                    .TASK_DECREMENTING_QUEUE_COUNT + " queue name: " + destinationQueueName, e);
        } finally {
            contextWrite.stop();
            close(preparedStatement, RDBMSConstants.TASK_DECREMENTING_QUEUE_COUNT);
            close(connection, RDBMSConstants.TASK_DECREMENTING_QUEUE_COUNT);
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
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();
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
                        .prepareStatement(RDBMSConstants.PS_STORE_EXCHANGE_INFO);
                preparedStatement.setString(1, exchangeName);
                preparedStatement.setString(2, exchangeInfo);
                preparedStatement.executeUpdate();

                connection.commit();
            }
        } catch (SQLNonTransientConnectionException sqlConEx) {
            rollback(connection, RDBMSConstants.TASK_STORING_EXCHANGE_INFORMATION);
            throw new AndesStoreUnavailableException("Error occurred while " +
                                                RDBMSConstants.TASK_STORING_EXCHANGE_INFORMATION + " exchange: " +
                                                exchangeName, sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_STORING_EXCHANGE_INFORMATION);
            throw new AndesException("Error occurred while " + RDBMSConstants
                    .TASK_STORING_EXCHANGE_INFORMATION + " exchange: " + exchangeName, e);
        } finally {
            contextWrite.stop();
            close(preparedStatement, RDBMSConstants.TASK_STORING_EXCHANGE_INFORMATION);
            close(connection, RDBMSConstants.TASK_STORING_EXCHANGE_INFORMATION);
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
            preparedStatement = connection.prepareStatement(RDBMSConstants
                    .PS_SELECT_EXCHANGE);

            preparedStatement.setString(1, exchangeName);
            resultSet = preparedStatement.executeQuery();
            return resultSet.next(); // if present true
        } catch (SQLNonTransientConnectionException sqlConEx) {
            throw new AndesStoreUnavailableException("Error occurred retrieving exchange information for" + " exchange: " +
                                                exchangeName, sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            throw new AndesException("Error occurred retrieving exchange information for" + " exchange: " +
                                     exchangeName, e);
        } finally {
            close(resultSet, RDBMSConstants.TASK_IS_EXCHANGE_EXIST);
            close(preparedStatement, RDBMSConstants.TASK_IS_EXCHANGE_EXIST);
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
        Context contextRead = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_READ, this.getClass()).start();
        try {
            List<AndesExchange> exchangeList = new ArrayList<AndesExchange>();

            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_SELECT_ALL_EXCHANGE_INFO);
            resultSet = preparedStatement.executeQuery();

            // traverse the result set and add it to exchange list and return the list
            while (resultSet.next()) {
                AndesExchange andesExchange = new AndesExchange(
                        resultSet.getString(RDBMSConstants.EXCHANGE_DATA)
                );
                exchangeList.add(andesExchange);
            }
            return exchangeList;
        } catch (SQLNonTransientConnectionException sqlConEx) {
            throw new AndesStoreUnavailableException("Error occurred while " +
                                                RDBMSConstants.TASK_RETRIEVING_ALL_EXCHANGE_INFO,
                                                sqlConEx.getSQLState(), sqlConEx);
        }  catch (SQLException e) {
            throw new AndesException("Error occurred while " + RDBMSConstants
                    .TASK_RETRIEVING_ALL_EXCHANGE_INFO, e);
        } finally {
            contextRead.stop();
            close(resultSet, RDBMSConstants.TASK_RETRIEVING_ALL_EXCHANGE_INFO);
            close(preparedStatement, RDBMSConstants.TASK_RETRIEVING_ALL_EXCHANGE_INFO);
            close(connection, RDBMSConstants.TASK_RETRIEVING_ALL_EXCHANGE_INFO);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteExchangeInformation(String exchangeName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();
        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_DELETE_EXCHANGE);
            preparedStatement.setString(1, exchangeName);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLNonTransientConnectionException sqlConEx) {
            String errMsg = RDBMSConstants.TASK_DELETING_EXCHANGE + " exchange: " + exchangeName;
            rollback(connection, errMsg);
            throw new AndesStoreUnavailableException("Error occurred while " + errMsg, sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            String errMsg = RDBMSConstants.TASK_DELETING_EXCHANGE + " exchange: " + exchangeName;
            rollback(connection, errMsg);
            throw new AndesException("Error occurred while " + errMsg, e);
        } finally {
            contextWrite.stop();
            close(preparedStatement, RDBMSConstants.TASK_DELETING_EXCHANGE);
            close(connection, RDBMSConstants.TASK_DELETING_EXCHANGE);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeQueueInformation(String queueName, String queueInfo) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INSERT_QUEUE_INFO);
            preparedStatement.setString(1, queueName);
            preparedStatement.setString(2, queueInfo);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLNonTransientConnectionException sqlConEx) {
            String errMsg = RDBMSConstants.TASK_STORING_QUEUE_INFO + " queue name:" + queueName;
            rollback(connection, errMsg);
            throw new AndesStoreUnavailableException("Error occurred while " + errMsg, sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            String errMsg = RDBMSConstants.TASK_STORING_QUEUE_INFO + " queue name:" + queueName;
            rollback(connection, errMsg);
            throw new AndesException("Error occurred while " + errMsg, e);
        } finally {
            contextWrite.stop();
            close(preparedStatement, RDBMSConstants.TASK_STORING_QUEUE_INFO);
            close(connection, RDBMSConstants.TASK_STORING_QUEUE_INFO);
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
        Context contextRead = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_READ, this.getClass()).start();
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_ALL_QUEUE_INFO);
            resultSet = preparedStatement.executeQuery();

            List<AndesQueue> queueList = new ArrayList<AndesQueue>();
            // iterate through the result set and add to queue list
            while (resultSet.next()) {
                AndesQueue andesQueue = new AndesQueue(
                        resultSet.getString(RDBMSConstants.QUEUE_DATA)
                );
                queueList.add(andesQueue);
            }

            return queueList;
        } catch (SQLNonTransientConnectionException sqlConEx) {
            throw new AndesStoreUnavailableException(
                                                "Error occurred while " + RDBMSConstants.TASK_RETRIEVING_ALL_QUEUE_INFO,
                                                sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            throw new AndesException(
                    "Error occurred while " + RDBMSConstants.TASK_RETRIEVING_ALL_QUEUE_INFO, e);
        } finally {
            contextRead.stop();
            close(resultSet, RDBMSConstants.TASK_RETRIEVING_ALL_QUEUE_INFO);
            close(preparedStatement, RDBMSConstants.TASK_RETRIEVING_ALL_QUEUE_INFO);
            close(connection, RDBMSConstants.TASK_RETRIEVING_ALL_QUEUE_INFO);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteQueueInformation(String queueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();

        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_DELETE_QUEUE_INFO);
            preparedStatement.setString(1, queueName);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLNonTransientConnectionException sqlConEx) {
            String errMsg = RDBMSConstants.TASK_DELETING_QUEUE_INFO + "queue name: " + queueName;
            rollback(connection, errMsg);
            throw new AndesStoreUnavailableException("Error occurred while " + errMsg, sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            String errMsg = RDBMSConstants.TASK_DELETING_QUEUE_INFO + "queue name: " + queueName;
            rollback(connection, errMsg);
            throw new AndesException("Error occurred while " + errMsg, e);
        } finally {
            contextWrite.stop();
            close(preparedStatement, RDBMSConstants.TASK_DELETING_QUEUE_INFO);
            close(connection, RDBMSConstants.TASK_DELETING_QUEUE_INFO);
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
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();

        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INSERT_BINDING);
            preparedStatement.setString(1, exchange);
            preparedStatement.setString(2, boundQueueName);
            preparedStatement.setString(3, bindingInfo);
            preparedStatement.executeUpdate();

            connection.commit();

        } catch (SQLNonTransientConnectionException sqlConEx) {
            String errMsg =
                            RDBMSConstants.TASK_STORING_BINDING + " exchange: " + exchange + " queue: " +
                                    boundQueueName + " routing key: " + bindingInfo;
            rollback(connection, errMsg);
            throw new AndesStoreUnavailableException("Error occurred while " + errMsg, sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            String errMsg = RDBMSConstants.TASK_STORING_BINDING + " exchange: " + exchange +
                    " queue: " + boundQueueName + " routing key: " + bindingInfo;
            rollback(connection, errMsg);
            throw new AndesException("Error occurred while " + errMsg, e);
        } finally {
            contextWrite.stop();
            close(preparedStatement, RDBMSConstants.TASK_STORING_BINDING);
            close(connection, RDBMSConstants.TASK_STORING_BINDING);
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
        Context contextRead = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_READ, this.getClass()).start();

        try {
            List<AndesBinding> bindingList = new ArrayList<AndesBinding>();
            connection = getConnection();

            preparedStatement = connection.prepareStatement(RDBMSConstants
                    .PS_SELECT_BINDINGS_FOR_EXCHANGE);
            preparedStatement.setString(1, exchangeName);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                AndesBinding andesBinding = new AndesBinding(
                        resultSet.getString(RDBMSConstants.BINDING_INFO)
                );
                bindingList.add(andesBinding);
            }

            return bindingList;
        } catch (SQLNonTransientConnectionException sqlConEx) {

            throw new AndesStoreUnavailableException("Error occurred while " + RDBMSConstants.TASK_RETRIEVING_BINDING_INFO,
                                                sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
         throw new AndesException(
                    "Error occurred while " + RDBMSConstants.TASK_RETRIEVING_BINDING_INFO, e);
        } finally {
            contextRead.stop();
            close(resultSet, RDBMSConstants.TASK_RETRIEVING_BINDING_INFO);
            close(preparedStatement, RDBMSConstants.TASK_RETRIEVING_BINDING_INFO);
            close(connection, RDBMSConstants.TASK_RETRIEVING_BINDING_INFO);
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
        Context contextWrite = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DB_WRITE, this.getClass()).start();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_DELETE_BINDING);
            preparedStatement.setString(1, exchangeName);
            preparedStatement.setString(2, boundQueueName);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLNonTransientConnectionException sqlConEx) {
            String errMsg =
                            RDBMSConstants.TASK_DELETING_BINDING + " exchange: " + exchangeName + " bound queue: " +
                                    boundQueueName;
            rollback(connection, errMsg);
            throw new AndesStoreUnavailableException("Error occurred while " + errMsg, sqlConEx.getSQLState(), sqlConEx);
        } catch (SQLException e) {
            String errMsg =
                            RDBMSConstants.TASK_DELETING_BINDING + " exchange: " + exchangeName + " bound queue: " +
                                    boundQueueName;
            rollback(connection, errMsg);
            throw new AndesException("Error occurred while " + errMsg, e);
        } finally {
            contextWrite.stop();
            close(preparedStatement, RDBMSConstants.TASK_DELETING_BINDING);
            close(connection, RDBMSConstants.TASK_DELETING_BINDING);
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
                logger.error("Failed to close connection after " + task, e);
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
            } catch (SQLException e) {
                logger.warn("Rollback failed on " + task, e);
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
                logger.error("Closing prepared statement failed after " + task, e);
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
                logger.error("Closing result set failed after " + task, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOperational(String testString, long testTime) {
        try {
            // Here oder is important
            return rdbmsStoreUtils.testInsert(getConnection(), testString, testTime) &&
                   rdbmsStoreUtils.testRead(getConnection(), testString, testTime) && 
                   rdbmsStoreUtils.testDelete(getConnection(), testString, testTime);
        } catch (SQLException e) {
            return false;
        }
    
    }
}
