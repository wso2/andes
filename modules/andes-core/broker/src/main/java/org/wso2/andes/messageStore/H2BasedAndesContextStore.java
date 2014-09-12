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

package org.wso2.andes.messageStore;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class H2BasedAndesContextStore implements AndesContextStore {

    private static final Logger logger = Logger.getLogger(H2BasedAndesContextStore.class);

    private DataSource datasource;
    private boolean isInMemoryMode;

    H2BasedAndesContextStore(boolean isInMemoryMode) {
        this.isInMemoryMode = isInMemoryMode;
    }

    @Override
    public void init(DurableStoreConnection connection) throws AndesException {
        H2Connection h2Connection = new H2Connection(isInMemoryMode);
        h2Connection.initialize(null);
        datasource = h2Connection.getDatasource();
    }

    @Override
    public Map<String, List<String>> getAllStoredDurableSubscriptions() throws AndesException {
        return null;
    }

    @Override
    public void storeDurableSubscription(String destinationIdentifier, String subscriptionID, String subscriptionEncodeAsStr) throws AndesException {

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

    }

    @Override
    public void storeNodeDetails(String nodeID, String data) throws AndesException {

    }

    @Override
    public Map<String, String> getAllStoredNodeData() throws AndesException {
        return null;
    }

    @Override
    public void removeNodeData(String nodeID) throws AndesException {

    }

    @Override
    public void addMessageCounterForQueue(String destinationQueueName) throws AndesException {

    }

    @Override
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {
        return 0;
    }

    @Override
    public void removeMessageCounterForQueue(String destinationQueueName) throws AndesException {

    }

    @Override
    public void storeExchangeInformation(String exchangeName, String exchangeInfo) throws AndesException {

    }

    @Override
    public List<AndesExchange> getAllExchangesStored() throws AndesException {
        return null;
    }

    @Override
    public void deleteExchangeInformation(String exchangeName) throws AndesException {

    }

    @Override
    public void storeQueueInformation(String queueName, String queueInfo) throws AndesException {

    }

    @Override
    public List<AndesQueue> getAllQueuesStored() throws AndesException {
        return null;
    }

    @Override
    public void deleteQueueInformation(String queueName) throws AndesException {

    }

    @Override
    public void storeBindingInformation(String exchange, String boundQueueName, String routingKey) throws AndesException {

    }

    @Override
    public List<AndesBinding> getBindingsStoredForExchange(String exchangeName) throws AndesException {
        return null;
    }

    @Override
    public void deleteBindingInformation(String exchangeName, String boundQueueName) throws AndesException {

    }

    @Override
    public void close() {

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
