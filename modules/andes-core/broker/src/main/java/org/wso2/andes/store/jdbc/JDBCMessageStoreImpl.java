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
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * ANSI SQL based message store implementation. Message persistence related methods are implemented
 * in this class.
 */
public class JDBCMessageStoreImpl implements MessageStore {

    private static final Logger logger = Logger.getLogger(JDBCMessageStoreImpl.class);
    /**
     * Cache queue name to queue_id mapping to avoid extra sql queries
     */
    private final Map<String, Integer> queueMap;

    /**
     * Connection pooled data source
     */
    private DataSource datasource;

    public JDBCMessageStoreImpl() {
        queueMap = new ConcurrentHashMap<String, Integer>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection initializeMessageStore(ConfigurationProperties
                                                                 connectionProperties)
            throws AndesException {

        JDBCConnection jdbcConnection = new JDBCConnection();
        // read data source name from config and use
        jdbcConnection.initialize(connectionProperties);
        datasource = jdbcConnection.getDataSource();

        return jdbcConnection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_INSERT_MESSAGE_PART);

            for (AndesMessagePart messagePart : partList) {
                preparedStatement.setLong(1, messagePart.getMessageID());
                preparedStatement.setInt(2, messagePart.getOffSet());
                preparedStatement.setBytes(3, messagePart.getData());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_STORING_MESSAGE_PARTS);
            throw new AndesException("Error occurred while adding message content to DB ", e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_STORING_MESSAGE_PARTS);
            close(connection, JDBCConstants.TASK_STORING_MESSAGE_PARTS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_DELETE_MESSAGE_PARTS);
            for (Long msgId : messageIdList) {
                preparedStatement.setLong(1, msgId);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_DELETING_MESSAGE_PARTS);
            throw new AndesException("Error occurred while deleting messages from DB ", e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_DELETING_MESSAGE_PARTS);
            close(connection, JDBCConstants.TASK_DELETING_MESSAGE_PARTS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {

        AndesMessagePart messagePart = null;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_RETRIEVE_MESSAGE_PART);
            preparedStatement.setLong(1, messageId);
            preparedStatement.setInt(2, offsetValue);
            results = preparedStatement.executeQuery();

            if (results.first()) {
                byte[] b = results.getBytes(JDBCConstants.MESSAGE_CONTENT);
                messagePart = new AndesMessagePart();
                messagePart.setMessageID(messageId);
                messagePart.setData(b);
                messagePart.setDataLength(b.length);
                messagePart.setOffSet(offsetValue);
            }
        } catch (SQLException e) {
            throw new AndesException("Error occurred while retrieving message content from DB" +
                    " [msg_id=" + messageId + "]", e);
        } finally {
            close(results, JDBCConstants.TASK_RETRIEVING_MESSAGE_PARTS);
            close(preparedStatement, JDBCConstants.TASK_RETRIEVING_MESSAGE_PARTS);
            close(connection, JDBCConstants.TASK_RETRIEVING_MESSAGE_PARTS);
        }
        return messagePart;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_INSERT_METADATA);

            for (AndesMessageMetadata metadata : metadataList) {
                addMetadataToBatch(preparedStatement, metadata, metadata.getDestination());
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
            addListToExpiryTable(connection, metadataList);
            connection.commit();

            if (logger.isDebugEnabled()) {
                logger.debug("Metadata list added. Metadata count: " + metadataList.size());
            }
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_ADDING_METADATA_LIST);
            throw new AndesException("Error occurred while inserting metadata list to queues ", e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_ADDING_METADATA_LIST);
            close(connection, JDBCConstants.TASK_ADDING_METADATA_LIST);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaData(AndesMessageMetadata metadata) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_INSERT_METADATA);

            addMetadataToBatch(preparedStatement, metadata, metadata.getDestination());
            preparedStatement.executeBatch();
            preparedStatement.close();
            addToExpiryTable(connection, metadata);
            connection.commit();

            if (logger.isDebugEnabled()) {
                logger.debug("Metadata added: msgID: " + metadata.getMessageID() +
                        " Destination: " + metadata.getDestination());
            }
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_ADDING_METADATA);
            throw new AndesException("Error occurred while inserting message metadata to queue ", e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_ADDING_METADATA);
            close(connection, JDBCConstants.TASK_ADDING_METADATA);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaDataToQueue(String queueName, AndesMessageMetadata metadata)
            throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_INSERT_METADATA);

            // add to metadata table
            addMetadataToBatch(preparedStatement, metadata, queueName);
            preparedStatement.executeBatch();
            preparedStatement.close();
            addToExpiryTable(connection, metadata);

            connection.commit();
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_ADDING_METADATA_TO_QUEUE + queueName);
            throw new AndesException(
                    "Error occurred while inserting message metadata to queue " + queueName, e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_ADDING_METADATA_TO_QUEUE + queueName);
            close(connection, JDBCConstants.TASK_ADDING_METADATA_TO_QUEUE + queueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetadataToQueue(String queueName, List<AndesMessageMetadata> metadataList)
            throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_INSERT_METADATA);

            for (AndesMessageMetadata md : metadataList) {
                addMetadataToBatch(preparedStatement, md, queueName);
            }

            preparedStatement.executeBatch();
            preparedStatement.close();
            addListToExpiryTable(connection, metadataList);

            connection.commit();
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_ADDING_METADATA_LIST_TO_QUEUE + queueName);
            throw new AndesException(
                    "Error occurred while inserting message metadata list to queue " + queueName, e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_ADDING_METADATA_LIST_TO_QUEUE + queueName);
            close(connection, JDBCConstants.TASK_ADDING_METADATA_LIST_TO_QUEUE + queueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetaDataToQueue(long messageId, String currentQueueName, String targetQueueName)
            throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_UPDATE_METADATA_QUEUE);

            preparedStatement.setInt(1, getCachedQueueID(targetQueueName));
            preparedStatement.setLong(2, messageId);
            preparedStatement.setInt(3, getCachedQueueID(currentQueueName));

            preparedStatement.execute();
            preparedStatement.close();

            connection.commit();
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_UPDATING_META_DATA_QUEUE + targetQueueName);
            throw new AndesException(
                    "Error occurred while updating message metadata to destination queue "
                            + targetQueueName, e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_UPDATING_META_DATA_QUEUE + targetQueueName);
            close(connection, JDBCConstants.TASK_UPDATING_META_DATA_QUEUE + targetQueueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws
            AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_UPDATE_METADATA);

            for (AndesMessageMetadata metadata : metadataList) {
                preparedStatement.setInt(1, getCachedQueueID(metadata.getDestination()));
                preparedStatement.setBytes(2, metadata.getMetadata());
                preparedStatement.setLong(3, metadata.getMessageID());
                preparedStatement.setInt(4, getCachedQueueID(currentQueueName));
                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();
            preparedStatement.close();
            addListToExpiryTable(connection, metadataList);

            connection.commit();
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_UPDATING_META_DATA);
            throw new AndesException("Error occurred while updating message metadata list.", e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_UPDATING_META_DATA);
            close(connection, JDBCConstants.TASK_UPDATING_META_DATA);
        }
    }

    /**
     * Adds a single metadata to a batch insert of metadata
     *
     * @param preparedStatement prepared statement to add messages to metadata table
     * @param metadata          AndesMessageMetadata
     * @param queueName         queue to be assigned
     * @throws SQLException
     */
    private void addMetadataToBatch(PreparedStatement preparedStatement,
                                    AndesMessageMetadata metadata,
                                    final String queueName) throws SQLException {
        preparedStatement.setLong(1, metadata.getMessageID());
        preparedStatement.setInt(2, getCachedQueueID(queueName));
        preparedStatement.setBytes(3, metadata.getMetadata());
        preparedStatement.addBatch();
    }

    private void addToExpiryTable(Connection connection, AndesMessageMetadata metadata)
            throws SQLException {

        if (metadata.getExpirationTime() > 0) {
            PreparedStatement preparedStatement = connection
                    .prepareStatement(JDBCConstants.PS_INSERT_EXPIRY_DATA);
            addExpiryTableEntryToBatch(preparedStatement, metadata);
            preparedStatement.executeBatch();
            preparedStatement.close();
        }
    }

    private void addListToExpiryTable(Connection connection, List<AndesMessageMetadata> list)
            throws SQLException {
        PreparedStatement preparedStatement = connection
                .prepareStatement(JDBCConstants.PS_INSERT_EXPIRY_DATA);

        for (AndesMessageMetadata andesMessageMetadata : list) {
            if (andesMessageMetadata.getExpirationTime() > 0) {
                addExpiryTableEntryToBatch(preparedStatement, andesMessageMetadata);
            }
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
    }

    private void addExpiryTableEntryToBatch(PreparedStatement preparedStatement,
                                            AndesMessageMetadata metadata) throws SQLException {
        preparedStatement.setLong(1, metadata.getMessageID());
        preparedStatement.setLong(2, metadata.getExpirationTime());
        preparedStatement.setString(3, metadata.getDestination());
        preparedStatement.addBatch();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessageMetadata getMetaData(long messageId) throws AndesException {
        AndesMessageMetadata md = null;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_SELECT_METADATA);
            preparedStatement.setLong(1, messageId);
            results = preparedStatement.executeQuery();
            if (results.first()) {
                byte[] b = results.getBytes(JDBCConstants.METADATA);
                md = new AndesMessageMetadata(messageId, b, true);
            }
        } catch (SQLException e) {
            throw new AndesException("error occurred while retrieving message " +
                    "metadata for msg id:" + messageId, e);
        } finally {
            String task = JDBCConstants.TASK_RETRIEVING_METADATA + messageId;
            close(results, task);
            close(preparedStatement, task);
            close(connection, task);
        }
        return md;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getMetaDataList(final String queueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException {

        List<AndesMessageMetadata> metadataList = new ArrayList<AndesMessageMetadata>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(JDBCConstants.PS_SELECT_METADATA_RANGE_FROM_QUEUE);
            preparedStatement.setInt(1, getCachedQueueID(queueName));
            preparedStatement.setLong(2, firstMsgId);
            preparedStatement.setLong(3, lastMsgID);

            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                AndesMessageMetadata md = new AndesMessageMetadata(
                        resultSet.getLong(JDBCConstants.MESSAGE_ID),
                        resultSet.getBytes(JDBCConstants.METADATA),
                        true
                );
                metadataList.add(md);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("request: metadata range (" + firstMsgId + " , " + lastMsgID +
                        ") in destination queue " + queueName
                        + "\nresponse: metadata count " + metadataList.size());
            }
        } catch (SQLException e) {
            throw new AndesException("Error occurred while retrieving messages between msg id "
                    + firstMsgId + " and " + lastMsgID + " from queue " + queueName, e);
        } finally {
            String task = JDBCConstants.TASK_RETRIEVING_METADATA_RANGE_FROM_QUEUE + queueName;
            close(resultSet, task);
            close(preparedStatement, task);
            close(connection, task);
        }
        //sort metadata list to increasing oder of message ids
        Collections.sort(metadataList);

        return metadataList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String queueName,
                                                                       long firstMsgId, int count)
            throws AndesException {

        List<AndesMessageMetadata> mdList = new ArrayList<AndesMessageMetadata>(count);
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;
        try {
            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(JDBCConstants.PS_SELECT_METADATA_FROM_QUEUE);
            preparedStatement.setLong(1, firstMsgId - 1);
            preparedStatement.setInt(2, getCachedQueueID(queueName));

            results = preparedStatement.executeQuery();
            int resultCount = 0;
            while (results.next()) {

                if (resultCount == count) {
                    break;
                }

                AndesMessageMetadata md = new AndesMessageMetadata(
                        results.getLong(JDBCConstants.MESSAGE_ID),
                        results.getBytes(JDBCConstants.METADATA),
                        true
                );
                mdList.add(md);
                resultCount++;
            }
        } catch (SQLException e) {
            throw new AndesException("error occurred while retrieving message metadata from queue ",
                    e);
        } finally {
            close(results, JDBCConstants.TASK_RETRIEVING_NEXT_N_METADATA_FROM_QUEUE);
            close(preparedStatement, JDBCConstants.TASK_RETRIEVING_NEXT_N_METADATA_FROM_QUEUE);
            close(connection, JDBCConstants.TASK_RETRIEVING_NEXT_N_METADATA_FROM_QUEUE);
        }
        return mdList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageMetadataFromQueue(final String queueName,
                                               List<AndesRemovableMetadata> messagesToRemove)
            throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            int queueID = getCachedQueueID(queueName);

            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection
                    .prepareStatement(JDBCConstants.PS_DELETE_METADATA_FROM_QUEUE);
            for (AndesRemovableMetadata md : messagesToRemove) {
                preparedStatement.setInt(1, queueID);
                preparedStatement.setLong(2, md.messageID);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();

            if (logger.isDebugEnabled()) {
                logger.debug("Metadata removed. " + messagesToRemove.size() +
                        " metadata from destination " + queueName);
            }
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_DELETING_METADATA_FROM_QUEUE + queueName);
            throw new AndesException("error occurred while deleting message metadata from queue ",
                    e);
        } finally {
            String task = JDBCConstants.TASK_DELETING_METADATA_FROM_QUEUE + queueName;
            close(preparedStatement, task);
            close(connection, task);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {

        // todo: can't we just delete expired messages?
        Connection connection = null;
        List<AndesRemovableMetadata> list = new ArrayList<AndesRemovableMetadata>(limit);
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();

            // get expired message list
            preparedStatement = connection
                    .prepareStatement(JDBCConstants.PS_SELECT_EXPIRED_MESSAGES);
            resultSet = preparedStatement.executeQuery();
            int resultCount = 0;
            while (resultSet.next()) {

                if (resultCount == limit) {
                    break;
                }
                list.add(new AndesRemovableMetadata(
                                resultSet.getLong(JDBCConstants.MESSAGE_ID),
                                resultSet.getString(JDBCConstants.DESTINATION_QUEUE)
                        )
                );
                resultCount++;
            }
            return list;
        } catch (SQLException e) {
            throw new AndesException("error occurred while retrieving expired messages.", e);
        } finally {
            close(resultSet, JDBCConstants.TASK_RETRIEVING_EXPIRED_MESSAGES);
            close(preparedStatement, JDBCConstants.TASK_RETRIEVING_EXPIRED_MESSAGES);
            close(connection, JDBCConstants.TASK_RETRIEVING_EXPIRED_MESSAGES);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

    }

    /**
     * This method is expected to be used in a transaction based update.
     *
     * @param connection       connection to be used
     * @param messagesToRemove AndesRemovableMetadata
     * @throws SQLException
     */
    private void deleteFromExpiryQueue(Connection connection,
                                       List<AndesRemovableMetadata> messagesToRemove)
            throws SQLException {

        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_DELETE_EXPIRY_DATA);
            for (AndesRemovableMetadata md : messagesToRemove) {
                preparedStatement.setLong(1, md.messageID);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();

        } finally {
            close(preparedStatement, JDBCConstants.TASK_DELETING_FROM_EXPIRY_TABLE);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(JDBCConstants.PS_DELETE_EXPIRY_DATA);
            for (Long mid : messagesToRemove) {
                preparedStatement.setLong(1, mid);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection, JDBCConstants.TASK_DELETING_FROM_EXPIRY_TABLE);
            throw new AndesException("error occurred while deleting message metadata " +
                    "from expiration table ", e);
        } finally {
            close(preparedStatement, JDBCConstants.TASK_DELETING_FROM_EXPIRY_TABLE);
            close(connection, JDBCConstants.TASK_DELETING_FROM_EXPIRY_TABLE);
        }
    }

    /**
     * This method caches the queue ids for destination queue names. If queried destination queue is
     * not in cache updates the cache and returns the queue id.
     *
     * @param destinationQueueName queue name
     * @return corresponding queue id for the destination queue. On error -1 is returned
     * @throws SQLException
     */
    private int getCachedQueueID(final String destinationQueueName) throws SQLException {

        // get from map
        Integer id = queueMap.get(destinationQueueName);
        if (id != null) {
            return id;
        }

        // not in map. query from DB (some other node might have created it)
        int queueID = getQueueID(destinationQueueName);

        if (queueID != -1) {
            queueMap.put(destinationQueueName, queueID);
            return queueID;
        }

        // if queue is not available create a queue in DB
        return createNewQueue(destinationQueueName);
    }

    // get queue id through a DB query
    private int getQueueID(final String destinationQueueName) throws SQLException {

        int queueID = -1;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(JDBCConstants.PS_SELECT_QUEUE_ID,
                    Statement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, destinationQueueName);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.first()) {
                queueID = resultSet.getInt(JDBCConstants.QUEUE_ID);
            }
        } catch (SQLException e) {
            logger.error("Error occurred while retrieving destination queue id " +
                    "for destination queue " + destinationQueueName, e);
            throw e;
        } finally {
            String task = JDBCConstants.TASK_RETRIEVING_QUEUE_ID + destinationQueueName;
            close(resultSet, task);
            close(preparedStatement, task);
            close(connection, task);
        }
        return queueID;
    }

    // creates a new queue entry in DB
    private int createNewQueue(final String destinationQueueName) throws SQLException {
        String sqlString = "INSERT INTO " + JDBCConstants.QUEUES_TABLE + " (" + JDBCConstants
                .QUEUE_NAME + ")" +
                " VALUES (?)";
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;
        int queueID = -1;

        try {
            connection = getConnection();

            /* This has been changed to a transaction since in H2 Database this call asynchronously returns if
            transactions are not used, leading to inconsistent DB. this is done to avoid that. */
            connection.setAutoCommit(false);

            preparedStatement = connection
                    .prepareStatement(sqlString, Statement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, destinationQueueName);
            preparedStatement.executeUpdate();

            connection.commit();

            results = preparedStatement.getGeneratedKeys();
            if (results.first()) {
                queueID = results.getInt(1);
            }
            preparedStatement.close();
            if (queueID == -1) {
                logger.warn("Creating queue with queue name " + destinationQueueName + " failed.");
            } else {
                queueMap.put(destinationQueueName, queueID);
            }
        } catch (SQLException e) {
            logger.error(
                    "Error occurred while inserting destination queue [" + destinationQueueName +
                            "] to database ");
            throw e;
        } finally {
            String task = JDBCConstants.TASK_CREATING_QUEUE + destinationQueueName;
            close(results, task);
            close(preparedStatement, task);
            close(connection, task);
        }
        return queueID;
    }

    /**
     * Returns SQL Connection object from connection pooled data source.
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
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime,
                                        boolean isMessageForTopic, String destination)
            throws AndesException {
        // todo: need to be implemented with changes done to topic messages.
    }
}
