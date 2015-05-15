/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.matrics.DataAccessMatrixManager;
import org.wso2.andes.matrics.MatrixConstants;
import org.wso2.carbon.metrics.manager.Timer.Context;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.wso2.andes.store.rdbms.RDBMSConstants.*;

/**
 * ANSI SQL based message store implementation. Message persistence related methods are implemented
 * in this class.
 */
public class RDBMSMessageStoreImpl implements MessageStore {

    private static final Logger log = Logger.getLogger(RDBMSMessageStoreImpl.class);

    /**
     * Fetch size of a metadata for deleting metadata of a certain queue from DLC
     */
    private static final Integer STANDARD_PAGE_SIZE = 1000;

    /**
     * Cache queue name to queue_id mapping to avoid extra sql queries
     */
    private final Map<String, Integer> queueMap;


    /**
     * Connection pooled data source
     */
    private DataSource datasource;


    /**
     * Partially created prepared statement to retrieve content of multiple messages using IN operator
     * this will be completed on the fly when the request comes
     */
    private static final String PS_SELECT_CONTENT_PART =
            "SELECT " + MESSAGE_CONTENT + ", " + MESSAGE_ID + ", " + MSG_OFFSET +
                    " FROM " + CONTENT_TABLE +
                    " WHERE " + MESSAGE_ID + " IN (";


    public RDBMSMessageStoreImpl() {
        queueMap = new ConcurrentHashMap<String, Integer>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection initializeMessageStore(AndesContextStore contextStore, 
                                                         ConfigurationProperties connectionProperties)
            throws AndesException {

        RDBMSConnection RDBMSConnection = new RDBMSConnection();
        // read data source name from config and use
        RDBMSConnection.initialize(connectionProperties);
        datasource = RDBMSConnection.getDataSource();
        log.info("Message Store initialised");
        return RDBMSConnection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.ADD_MESSAGE_PART, this).start();
        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INSERT_MESSAGE_PART);

            for (AndesMessagePart messagePart : partList) {
                preparedStatement.setLong(1, messagePart.getMessageID());
                preparedStatement.setInt(2, messagePart.getOffSet());
                preparedStatement.setBytes(3, messagePart.getData());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_STORING_MESSAGE_PARTS);
            throw new AndesException("Error occurred while adding message content to DB ", e);
        } finally {

            context.stop();

            close(preparedStatement, RDBMSConstants.TASK_STORING_MESSAGE_PARTS);
            close(connection, RDBMSConstants.TASK_STORING_MESSAGE_PARTS);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param messageIdList
     */
    @Override
    public void deleteMessageParts(Collection<Long> messageIdList) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.DELETE_MESSAGE_PART, this).start();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_DELETE_MESSAGE_PARTS);
            for (Long msgId : messageIdList) {
                preparedStatement.setLong(1, msgId);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_DELETING_MESSAGE_PARTS);
            throw new AndesException("Error occurred while deleting messages from DB ", e);
        } finally {
            context.stop();
            close(preparedStatement, RDBMSConstants.TASK_DELETING_MESSAGE_PARTS);
            close(connection, RDBMSConstants.TASK_DELETING_MESSAGE_PARTS);
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

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.GET_CONTENT, this).start();

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_RETRIEVE_MESSAGE_PART);
            preparedStatement.setLong(1, messageId);
            preparedStatement.setInt(2, offsetValue);
            results = preparedStatement.executeQuery();

            if (results.next()) {
                messagePart = createMessagePart(results, messageId, offsetValue);
            }
        } catch (SQLException e) {
            throw new AndesException("Error occurred while retrieving message content from DB" +
                    " [msg_id=" + messageId + "]", e);
        } finally {
            context.stop();

            close(results, RDBMSConstants.TASK_RETRIEVING_MESSAGE_PARTS);
            close(preparedStatement, RDBMSConstants.TASK_RETRIEVING_MESSAGE_PARTS);
            close(connection, RDBMSConstants.TASK_RETRIEVING_MESSAGE_PARTS);
        }
        return messagePart;
    }

    @Override
    public Map<Long, List<AndesMessagePart>> getContent(List<Long> messageIDList) throws AndesException {

        Map<Long, List<AndesMessagePart>> contentList = new HashMap<Long, List<AndesMessagePart>>(messageIDList.size());

        if (messageIDList.isEmpty()) {
            return contentList;
        }

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.GET_CONTENT_BATCH, this).start();


        try {


            connection = getConnection();
            preparedStatement = connection.prepareStatement(getSelectContentPreparedStmt(messageIDList.size()));
            for (int mesageIDCounter = 0; mesageIDCounter < messageIDList.size(); mesageIDCounter++) {
                preparedStatement.setLong(mesageIDCounter + 1, messageIDList.get(mesageIDCounter));
            }

            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                long messageID = resultSet.getLong(MESSAGE_ID);
                int offset = resultSet.getInt(MSG_OFFSET);
                List<AndesMessagePart> partList = contentList.get(messageID);
                if (null == partList) {
                    partList = new ArrayList<AndesMessagePart>();
                    contentList.put(messageID, partList);
                }
                AndesMessagePart msgPart = createMessagePart(resultSet, messageID, offset);
                partList.add(msgPart);
            }

        } catch (SQLException e) {
            throw new AndesException("Error occurred while retrieving message content from DB for " +
                    messageIDList.size() + " messages ", e);
        } finally {

            context.stop();

            close(connection, TASK_RETRIEVING_CONTENT_FOR_MESSAGES);
            close(preparedStatement, TASK_RETRIEVING_CONTENT_FOR_MESSAGES);
            close(resultSet, TASK_RETRIEVING_CONTENT_FOR_MESSAGES);
        }
        return contentList;
    }


    private AndesMessagePart createMessagePart(ResultSet results, long messageId, int offsetValue) throws SQLException {
        byte[] b = results.getBytes(MESSAGE_CONTENT);
        AndesMessagePart messagePart = new AndesMessagePart();
        messagePart.setMessageID(messageId);
        messagePart.setData(b);
        messagePart.setDataLength(b.length);
        messagePart.setOffSet(offsetValue);

        return messagePart;
    }

    /**
     * Create a prepared statement with given number of ? values set to IN operator
     *
     * @param messageCount number of messages that content need to be retrieved from.
     *                     CONDITION: messageCount > 0
     * @return Prepared Statement
     */
    private String getSelectContentPreparedStmt(int messageCount) {

        StringBuilder stmtBuilder = new StringBuilder(PS_SELECT_CONTENT_PART);
        for (int i = 0; i < messageCount - 1; i++) {
            stmtBuilder.append("?,");
        }

        stmtBuilder.append("?)");
        return stmtBuilder.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.ADD_META_DATA_LIST, this).start();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INSERT_METADATA);

            for (AndesMessageMetadata metadata : metadataList) {
                addMetadataToBatch(preparedStatement, metadata, metadata.getStorageQueueName());
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
            addListToExpiryTable(connection, metadataList);
            connection.commit();

            if (log.isDebugEnabled()) {
                log.debug("Metadata list added. Metadata count: " + metadataList.size());
            }
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_ADDING_METADATA_LIST);
            throw new AndesException("Error occurred while inserting metadata list to queues ", e);
        } finally {
            context.stop();
            close(preparedStatement, RDBMSConstants.TASK_ADDING_METADATA_LIST);
            close(connection, RDBMSConstants.TASK_ADDING_METADATA_LIST);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaData(AndesMessageMetadata metadata) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.ADD_META_DATA, this).start();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INSERT_METADATA);

            addMetadataToBatch(preparedStatement, metadata, metadata.getStorageQueueName());
            preparedStatement.executeBatch();
            preparedStatement.close();
            addToExpiryTable(connection, metadata);
            connection.commit();

            if (log.isDebugEnabled()) {
                log.debug("Metadata added: msgID: " + metadata.getMessageID() +
                        " Destination: " + metadata.getStorageQueueName());
            }
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_ADDING_METADATA);
            throw new AndesException("Error occurred while inserting message metadata to queue ", e);
        } finally {

            context.stop();

            close(preparedStatement, RDBMSConstants.TASK_ADDING_METADATA);
            close(connection, RDBMSConstants.TASK_ADDING_METADATA);
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

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.ADD_META_DATA_TO_QUEUE, this).start();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INSERT_METADATA);

            // add to metadata table
            addMetadataToBatch(preparedStatement, metadata, queueName);
            preparedStatement.executeBatch();
            preparedStatement.close();
            addToExpiryTable(connection, metadata);

            connection.commit();
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_ADDING_METADATA_TO_QUEUE + queueName);
            throw new AndesException(
                    "Error occurred while inserting message metadata to queue " + queueName, e);
        } finally {

            context.stop();

            close(preparedStatement, RDBMSConstants.TASK_ADDING_METADATA_TO_QUEUE + queueName);
            close(connection, RDBMSConstants.TASK_ADDING_METADATA_TO_QUEUE + queueName);
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

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.ADD_META_DATA_TO_QUEUE_LIST, this).start();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INSERT_METADATA);

            for (AndesMessageMetadata md : metadataList) {
                addMetadataToBatch(preparedStatement, md, queueName);
            }

            preparedStatement.executeBatch();
            preparedStatement.close();
            addListToExpiryTable(connection, metadataList);

            connection.commit();
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_ADDING_METADATA_LIST_TO_QUEUE + queueName);
            throw new AndesException(
                    "Error occurred while inserting message metadata list to queue " + queueName, e);
        } finally {

            context.stop();

            close(preparedStatement, RDBMSConstants.TASK_ADDING_METADATA_LIST_TO_QUEUE + queueName);
            close(connection, RDBMSConstants.TASK_ADDING_METADATA_LIST_TO_QUEUE + queueName);
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
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_UPDATE_METADATA_QUEUE);

            preparedStatement.setInt(1, getCachedQueueID(targetQueueName));
            preparedStatement.setLong(2, messageId);
            preparedStatement.setInt(3, getCachedQueueID(currentQueueName));

            preparedStatement.execute();
            preparedStatement.close();

            connection.commit();
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_UPDATING_META_DATA_QUEUE + targetQueueName);
            throw new AndesException("Error occurred while updating message metadata to destination queue "
                    + targetQueueName, e);
        } finally {
            close(preparedStatement, RDBMSConstants.TASK_UPDATING_META_DATA_QUEUE + targetQueueName);
            close(connection, RDBMSConstants.TASK_UPDATING_META_DATA_QUEUE + targetQueueName);
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

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.UPDATE_META_DATA_INFORMATION, this).start();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_UPDATE_METADATA);

            for (AndesMessageMetadata metadata : metadataList) {
                preparedStatement.setInt(1, getCachedQueueID(metadata.getStorageQueueName()));
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
            rollback(connection, RDBMSConstants.TASK_UPDATING_META_DATA);
            throw new AndesException("Error occurred while updating message metadata list.", e);
        } finally {

            context.stop();

            close(preparedStatement, RDBMSConstants.TASK_UPDATING_META_DATA);
            close(connection, RDBMSConstants.TASK_UPDATING_META_DATA);
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


        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.ADD_META_DATA_TO_BATCH, this).start();

        try {
            preparedStatement.setLong(1, metadata.getMessageID());
            preparedStatement.setInt(2, getCachedQueueID(queueName));
            preparedStatement.setBytes(3, metadata.getMetadata());
            preparedStatement.addBatch();
        } finally {
            context.stop();
        }


    }

    /**
     * Add metadata entry to expiry table
     *
     * @param connection SQLConnection. Connection resource is not closed within the method
     * @param metadata   AndesMessageMetadata
     * @throws SQLException
     */
    private void addToExpiryTable(Connection connection, AndesMessageMetadata metadata)
            throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            if (metadata.getExpirationTime() > 0) {
                preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INSERT_EXPIRY_DATA);
                addExpiryTableEntryToBatch(preparedStatement, metadata);
                preparedStatement.executeBatch();
            }
        } finally {
            close(preparedStatement, "adding entry to expiry table");
        }
    }

    /**
     * Add a list of metadata entries to expiry table
     *
     * @param connection SQLConnection. Connection resource is not closed within the method
     * @param list       AndesMessageMetadata list
     * @throws SQLException
     */
    private void addListToExpiryTable(Connection connection, List<AndesMessageMetadata> list)
            throws SQLException {

        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_INSERT_EXPIRY_DATA);

            for (AndesMessageMetadata andesMessageMetadata : list) {
                if (andesMessageMetadata.getExpirationTime() > 0) {
                    addExpiryTableEntryToBatch(preparedStatement, andesMessageMetadata);
                }
            }
            preparedStatement.executeBatch();
        } finally {
            close(preparedStatement, "adding list to expiry table");
        }
    }

    /**
     * Does a batch update on the given prepared statement to add entries to expiry table.
     *
     * @param preparedStatement PreparedStatement. Object is not closed within the method
     * @param metadata          AndesMessageMetadata
     * @throws SQLException
     */
    private void addExpiryTableEntryToBatch(PreparedStatement preparedStatement,
                                            AndesMessageMetadata metadata) throws SQLException {
        preparedStatement.setLong(1, metadata.getMessageID());
        preparedStatement.setLong(2, metadata.getExpirationTime());
        preparedStatement.setString(3, metadata.getStorageQueueName());
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

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.GET_META_DATA, this).start();

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_METADATA);
            preparedStatement.setLong(1, messageId);
            results = preparedStatement.executeQuery();
            if (results.next()) {
                byte[] b = results.getBytes(RDBMSConstants.METADATA);
                md = new AndesMessageMetadata(messageId, b, true);
            }
        } catch (SQLException e) {
            throw new AndesException("error occurred while retrieving message " +
                    "metadata for msg id:" + messageId, e);
        } finally {

            context.stop();

            String task = RDBMSConstants.TASK_RETRIEVING_METADATA + messageId;
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
    public List<AndesMessageMetadata> getMetaDataList(final String storageQueueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException {

        List<AndesMessageMetadata> metadataList = new ArrayList<AndesMessageMetadata>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.GET_META_DATA_LIST, this).start();

        try {
            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_SELECT_METADATA_RANGE_FROM_QUEUE);
            preparedStatement.setInt(1, getCachedQueueID(storageQueueName));
            preparedStatement.setLong(2, firstMsgId);
            preparedStatement.setLong(3, lastMsgID);

            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                AndesMessageMetadata md = new AndesMessageMetadata(
                        resultSet.getLong(RDBMSConstants.MESSAGE_ID),
                        resultSet.getBytes(RDBMSConstants.METADATA),
                        true
                );
                md.setStorageQueueName(storageQueueName);
                metadataList.add(md);
            }
            if (log.isDebugEnabled()) {
                log.debug("request: metadata range (" + firstMsgId + " , " + lastMsgID +
                        ") in destination queue " + storageQueueName
                        + "\nresponse: metadata count " + metadataList.size());
            }
        } catch (SQLException e) {
            throw new AndesException("Error occurred while retrieving messages between msg id "
                    + firstMsgId + " and " + lastMsgID + " from queue " + storageQueueName, e);
        } finally {

            context.stop();

            String task = RDBMSConstants.TASK_RETRIEVING_METADATA_RANGE_FROM_QUEUE + storageQueueName;
            close(resultSet, task);
            close(preparedStatement, task);
            close(connection, task);
        }
        return metadataList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String storageQueueName,
                                                                       long firstMsgId, int count)
            throws AndesException {

        List<AndesMessageMetadata> mdList = new ArrayList<AndesMessageMetadata>(count);
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;


        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.GET_NEXT_MESSAGE_METADATA_FROM_QUEUE,
                this).start();

        try {
            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_SELECT_METADATA_FROM_QUEUE);
            preparedStatement.setLong(1, firstMsgId - 1);
            preparedStatement.setInt(2, getCachedQueueID(storageQueueName));

            results = preparedStatement.executeQuery();
            int resultCount = 0;
            while (results.next()) {

                if (resultCount == count) {
                    break;
                }

                AndesMessageMetadata md = new AndesMessageMetadata(
                        results.getLong(RDBMSConstants.MESSAGE_ID),
                        results.getBytes(RDBMSConstants.METADATA),
                        true
                );
                md.setStorageQueueName(storageQueueName);
                mdList.add(md);
                resultCount++;
            }
        } catch (SQLException e) {
            throw new AndesException("error occurred while retrieving message metadata from queue ",
                    e);
        } finally {

            context.stop();

            close(results, RDBMSConstants.TASK_RETRIEVING_NEXT_N_METADATA_FROM_QUEUE);
            close(preparedStatement, RDBMSConstants.TASK_RETRIEVING_NEXT_N_METADATA_FROM_QUEUE);
            close(connection, RDBMSConstants.TASK_RETRIEVING_NEXT_N_METADATA_FROM_QUEUE);
        }
        return mdList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageMetadataFromQueue(final String storageQueueName,
                                               List<AndesRemovableMetadata> messagesToRemove)
            throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.DELETE_MESSAGE_META_DATA_FROM_QUEUE, this).
                start();

        try {
            int queueID = getCachedQueueID(storageQueueName);

            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_DELETE_METADATA_FROM_QUEUE);
            for (AndesRemovableMetadata md : messagesToRemove) {
                preparedStatement.setInt(1, queueID);
                preparedStatement.setLong(2, md.getMessageID());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();

            if (log.isDebugEnabled()) {
                log.debug("Metadata removed. " + messagesToRemove.size() +
                        " metadata from destination " + storageQueueName);
            }
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_DELETING_METADATA_FROM_QUEUE + storageQueueName);
            throw new AndesException("error occurred while deleting message metadata from queue ",
                    e);
        } finally {

            context.stop();

            String task = RDBMSConstants.TASK_DELETING_METADATA_FROM_QUEUE + storageQueueName;
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
                    .prepareStatement(RDBMSConstants.PS_SELECT_EXPIRED_MESSAGES);
            resultSet = preparedStatement.executeQuery();
            int resultCount = 0;
            while (resultSet.next()) {

                if (resultCount == limit) {
                    break;
                }
                list.add(new AndesRemovableMetadata(
                                resultSet.getLong(RDBMSConstants.MESSAGE_ID),
                                resultSet.getString(RDBMSConstants.DESTINATION_QUEUE),
                                resultSet.getString(RDBMSConstants.DESTINATION_QUEUE)
                        )
                );
                resultCount++;
            }
            return list;
        } catch (SQLException e) {
            throw new AndesException("error occurred while retrieving expired messages.", e);
        } finally {
            close(resultSet, RDBMSConstants.TASK_RETRIEVING_EXPIRED_MESSAGES);
            close(preparedStatement, RDBMSConstants.TASK_RETRIEVING_EXPIRED_MESSAGES);
            close(connection, RDBMSConstants.TASK_RETRIEVING_EXPIRED_MESSAGES);
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
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_DELETE_EXPIRY_DATA);
            for (AndesRemovableMetadata md : messagesToRemove) {
                preparedStatement.setLong(1, md.getMessageID());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();

        } finally {
            close(preparedStatement, RDBMSConstants.TASK_DELETING_FROM_EXPIRY_TABLE);
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

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_DELETE_EXPIRY_DATA);
            for (Long mid : messagesToRemove) {
                preparedStatement.setLong(1, mid);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_DELETING_FROM_EXPIRY_TABLE);
            throw new AndesException("error occurred while deleting message metadata " +
                    "from expiration table ", e);
        } finally {
            close(preparedStatement, RDBMSConstants.TASK_DELETING_FROM_EXPIRY_TABLE);
            close(connection, RDBMSConstants.TASK_DELETING_FROM_EXPIRY_TABLE);
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

        // If not in cached map. query from DB (some other node might have created it)
        // If queue is not available create a queue in DB
        int queueID = getQueueID(destinationQueueName);

        if (queueID != -1) {
            queueMap.put(destinationQueueName, queueID);
        }
        return queueID;
    }

    /**
     * Retrieved the queue ID from DB. If the ID is not present create a new queue and get the id.
     *
     * @param destinationQueueName queue name
     * @return queue id
     * @throws SQLException
     */
    private int getQueueID(final String destinationQueueName) throws SQLException {

        int queueID = -1;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_QUEUE_ID);
            preparedStatement.setString(1, destinationQueueName);
            resultSet = preparedStatement.executeQuery();

            // ResultSet.first() is not supported by MS SQL hence using next()
            if (resultSet.next()) {
                queueID = resultSet.getInt(RDBMSConstants.QUEUE_ID);
            }
            resultSet.close();

            // If queue is not present create a new queue entry
            if (queueID == -1) {
                createNewQueue(connection, destinationQueueName);
            }

            // Get the resultant ID.
            // NOTE: In different DB implementations getting the auto generated queue id differs in subtle ways
            // Hence doing a simple select again after adding the entry to DB
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                queueID = resultSet.getInt(RDBMSConstants.QUEUE_ID);
            }

        } catch (SQLException e) {
            log.error("Error occurred while retrieving destination queue id " +
                    "for destination queue " + destinationQueueName, e);
            throw e;
        } finally {
            String task = RDBMSConstants.TASK_RETRIEVING_QUEUE_ID + destinationQueueName;
            close(resultSet, task);
            close(preparedStatement, task);
            close(connection, task);
        }
        return queueID;
    }

    /**
     * Using the provided connection create a new queue with queue id in database
     *
     * @param connection           Connection
     * @param destinationQueueName queue name
     * @throws SQLException
     */
    private void createNewQueue(final Connection connection, final String destinationQueueName) throws SQLException {

        PreparedStatement preparedStatement = null;

        try {
            boolean isAutoCommit = connection.getAutoCommit();
            // This has been changed to a transaction since in H2 Database this call asynchronously
            // returns if transactions are not used, leading to inconsistent DB. this is done to avoid
            // that.
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INSERT_QUEUE);
            preparedStatement.setString(1, destinationQueueName);
            preparedStatement.executeUpdate();

            connection.commit();
            preparedStatement.close();

            // set the auto commit value back to its previous value
            connection.setAutoCommit(isAutoCommit);
        } catch (SQLException e) {
            log.error("Error occurred while inserting destination queue [" + destinationQueueName +
                    "] to database ");
            throw e;
        } finally {
            String task = RDBMSConstants.TASK_CREATING_QUEUE + destinationQueueName;
            close(preparedStatement, task);
        }
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
                log.error("Failed to close connection after " + task, e);
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
                log.warn("Rollback failed on " + task, e);
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
                log.error("Closing prepared statement failed after " + task, e);
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
                log.error("Closing result set failed after " + task, e);
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
        // NOTE: Feature Message Expiration moved to a future release
    }

    /**
     * {@inheritDoc}
     *
     * @param storageQueueName name of the queue being purged
     * @throws AndesException
     */
    @Override
    public void deleteAllMessageMetadata(String storageQueueName) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            int queueID = getCachedQueueID(storageQueueName);

            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_CLEAR_QUEUE_FROM_METADATA);

            preparedStatement.setInt(1, queueID);

            preparedStatement.execute();
            connection.commit();

            if (log.isDebugEnabled()) {
                log.debug("DELETED all message metadata from " + storageQueueName +
                        " with queue ID " + queueID);
            }
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_DELETING_METADATA_FROM_QUEUE + storageQueueName);
            throw new AndesException("error occurred while clearing message metadata from queue :" +
                    storageQueueName, e);
        } finally {
            String task = RDBMSConstants.TASK_DELETING_METADATA_FROM_QUEUE + storageQueueName;
            close(preparedStatement, task);
            close(connection, task);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getMessageIDsAddressedToQueue(String storageQueueName, Long startMessageID)
            throws AndesException {

        List<Long> messageIDs = new ArrayList<Long>();

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;

        try {
            connection = getConnection();

            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_SELECT_MESSAGE_IDS_FROM_METADATA_FOR_QUEUE);

            preparedStatement.setInt(1, getCachedQueueID(storageQueueName));

            results = preparedStatement.executeQuery();

            while (results.next()) {
                messageIDs.add(results.getLong(RDBMSConstants.MESSAGE_ID));
            }

        } catch (SQLException e) {
            throw new AndesException("Error while getting message IDs for queue : " +
                    storageQueueName, e);
        } finally {
            close(results, RDBMSConstants.TASK_RETRIEVING_NEXT_N_MESSAGE_IDS_OF_QUEUE + storageQueueName);
            close(preparedStatement, RDBMSConstants.TASK_RETRIEVING_NEXT_N_MESSAGE_IDS_OF_QUEUE + storageQueueName);
            close(connection, RDBMSConstants.TASK_RETRIEVING_NEXT_N_MESSAGE_IDS_OF_QUEUE + storageQueueName);
        }

        return messageIDs;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addQueue(String destinationQueueName) throws AndesException {
        // Message count is taken from DB itself. No need to implement this
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String storageQueueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;
        long messageCount = 0;

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_QUEUE_MESSAGE_COUNT);
            preparedStatement.setInt(1, getCachedQueueID(storageQueueName));
            
            results = preparedStatement.executeQuery();
            
            while (results.next()) {
                messageCount = results.getLong(RDBMSConstants.PS_ALIAS_FOR_COUNT);
            }
            
            
        } catch (SQLException e) {
            throw new AndesException("Error while getting message count from queue " +
                    storageQueueName, e);
        } finally {
            close(results, RDBMSConstants.TASK_RETRIEVING_QUEUE_MSG_COUNT + storageQueueName);
            close(preparedStatement, RDBMSConstants.TASK_RETRIEVING_QUEUE_MSG_COUNT + storageQueueName);
            close(connection, RDBMSConstants.TASK_RETRIEVING_QUEUE_MSG_COUNT + storageQueueName);
        }

        return messageCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {
        // Message count is taken from DB itself. No need to implement this
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeQueue(String storageQueueName) throws AndesException {
        queueMap.remove(storageQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementMessageCountForQueue(String destinationQueueName, long incrementBy) throws AndesException {
        // Message count is taken from DB itself. No need to implement this
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementMessageCountForQueue(String destinationQueueName, long decrementBy) throws AndesException {
        // Message count is taken from DB itself. No need to implement this
    }

    /**
     * {@inheritDoc}
     *
     * @param storageQueueName name of the queue being purged
     * @param DLCQueueName     Name of the DLC queue used within the resident tenant.
     * @return number of deleted messages.
     * @throws AndesException
     */
    @Override
    public int deleteAllMessageMetadataFromDLC(String storageQueueName, String DLCQueueName) throws
            AndesException {

        Connection connection;
        PreparedStatement preparedStatement;

        int messageCountInDLCForQueue = 0;

        try {

            Long lastProcessedID = 0l;

            Integer pageSize = STANDARD_PAGE_SIZE;

            int queueID = getCachedQueueID(DLCQueueName);

            connection = getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_DELETE_METADATA_FROM_QUEUE);

            Boolean allRecordsReceived = false;

            while (!allRecordsReceived) {

                List<AndesMessageMetadata> metadataList = getNextNMessageMetadataFromQueue
                        (DLCQueueName, lastProcessedID, pageSize);

                if (metadataList.size() == 0) {
                    allRecordsReceived = true; // this means that there are no more messages to
                    // be retrieved for this queue
                } else {
                    for (AndesMessageMetadata amm : metadataList) {
                        if (storageQueueName.equals(amm.getDestination())) {
                            preparedStatement.setInt(1, queueID);
                            preparedStatement.setLong(2, amm.getMessageID());
                            preparedStatement.addBatch();
                        }
                    }

                    lastProcessedID = metadataList.get(metadataList.size() - 1).getMessageID();

                    if (metadataList.size() < pageSize) {
                        // again means there are no more metadata to be retrieved
                        allRecordsReceived = true;
                    }
                }
            }
        } catch (SQLException e) {
            // This could be thrown only from the while loop reading messages in DLC.
            log.error("Error while deleting messages in DLC for queue : " + storageQueueName, e);
            throw new AndesException("Error while deleting messages in DLC for queue : " + storageQueueName, e);
        }

        // Execute Batch Delete
        try {
            preparedStatement.executeBatch();
            connection.commit();

            if (log.isDebugEnabled()) {
                log.debug("Removed. " + messageCountInDLCForQueue +
                        " messages from DLC for destination " + storageQueueName);
            }
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_DELETING_METADATA_FROM_QUEUE + storageQueueName);
            throw new AndesException("Error occurred while deleting message metadata from queue :" + storageQueueName, e);
        } finally {
            String task = RDBMSConstants.TASK_DELETING_METADATA_FROM_QUEUE + storageQueueName;
            close(preparedStatement, task);
            close(connection, task);
        }

        return messageCountInDLCForQueue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeRetainedMessages(List<AndesMessage> retainList) throws AndesException {

        Connection connection = null;

        PreparedStatement updateMetadataPreparedStatement = null;
        PreparedStatement deleteContentPreparedStatement = null;
        PreparedStatement insertContentPreparedStatement = null;

        boolean batchEmpty = true;

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            updateMetadataPreparedStatement = connection.prepareStatement(
                                                         RDBMSConstants.PS_UPDATE_RETAINED_METADATA);
            deleteContentPreparedStatement = connection.prepareStatement(
                                                         RDBMSConstants.PS_DELETE_RETAIN_MESSAGE_PARTS);
            insertContentPreparedStatement = connection.prepareStatement(
                                                         RDBMSConstants.PS_INSERT_RETAIN_MESSAGE_PART);

            for (AndesMessage message : retainList) {

                AndesMessageMetadata metadata = message.getMetadata();
                String destination = metadata.getDestination();
                RetainedItemData retainedItemData = getRetainedTopicID(connection, destination);

                if (null != retainedItemData) {

                    if (batchEmpty) {
                        batchEmpty = false;
                    }

                    addRetainedMessageToUpdateBatch(updateMetadataPreparedStatement,
                                                    deleteContentPreparedStatement,
                                                    insertContentPreparedStatement,
                                                    message, metadata, retainedItemData);
                    retainedItemData.messageID = metadata.getMessageID();

                } else {

                    createRetainedEntry(connection, message);

                }

            }

            if (!batchEmpty) {
                deleteContentPreparedStatement.executeBatch();
                updateMetadataPreparedStatement.executeBatch();
                insertContentPreparedStatement.executeBatch();
                connection.commit();
            }

        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE_PARTS);
            throw new AndesException("Error occurred while adding retained message content to DB ", e);

        } finally {
            close(updateMetadataPreparedStatement, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE_PARTS);
            close(deleteContentPreparedStatement, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE_PARTS);
            close(insertContentPreparedStatement, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE_PARTS);
            close(connection, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE_PARTS);
        }
    }


    /**
     * Used to store details about a retained item entry
     */
    private static class RetainedItemData {
        /**
         * Topic id of the DB entry
         */
        public int topicID;

        /**
         * Retained message ID for the topic
         */
        public long messageID;

        private RetainedItemData(Integer topicID, long messageID) {
            this.topicID = topicID;
            this.messageID = messageID;
        }
    }

    /**
     * Update batching prepared statements with current message
     *
     * @param updateMetadataPreparedStatement
     *         update prepared statement
     * @param deleteContentPreparedStatement
     *         delete prepared statement
     * @param insertContentPreparedStatement
     *         insert prepared statement
     * @param message
     *         current message
     * @param metadata
     *         current message metadata
     * @param retainedItemData
     *         retained item data
     * @throws SQLException
     */
    private void addRetainedMessageToUpdateBatch(PreparedStatement updateMetadataPreparedStatement,
                                                 PreparedStatement deleteContentPreparedStatement,
                                                 PreparedStatement insertContentPreparedStatement,
                                                 AndesMessage message,
                                                 AndesMessageMetadata metadata,
                                                 RetainedItemData retainedItemData)
            throws SQLException {
        // update metadata
        updateMetadataPreparedStatement.setLong(1, metadata.getMessageID());
        updateMetadataPreparedStatement.setBytes(2, metadata.getMetadata());
        updateMetadataPreparedStatement.setInt(3, retainedItemData.topicID);
        updateMetadataPreparedStatement.addBatch();

        // update content
        deleteContentPreparedStatement.setLong(1, retainedItemData.messageID);
        deleteContentPreparedStatement.addBatch();
        for (AndesMessagePart messagePart : message.getContentChunkList()) {
            insertContentPreparedStatement.setLong(1, metadata.getMessageID());
            insertContentPreparedStatement.setInt(2, messagePart.getOffSet());
            insertContentPreparedStatement.setBytes(3, messagePart.getData());
            insertContentPreparedStatement.addBatch();
        }
    }

    /**
     * Get retained topic ID for destination
     *
     * @param connection
     *         database connection to used
     * @param destination
     *         destination name
     * @return Retained item data
     * @throws SQLException
     */
    private RetainedItemData getRetainedTopicID(Connection connection, String destination)
            throws SQLException {
        PreparedStatement preparedStatementForMetadataSelect = null;
        RetainedItemData itemData = null;

        try {
            preparedStatementForMetadataSelect = connection
                    .prepareStatement(RDBMSConstants.PS_SELECT_RETAINED_MESSAGE_ID);
            preparedStatementForMetadataSelect.setString(1, destination);
            ResultSet results = preparedStatementForMetadataSelect.executeQuery();

            if (results.next()) {
                int topicID = results.getInt(RDBMSConstants.TOPIC_ID);
                long messageID = results.getLong(RDBMSConstants.MESSAGE_ID);
                itemData = new RetainedItemData(topicID, messageID);
            }
        } finally {
            close(preparedStatementForMetadataSelect, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE_PARTS);
        }

        return itemData;
    }

    /**
     * Create a new entry for retained message
     *
     * @param connection
     *         database connection
     * @param message
     *         retained message
     * @return Retained item data
     * @throws SQLException
     */
    private RetainedItemData createRetainedEntry(Connection connection, AndesMessage message) throws SQLException {
        PreparedStatement preparedStatementForContent = null;
        PreparedStatement preparedStatementForMetadata = null;

        AndesMessageMetadata metadata = message.getMetadata();
        String destination = metadata.getDestination();
        Integer topicID = destination.hashCode();
        long messageID = metadata.getMessageID();

        try {
            // create metadata entry
            preparedStatementForMetadata = connection.prepareStatement(
                                                      RDBMSConstants.PS_INSERT_RETAINED_METADATA);
            preparedStatementForMetadata.setInt(1, topicID);
            preparedStatementForMetadata.setString(2, destination);
            preparedStatementForMetadata.setLong(3, messageID);
            preparedStatementForMetadata.setBytes(4, metadata.getMetadata());
            preparedStatementForMetadata.addBatch();

            // create content
            preparedStatementForContent = connection.prepareStatement(
                                                     RDBMSConstants.PS_INSERT_RETAIN_MESSAGE_PART);
            for (AndesMessagePart messagePart : message.getContentChunkList()) {
                preparedStatementForContent.setLong(1, messageID);
                preparedStatementForContent.setInt(2, messagePart.getOffSet());
                preparedStatementForContent.setBytes(3, messagePart.getData());
                preparedStatementForContent.addBatch();
            }

            preparedStatementForMetadata.executeBatch();
            preparedStatementForContent.executeBatch();
            connection.commit();

            return new RetainedItemData(topicID, messageID);
        } finally {
            close(preparedStatementForContent, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE_PARTS);
            close(preparedStatementForMetadata, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE_PARTS);
        }
    }



    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllRetainedTopics() throws AndesException{
        Connection connection = null;
        PreparedStatement preparedStatementForTopicSelect = null;
        List<String> topicList = new ArrayList<String>();

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            preparedStatementForTopicSelect = connection.prepareStatement(RDBMSConstants.PS_SELECT_ALL_RETAINED_TOPICS);
            ResultSet results = preparedStatementForTopicSelect.executeQuery();

            while (results.next()){
                topicList.add(results.getString(RDBMSConstants.TOPIC_NAME));
            }

        }  catch (SQLException e) {
            rollback(connection, "reading all retained topics");
            throw new AndesException("Error occurred while reading retained topics ", e);
        } finally {
            close(preparedStatementForTopicSelect, "reading all retained topics");
        }

        return topicList;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessageMetadata getRetainedMetaData(String destination) throws AndesException {
        AndesMessageMetadata metadata = null;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;


        try {

            connection = getConnection();

            RetainedItemData retainedItemData = getRetainedTopicID(connection, destination);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_RETAINED_METADATA);
            preparedStatement.setLong(1, retainedItemData.topicID);

            results = preparedStatement.executeQuery();

            if (results.next()) {
                byte[] b = results.getBytes(RDBMSConstants.METADATA);
                long messageId = results.getLong(RDBMSConstants.MESSAGE_ID);
                metadata = new AndesMessageMetadata(messageId, b, true);
            }
        } catch (SQLException e) {
            throw new AndesException("error occurred while retrieving retained message " +
                                     "for destination:" + destination, e);
        } finally {
            String task = "Retrieve retained message for destination";
            close(results, task);
            close(preparedStatement, task);
            close(connection, task);
        }
        return metadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Integer, AndesMessagePart> getRetainedContentParts(long messageID) throws AndesException {

        Connection connection = null;

        PreparedStatement preparedStatement = null;

        ResultSet results = null;
        Map<Integer, AndesMessagePart> contentParts = new HashMap<Integer, AndesMessagePart>();

        try {
            connection = getConnection();

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_RETRIEVE_RETAIN_MESSAGE_PART);
            preparedStatement.setLong(1, messageID);
            results = preparedStatement.executeQuery();

            while (results.next()) {
                byte[] b = results.getBytes(RDBMSConstants.MESSAGE_CONTENT);
                int offset = results.getInt(RDBMSConstants.MSG_OFFSET);

                AndesMessagePart messagePart = new AndesMessagePart();

                messagePart.setMessageID(messageID);
                messagePart.setData(b);
                messagePart.setDataLength(b.length);
                messagePart.setOffSet(offset);
                contentParts.put(offset, messagePart);
            }
        } catch (SQLException e) {
            throw new AndesException("Error occurred while retrieving retained message content from DB" +
                                     " [msg_id=" + messageID + "]", e);
        } finally {
            close(results, RDBMSConstants.TASK_RETRIEVING_RETAINED_MESSAGE_PARTS);
            close(preparedStatement, RDBMSConstants.TASK_RETRIEVING_RETAINED_MESSAGE_PARTS);
            close(connection, RDBMSConstants.TASK_RETRIEVING_RETAINED_MESSAGE_PARTS);
        }
        return contentParts;
    }








}
