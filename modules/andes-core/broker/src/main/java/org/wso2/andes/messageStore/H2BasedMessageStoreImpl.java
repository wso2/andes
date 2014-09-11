/*
 *
 *   Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.andes.messageStore;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.store.util.CassandraDataAccessException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * H2 based message store implementation. This can be initialised in either
 * in memory mode, embedded mode or server mode.
 */
public class H2BasedMessageStoreImpl implements MessageStore {

    private static final Logger logger = Logger.getLogger(H2BasedMessageStoreImpl.class);
    private final Map<String, Integer> queueMap; // cache queue name to queue_id mapping to avoid extra sql queries
    private DataSource datasource;
    private String icEntry;
    private ConcurrentSkipListMap<Long, Long> contentDeletionTasks = new ConcurrentSkipListMap<Long, Long>();
    private MessageContentRemoverTask messageContentRemoverTask;

    /**
     * Initialise to work in embedded or server mode
     */
    public H2BasedMessageStoreImpl() {
        icEntry = "jdbc/H2MessageStoreDB";
        queueMap = new ConcurrentHashMap<String, Integer>();

    }

    /**
     *
     * @param isInMemory if true starts in in-memory mode
     */
    public H2BasedMessageStoreImpl(boolean isInMemory) {
        this();
        if (isInMemory) {
            icEntry = "jdbc/InMemoryMessageStoreDB";
        }
    }

    @Override
    public void initializeMessageStore(DurableStoreConnection durableStoreConnection) throws AndesException {
        Connection connection = null;
        H2Connection h2Connection = new H2Connection();

        try {

            datasource = InitialContext.doLookup(icEntry);
            connection = datasource.getConnection();
            h2Connection.setIsConnected(true);

            // start periodic message removal task
            messageContentRemoverTask = new MessageContentRemoverTask(
                    ClusterResourceHolder.getInstance().getClusterConfiguration().getContentRemovalTaskInterval(),
                    contentDeletionTasks, this, h2Connection );
            
            messageContentRemoverTask.start();

            logger.info("H2 message store initialised");

        } catch (SQLException e) {
            throw new AndesException("Connecting to H2 database failed!", e);
        } catch (NamingException e) {
            logger.error("Couldn't find \"" + icEntry + "\" entry in master_datasources.xml");
        } finally {
            close(connection);
        }
    }

    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        Connection connection = null;
        String insert = "INSERT INTO " + JDBCConstants.MESSAGES_TABLE + "(" + JDBCConstants.MESSAGE_ID + "," +
                JDBCConstants.MSG_OFFSET + "," + JDBCConstants.MESSAGE_CONTENT + ") VALUES (?, ?, ?)";
        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement(insert);
            for (AndesMessagePart messagePart : partList) {
                preparedStatement.setLong(1, messagePart.getMessageID());
                preparedStatement.setInt(2, messagePart.getOffSet());
                preparedStatement.setBytes(3, messagePart.getData());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection);
            throw new AndesException("Error occurred while adding message content to DB ", e);
        } finally {
            close(connection);
        }
    }

    @Override
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {
        //TODO: cascade delete?
        String delete = "DELETE " +
                " FROM " + JDBCConstants.MESSAGES_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=?";

        Connection connection = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(delete);
            for (Long msgId : messageIdList) {
                preparedStatement.setLong(1, msgId);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection);
            throw new AndesException("Error occurred while deleting messages from DB ", e);
        } finally {
            close(connection);
        }
    }

    @Override
    public AndesMessagePart getContent(String messageId, int offsetValue) throws AndesException {
        String select = "SELECT " + JDBCConstants.MESSAGE_CONTENT +
                " FROM " + JDBCConstants.MESSAGES_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=" + messageId +
                " AND " + JDBCConstants.MSG_OFFSET + "=" + offsetValue;

        AndesMessagePart messagePart = null;
        Connection connection = null;
        try {
            connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(select);
            ResultSet results = preparedStatement.executeQuery();

            if (results.first()) {
                byte[] b = results.getBytes(JDBCConstants.MESSAGE_CONTENT);
                messagePart = new AndesMessagePart();
                messagePart.setMessageID(Long.parseLong(messageId));
                messagePart.setData(b);
                messagePart.setDataLength(b.length);
                messagePart.setOffSet(offsetValue);
            }
            preparedStatement.close();
        } catch (SQLException e) {
            throw new AndesException("Error occurred while retrieving message content from DB" +
                    " [msg_id=" + messageId + "]", e);
        } finally {
            close(connection);
        }
        return messagePart;
    }

    @Override
    public void ackReceived(List<AndesAckData> ackList) throws AndesException {
        List<AndesRemovableMetadata> messagesAddressedToQueues = new ArrayList<AndesRemovableMetadata>();
        List<AndesRemovableMetadata> messagesAddressedToTopics = new ArrayList<AndesRemovableMetadata>();

        List<Long> messageIds = new ArrayList<Long>();

        for (AndesAckData ackData : ackList) {
            if (!ackData.isTopic) {

                messagesAddressedToTopics.add(ackData.convertToRemovableMetaData());

                //schedule to remove queue and topic message content
                long timeGapConfigured = ClusterResourceHolder.getInstance().
                        getClusterConfiguration().getPubSubMessageRemovalTaskInterval() * 1000000;
                contentDeletionTasks.put(System.nanoTime() + timeGapConfigured, ackData.messageID);

            } else {

                messagesAddressedToQueues.add(ackData.convertToRemovableMetaData());
                OnflightMessageTracker onflightMessageTracker = OnflightMessageTracker.getInstance();
                onflightMessageTracker.updateDeliveredButNotAckedMessages(ackData.messageID);

                //schedule to remove queue and topic message content
                contentDeletionTasks.put(System.nanoTime(), ackData.messageID);
            }

            PerformanceCounter.recordMessageRemovedAfterAck();
            messageIds.add(ackData.messageID);
        }

        //remove queue message metadata now
        deleteMessages(messagesAddressedToQueues, false);

        //remove topic message metadata now
        deleteMessages(messagesAddressedToTopics, false);

        deleteMessagesFromExpiryQueue(messageIds);      // hasithad This cant be done here cos topic delivery logic comes here before a delivery :(
    }

    @Override
    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException {

        Connection connection = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(getInsertIntoMetadataSqlString());
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
            rollback(connection);
            throw new AndesException("Error occurred while inserting metadata list to queues ", e);
        } finally {
            close(connection);
        }
    }

    @Override
    public void addMetaData(AndesMessageMetadata metadata) throws AndesException {

        Connection connection = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(getInsertIntoMetadataSqlString());

            addMetadataToBatch(preparedStatement, metadata, metadata.getDestination());
            preparedStatement.executeBatch();
            preparedStatement.close();
            addToExpiryTable(connection, metadata);
            incrementRefCount(connection, metadata.getMessageID());
            connection.commit();

            if (logger.isDebugEnabled()) {
                logger.debug("Metadata added: msgID: " + metadata.getMessageID() + " Destination: " + metadata.getDestination());
            }
        } catch (SQLException e) {
            rollback(connection);
            throw new AndesException("Error occurred while inserting message metadata to queue ", e);
        } finally {
            close(connection);
        }
    }

    @Override
    public void addMetaDataToQueue(String queueName, AndesMessageMetadata metadata) throws AndesException {

        Connection connection = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(getInsertIntoMetadataSqlString());

            // add to metadata table
            addMetadataToBatch(preparedStatement, metadata, queueName);
            preparedStatement.executeBatch();
            preparedStatement.close();
            addToExpiryTable(connection, metadata);

            connection.commit();
        } catch (SQLException e) {
            rollback(connection);
            throw new AndesException("Error occurred while inserting message metadata to queue " + queueName, e);
        } finally {
            close(connection);
        }
    }

    @Override
    public void addMetadataToQueue(String queueName, List<AndesMessageMetadata> metadataList) throws AndesException {

        Connection connection = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(getInsertIntoMetadataSqlString());

            for (AndesMessageMetadata md : metadataList) {
                addMetadataToBatch(preparedStatement, md, queueName);
            }

            preparedStatement.executeBatch();
            preparedStatement.close();
            addListToExpiryTable(connection, metadataList);

            connection.commit();
        } catch (SQLException e) {
            rollback(connection);
            throw new AndesException("Error occurred while inserting message metadata list to queue " + queueName, e);
        } finally {
            close(connection);
        }
    }

    private String getInsertIntoMetadataSqlString() {
        return "INSERT INTO " + JDBCConstants.METADATA_TABLE +
                " (" + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.QUEUE_ID + "," + JDBCConstants.METADATA + ")" +
                " VALUES ( ?,?,? )";
    }

    /**
     * Adds a single metadata to a batch insert of metadata
     *
     * @param preparedStatement prepared statement to add messages to metadata table
     * @param metadata          AndesMessageMetadata
     * @param queueName         queue to be assigned
     * @throws SQLException
     */
    private void addMetadataToBatch(PreparedStatement preparedStatement, AndesMessageMetadata metadata,
                                    final String queueName) throws SQLException {
        preparedStatement.setLong(1, metadata.getMessageID());
        preparedStatement.setInt(2, getCachedQueueID(queueName));
        preparedStatement.setBytes(3, metadata.getMetadata());
        preparedStatement.addBatch();
    }

    private String getInsertIntoExpiryTableSqlString() {
        return " INSERT INTO " + JDBCConstants.EXPIRATION_TABLE +
                " (" + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.EXPIRATION_TIME + "," +
                JDBCConstants.DESTINATION_QUEUE + ")" +
                " VALUES ( ?,?,? );";
    }

    private void addToExpiryTable(Connection connection, AndesMessageMetadata metadata) throws SQLException {

        if (metadata.getExpirationTime() > 0) {
            PreparedStatement preparedStatement = connection.prepareStatement(getInsertIntoExpiryTableSqlString());
            addExpiryTableEntryToBatch(preparedStatement, metadata);
            preparedStatement.executeBatch();
            preparedStatement.close();
        }
    }

    private void addListToExpiryTable(Connection connection, List<AndesMessageMetadata> list) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(getInsertIntoExpiryTableSqlString());

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

    @Override
    public long getMessageCountForQueue(final String destinationQueueName) throws AndesException {
        String select = "SELECT COUNT(" + JDBCConstants.QUEUE_ID + ") AS count" +
                " FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.QUEUE_ID + "=?";

        long count = 0;
        Connection connection = null;
        try {
            connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(select);
            preparedStatement.setInt(1, getCachedQueueID(destinationQueueName));

            ResultSet results = preparedStatement.executeQuery();
            if (results.first()) {
                count = results.getLong("count");
            }
            preparedStatement.close();
        } catch (SQLException e) {
            throw new AndesException("error occurred while retrieving message count for queue ", e);
        } finally {
            close(connection);
        }
        return count;
    }

    @Override
    public AndesMessageMetadata getMetaData(long messageId) throws AndesException {
        String select = "SELECT " + JDBCConstants.METADATA +
                " FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=?" +
                " LIMIT 1";
        AndesMessageMetadata md = null;
        Connection connection = null;
        try {
            connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(select);
            preparedStatement.setLong(1, messageId);
            ResultSet results = preparedStatement.executeQuery();
            if (results.first()) {
                byte[] b = results.getBytes(JDBCConstants.METADATA);
                md = new AndesMessageMetadata(messageId, b, true);
            }
        } catch (SQLException e) {
            throw new AndesException("error occurred while retrieving message " +
                    "metadata for msg id:" + messageId, e);
        } finally {
            close(connection);
        }
        return md;
    }

    @Override
    public List<AndesMessageMetadata> getMetaDataList(final String queueName, long firstMsgId, long lastMsgID) throws AndesException {

        String select = "SELECT " + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.METADATA +
                " FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.QUEUE_ID + "=?" +
                " AND " + JDBCConstants.MESSAGE_ID +
                " BETWEEN ?" +
                " AND ?";

        List<AndesMessageMetadata> metadataList = new ArrayList<AndesMessageMetadata>();
        Connection connection = null;
        try {
            connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(select);
            preparedStatement.setInt(1, getCachedQueueID(queueName));
            preparedStatement.setLong(2, firstMsgId);
            preparedStatement.setLong(3, lastMsgID);

            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                AndesMessageMetadata md = new AndesMessageMetadata(
                        resultSet.getLong(JDBCConstants.MESSAGE_ID),
                        resultSet.getBytes(JDBCConstants.METADATA),
                        true
                );
                metadataList.add(md);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("request: metadata range (" + firstMsgId + " , " + lastMsgID + ") in destination queue " + queueName
                        + "\nresponse: metadata count " + metadataList.size());
            }
        } catch (SQLException e) {
            throw new AndesException("Error occurred while retrieving messages between msg id "
                    + firstMsgId + " and " + lastMsgID + " from queue " + queueName, e);
        } finally {
            close(connection);
        }
        return metadataList;
    }

    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String queueName, long firstMsgId, int count) throws AndesException {
        String select = "SELECT " + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.METADATA +
                " FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + ">?" +
                " AND " + JDBCConstants.QUEUE_ID + "=?" +
                " LIMIT ?";

        List<AndesMessageMetadata> mdList = new ArrayList<AndesMessageMetadata>();
        Connection connection = null;
        try {
            connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(select);
            preparedStatement.setLong(1, firstMsgId - 1);
            preparedStatement.setInt(2, getCachedQueueID(queueName));
            preparedStatement.setInt(3, count);

            ResultSet results = preparedStatement.executeQuery();
            while (results.next()) {
                AndesMessageMetadata md = new AndesMessageMetadata(
                        results.getLong(JDBCConstants.MESSAGE_ID),
                        results.getBytes(JDBCConstants.METADATA),
                        true
                );
                mdList.add(md);
            }
        } catch (SQLException e) {
            throw new AndesException("error occurred while retrieving message metadata from queue ", e);
        } finally {
            close(connection);
        }
        return mdList;
    }

    @Override
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDeadLetterChannel) throws AndesException {

        // todo: update when DLC implementation is ported to 3.0.0
        String sqlString = "DELETE " +
                " FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.QUEUE_ID + "=?" +
                " AND " + JDBCConstants.MESSAGE_ID + "=?";

        Connection connection = null;
        try {

            connection = getConnection();
            connection.setAutoCommit(false);

            // delete from expiry queue first.
            deleteFromExpiryQueue(connection, messagesToRemove);

            // remove from metadata table
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            for (AndesRemovableMetadata md : messagesToRemove) {
                preparedStatement.setInt(1, getCachedQueueID(md.destination));
                preparedStatement.setLong(2, md.messageID);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.close();

            // commit
            connection.commit();

            // finally add to remove content task
            for (AndesRemovableMetadata md : messagesToRemove) {
                contentDeletionTasks.put(System.nanoTime(), md.messageID);
            }
            if (logger.isDebugEnabled()) {
                logger.debug(messagesToRemove.size() + " messages scheduled to be removed.");
            }
        } catch (SQLException e) {
            rollback(connection);
            throw new AndesException("error occurred while deleting messages ", e);
        } finally {
            close(connection);
        }
    }

    @Override
    public void deleteMessageMetadataFromQueue(final String queueName, List<AndesRemovableMetadata> messagesToRemove) throws AndesException {

        String sqlString = "DELETE " +
                " FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.QUEUE_ID + "=?" +
                " AND " + JDBCConstants.MESSAGE_ID + "=?";
        Connection connection = null;
        try {
            int queueID = getCachedQueueID(queueName);

            connection = getConnection();
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            for (AndesRemovableMetadata md : messagesToRemove) {
                preparedStatement.setInt(1, queueID);
                preparedStatement.setLong(2, md.messageID);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
            preparedStatement.close();

            if (logger.isDebugEnabled()) {
                logger.debug("Metadata removed. " + messagesToRemove.size() + " metadata from destination "
                        + queueName);
            }
        } catch (SQLException e) {
            rollback(connection);
            throw new AndesException("error occurred while deleting message metadata from queue ", e);
        } finally {
            close(connection);
        }
    }

    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {

        // todo: can't we just delete expired messages?
        Connection connection = null;
        List<AndesRemovableMetadata> list;

        String select = "SELECT " + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.DESTINATION_QUEUE +
                " FROM " + JDBCConstants.EXPIRATION_TABLE +
                " WHERE " + JDBCConstants.EXPIRATION_TIME + "<" + System.currentTimeMillis() +
                " LIMIT " + Integer.toString(limit);

        try {
            connection = getConnection();
            list = new ArrayList<AndesRemovableMetadata>();

            // get expired message list
            Statement stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(select);
            while (resultSet.next()) {
                list.add(new AndesRemovableMetadata(
                                resultSet.getLong(JDBCConstants.MESSAGE_ID),
                                resultSet.getString(JDBCConstants.DESTINATION_QUEUE)
                        )
                );

            }
            stmt.close();
            return list;
        } catch (SQLException e) {
            throw new AndesException("error occurred while retrieving expired messages.", e);
        } finally {
            close(connection);
        }
    }

    @Override
    public void close() {
        messageContentRemoverTask.setRunning(false);
    }

    /**
     * This method is expected to be used in a transaction based update.
     *
     * @param connection       connection to be used
     * @param messagesToRemove AndesRemovableMetadata
     * @throws SQLException
     */
    private void deleteFromExpiryQueue(Connection connection, List<AndesRemovableMetadata> messagesToRemove) throws SQLException {

        String sqlString = "DELETE " +
                " FROM " + JDBCConstants.EXPIRATION_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=?";

        PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
        for (AndesRemovableMetadata md : messagesToRemove) {
            preparedStatement.setLong(1, md.messageID);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
    }


    @Override
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {
        Connection connection = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            String sqlString = "DELETE " +
                    " FROM " + JDBCConstants.EXPIRATION_TABLE +
                    " WHERE " + JDBCConstants.MESSAGE_ID + "=?";

            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            for (Long mid : messagesToRemove) {
                preparedStatement.setLong(1, mid);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
            preparedStatement.close();
        } catch (SQLException e) {
            rollback(connection);
            throw new AndesException("error occurred while deleting message metadata " +
                    "from expiration table ", e);
        } finally {
            close(connection);
        }
    }

    /**
     * This method caches the queue ids for destination queue names. If queried destination queue
     * is not in cache updates the cache and returns the queue id.
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

        // not in map query from DB (some other node might have created it)
        int queueID = getQueueID(destinationQueueName);

        if (queueID != -1) {
            return queueID;
        }

        // if queue is not available create a queue in DB
        return createNewQueue(destinationQueueName);
    }

    // get queue id through a DB query
    private int getQueueID(final String destinationQueueName) throws SQLException {
        String sqlString = "SELECT " + JDBCConstants.QUEUE_ID +
                " FROM " + JDBCConstants.QUEUES_TABLE +
                " WHERE " + JDBCConstants.QUEUE_NAME + "=?";

        int queueID = -1;
        Connection connection = null;
        try {
            connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString, Statement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, destinationQueueName);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.first()) {
                queueID = resultSet.getInt(JDBCConstants.QUEUE_ID);
            }
        } catch (SQLException e) {
            logger.error("Error occurred while retrieving destination queue id " +
                    "for destination queue " + destinationQueueName);
            throw e;
        } finally {
            close(connection);
        }
        return queueID;
    }

    // creates a new queue entry in DB
    private int createNewQueue(final String destinationQueueName) throws SQLException {
        String sqlString = "INSERT INTO " + JDBCConstants.QUEUES_TABLE + " (" + JDBCConstants.QUEUE_NAME + ")" +
                " VALUES (?)";
        Connection connection = null;
        int queueID = -1;

        try {
            connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString, Statement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, destinationQueueName);
            preparedStatement.executeUpdate();

            ResultSet results = preparedStatement.getGeneratedKeys();
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
            logger.error("Error occurred while inserting destination queue [" + destinationQueueName + "] to database ");
            throw e;
        } finally {
            close(connection);
        }
        return queueID;
    }

    private Connection getConnection() throws SQLException {
        return datasource.getConnection();
    }


    private void incrementRefCount(Connection connection, long messageId) throws SQLException {

        // todo: ON DUPLICATE KEY UPDATE functionality requires newer version of H2 1.3.175
        String sql = "INSERT INTO " + JDBCConstants.REF_COUNT_TABLE +
                " (" + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.REF_COUNT + ") " +
                "VALUES (" + messageId + ", 1) ";
//                "ON DUPLICATE KEY UPDATE " + JDBCConstants.REF_COUNT + "=" + JDBCConstants.REF_COUNT + " + 1";

        Statement stmt = connection.createStatement();
        stmt.executeUpdate(sql);
    }

    /**
     * Closes the provided connection. on failure log the error;
     *
     * @param connection Connection
     */
    private void close(Connection connection) {
        if (connection != null) try {
            connection.close();
        } catch (SQLException e) {
            logger.error("failed to close connection");
        }
    }

    private void rollback(Connection connection) {
        if (connection != null) try {
            connection.rollback();
        } catch (SQLException e1) {
            logger.fatal("rollback failed!");
        }
    }


    /////////////////////////////////////////////////////////////
    // DEPRECATED METHODS
    ////////////////////////////////////////////////////////////

    @Override
    public void deleteMessageMetadataFromQueue(QueueAddress queueAddress, List<AndesRemovableMetadata> messagesToRemove) throws AndesException {
    }

    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(QueueAddress queueAddress, long startMsgID, int count) throws AndesException {
        return null;
    }

    @Override
    public void addMessageMetaData(QueueAddress queueAddress, List<AndesMessageMetadata> messageList) throws AndesException {
//
//        for(AndesMessageMetadata md: messageList){
//
//        }
    }

    @Override
    public void moveMessageMetaData(QueueAddress sourceAddress, QueueAddress targetAddress, List<AndesMessageMetadata> messageList) throws AndesException {

    }

    @Override
    public long moveAllMessageMetaDataOfQueue(QueueAddress sourceAddress, QueueAddress targetAddress, String destinationQueue) throws AndesException {
//        long movedCount = 0;
//        try {
//            String sqlString = "UPDATE " + JDBCConstants.METADATA_TABLE +
//                    " SET " + JDBCConstants.TNQ_NQ_GQ_ID + "=" + String.valueOf(getMainQueueID(targetAddress.queueName)) +
//                    " WHERE " + JDBCConstants.TNQ_NQ_GQ_ID + "=" + String.valueOf(getMainQueueID(sourceAddress.queueName)) +
//                    " AND " + JDBCConstants.QUEUE_ID + "=" + String.valueOf(getCachedQueueID(destinationQueue));
//
//            Statement stmt = connection.createStatement();
//            movedCount = stmt.executeUpdate(sqlString);
//            stmt.close();
//        } catch (SQLException e) {
//            logger.error("error occurred while trying to move metadata from " + sourceAddress.queueName + " to "
//                            + targetAddress.queueName);
//        }
        return 0;
    }

    @Override
    public int countMessagesOfQueue(QueueAddress queueAddress, String destinationQueueNameToMatch) throws AndesException {
//        String select = "SELECT COUNT(" + JDBCConstants.TNQ_NQ_GQ_ID + ") AS count" +
//                " WHERE (" + JDBCConstants.TNQ_NQ_GQ_ID + "," + JDBCConstants.QUEUE_ID + ") = " +
//                "(";
//
//        int count = 0;
//        try {
//            Statement stmt = connection.createStatement();
//            ResultSet results =stmt.executeQuery(select);
//            while (results.next()){
//                count = results.getInt("count");
//            }
//        } catch (SQLException e) {
//            logger.error("error occurred while retrieving message count of queue");
//        }
        return 0;
    }

    @Override
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime, boolean isMessageForTopic, String destination) throws CassandraDataAccessException {

    }

    @Override
    public void deleteMessageMetadata(List<AndesRemovableMetadata> messagesToRemove,
                               boolean moveToDeadLetterChannel) throws AndesException {

    }

    @Override
    public void moveToDeadLetterChannel(List<AndesRemovableMetadata> messageList) {

    }

    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(Long limit, String columnFamilyName, String keyspace) {
        return null;
    }
}
