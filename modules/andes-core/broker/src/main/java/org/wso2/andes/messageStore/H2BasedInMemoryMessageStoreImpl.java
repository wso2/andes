/*
 *
 *   Copyright (c) 2005-2011, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.andes.server.store.util.CassandraDataAccessException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class H2BasedInMemoryMessageStoreImpl implements MessageStore {

    private static final Logger logger = Logger.getLogger(H2BasedInMemoryMessageStoreImpl.class);
    private final Map<String, Integer> queueMap; // cache queue name to queue_id mapping to avoid extra sql queries
    protected Connection connection;

    public H2BasedInMemoryMessageStoreImpl() {
        queueMap = new HashMap<String, Integer>();
    }

    @Override
    public void initializeMessageStore(DurableStoreConnection cassandraConnection) throws AndesException {
        // this will load the MySQL driver, each DB has its own driver
        // setup the connection with the DB.
        String icEntry = "";
        try {
            icEntry = "jdbc/InMemoryMessageStoreDB";
            DataSource dataSource = InitialContext.doLookup(icEntry);
            connection = dataSource.getConnection();
            createTables();
            logger.info("H2 In-Memory message store initialised");
        } catch (SQLException e) {
            logger.error("Connecting to H2 database failed!", e.getCause());
        } catch (NamingException e) {
            logger.error("Couldn't find \"" + icEntry + "\" entry in master_datasources.xml");
        }
    }

    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        try {
            String insert = "INSERT INTO " + JDBCConstants.MESSAGES_TABLE + "("+ JDBCConstants.MESSAGE_ID + "," +
                    JDBCConstants.MSG_OFFSET + "," + JDBCConstants.MESSAGE_CONTENT + ") VALUES (?, ?, ?)";

            PreparedStatement preparedStatement = connection.prepareStatement(insert);
            for(AndesMessagePart p: partList){
                preparedStatement.setLong(1, p.getMessageID());
                preparedStatement.setInt(2, p.getOffSet());
                preparedStatement.setBytes(3, p.getData());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
        } catch (SQLException e) {
            logger.error("Error occurred while adding message content to DB [" + e.getMessage() + "]");
        }
    }

    @Override
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {
        String delete = "DELETE * " +
                " FROM " + JDBCConstants.MESSAGES_TABLE +
                " AND " + JDBCConstants.METADATA_TABLE + "" +
                " AND " + JDBCConstants.REF_COUNT_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=?";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(delete);
            for(Long msgId: messageIdList){
                preparedStatement.setLong(1, msgId);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.close();

        } catch (SQLException e) {
            logger.error("Error occurred while deleting messages from DB [" + e.getMessage() + "]");
        }
    }

    @Override
    public AndesMessagePart getContent(String messageId, int offsetValue) throws AndesException {
        String select = "SELECT " + JDBCConstants.MESSAGE_CONTENT +
                " FROM " + JDBCConstants.MESSAGES_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=" + messageId +
                " AND " + JDBCConstants.MSG_OFFSET + "=" + offsetValue;
        AndesMessagePart messagePart = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(select);
            ResultSet results = preparedStatement.executeQuery();

            if(results.first()){
                byte[] b = results.getBytes(JDBCConstants.MESSAGE_CONTENT);
                messagePart = new AndesMessagePart();
                messagePart.setMessageID(Long.parseLong(messageId));
                messagePart.setData(b);
                messagePart.setDataLength(b.length);
                messagePart.setOffSet(offsetValue);
            }
        } catch (SQLException e) {
            logger.error("Error occurred while retrieving message content from DB [msg_id=" + messageId + "]" +
                    " {" + e.getMessage() + "}");
        }
        return messagePart;
    }

    @Override
    public void ackReceived(List<AndesAckData> ackList) throws AndesException {
        String delete = " DELETE * " +
                " FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE (" + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.QUEUE_ID + ") = " +
                " ( ?, ? )";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(delete);
            for(AndesAckData ackData: ackList){
                preparedStatement.setLong(1, ackData.messageID);
                preparedStatement.setInt(2, getQueueID(ackData.qName));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
        } catch (SQLException e) {
            logger.error("Error occurred while deleting acknowledged messages. [" + e.getMessage() + "]");
        }
    }

    @Override
    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException {
        String insert = "INSERT INTO " + JDBCConstants.METADATA_TABLE +
                "(" + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.QUEUE_ID + "," + JDBCConstants.METADATA + ")" +
                " VALUES ( ?,?, ? )";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(insert);
            for(AndesMessageMetadata metadata: metadataList){
                preparedStatement.setLong(1, metadata.getMessageID());
                preparedStatement.setInt(2, getQueueID(metadata.getDestination()));
                preparedStatement.setBytes(3, metadata.getMetadata());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
        } catch (SQLException e) {
            logger.error("Error occurred while inserting messages to queues [" + e.getMessage() + "]");
        }
    }

    @Override
    public long getMessageCountForQueue(final String destinationQueueName) throws AndesException {
        String select = "SELECT COUNT(" + JDBCConstants.QUEUE_ID +") AS count" +
                " FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.QUEUE_ID +
                " IN ( SELECT " + JDBCConstants.QUEUE_ID + " FROM " + JDBCConstants.QUEUES_TABLE +
                " WHERE " + JDBCConstants.QUEUE_NAME +"=" + destinationQueueName +")";

        long count = 0;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(select);
            ResultSet results = preparedStatement.executeQuery();
            while (results.next()){
                count = results.getLong("count");
            }
            preparedStatement.close();
        } catch (SQLException e) {
            logger.error("error occurred while retrieving message count for queue [" + e.getMessage() + "]");
        }
        return count;
    }

    @Override
    public AndesMessageMetadata getMetaData(long messageId) {
        String select = "SELECT " + JDBCConstants.METADATA +
                " FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=" + messageId +
                " LIMIT 1";
        AndesMessageMetadata md = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(select);
            ResultSet results = preparedStatement.executeQuery();
            while (results.next()){
                byte[] b = results.getBytes(JDBCConstants.METADATA);
                md = new AndesMessageMetadata(messageId, b, true);
            }
        } catch (SQLException e) {
            logger.error("error occurred while retrieving message metadata for msg id:" + messageId +
                    "[" + e.getMessage() + "]");
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
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(select);
            preparedStatement.setInt(1, getQueueID(queueName));
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
        } catch (SQLException e) {
            logger.error("Error occurred while retrieving messages between msg id "
                    + firstMsgId + " and " + lastMsgID + " from queue " + queueName + " [" + e.getMessage() + "]");
        }
        return metadataList;
    }

    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String queueName, long firstMsgId, int count) throws AndesException {
        String select = "SELECT " + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.METADATA +
                " FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + ">" + (firstMsgId - 1) +
                " AND " + JDBCConstants.QUEUE_ID + "=" + getQueueID(queueName) +
                " LIMIT " + count;

        List<AndesMessageMetadata> mdList = new ArrayList<AndesMessageMetadata>();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(select);
            ResultSet results = preparedStatement.executeQuery();
            while (results.next()){
                AndesMessageMetadata md = new AndesMessageMetadata(
                        results.getLong(JDBCConstants.MESSAGE_ID),
                        results.getBytes(JDBCConstants.METADATA),
                        true
                );
                mdList.add(md);
            }
        } catch (SQLException e) {
            logger.error("error occurred while retrieving message metadata from queue [" + e.getMessage() + "]");
        }
        return mdList;
    }

    @Override
    public void deleteMessageMetadataFromQueue(final String queueName, List<AndesRemovableMetadata> messagesToRemove) throws AndesException {

        try {
            String sqlString = "DELETE * " +
                    " FROM " + JDBCConstants.METADATA_TABLE +
                    " WHERE " + JDBCConstants.QUEUE_ID + "=" + getQueueID(queueName) +
                    " AND " + JDBCConstants.MESSAGE_ID + "=?";

            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            for(AndesRemovableMetadata md: messagesToRemove){
                preparedStatement.setLong(1, md.messageID);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
        } catch (SQLException e) {
            logger.error("error occurred while deleting message metadata from queue [" + e.getMessage() + "]");
        }
    }

    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(Long limit) {
//        String select =
        return null;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error("Connection close failed! [" + e.getMessage() + "]");
        }
    }

    @Override
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {
        try {
            String sqlString = "DELETE * " +
                    " FROM " + JDBCConstants.EXPIRATION_TABLE +
                    " WHERE " + JDBCConstants.MESSAGE_ID + "=?";


            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            for(Long mid: messagesToRemove){
                preparedStatement.setLong(1, mid);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
        } catch (SQLException e) {
            logger.error("error occurred while deleting message metadata from expiration table [" + e.getMessage() + "]");
        }
    }

    private int getQueueID(final String destinationQueueName){

        Integer id = queueMap.get(destinationQueueName);
        if(id != null){
            return id;
        }

        String sqlString = "INSERT INTO " + JDBCConstants.QUEUES_TABLE + "( "+ JDBCConstants.QUEUE_NAME +")" +
                " VALUES (\"" + destinationQueueName + "\")";
        PreparedStatement preparedStatement;
        try {
            preparedStatement = connection.prepareStatement(sqlString, Statement.RETURN_GENERATED_KEYS);
            preparedStatement.executeUpdate();
            ResultSet results = preparedStatement.getGeneratedKeys();
            if(results.first()){
                id = results.getInt(1);
            }
            queueMap.put(destinationQueueName, id);
        } catch (SQLException e) {
            logger.error("Error occurred while inserting destination queue details to database [" + e.getMessage() + "]");
        }
        return id;
    }

    protected void createTables() throws SQLException{
        try {
            String[] queries = {
                    "CREATE TABLE " + JDBCConstants.MESSAGES_TABLE + " (" +
                            JDBCConstants.MESSAGE_ID + " BIGINT, " +
                            JDBCConstants.MSG_OFFSET + " INT, "  +
                            JDBCConstants.MESSAGE_CONTENT + " BINARY NOT NULL, " +
                            "PRIMARY KEY (" + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.MSG_OFFSET + ")" +
                            ");"
//                    ,
//
//                    "CREATE TABLE " + JDBCConstants.QUEUES_TABLE + " (" +
//                            JDBCConstants.QUEUE_ID + " INT AUTO_INCREMENT, " +
//                            JDBCConstants.QUEUE_NAME + " VARCHAR NOT NULL, " +
//                            "UNIQUE (" + JDBCConstants.QUEUE_NAME + ")," +
//                            "PRIMARY KEY (" + JDBCConstants.QUEUE_ID + ")" +
//                            ");",
//
//                    "CREATE TABLE " + JDBCConstants.REF_COUNT_TABLE + " (" +
//                            JDBCConstants.MESSAGE_ID + " BIGINT, " +
//                            JDBCConstants.REF_COUNT + " INT, " +
//                            "PRIMARY KEY (" + JDBCConstants.MESSAGE_ID +")" +
//                            ");",
//
//                    "CREATE TABLE " + JDBCConstants.METADATA_TABLE + "(" +
//                            JDBCConstants.MESSAGE_ID + " BIGINT, " +
//                            JDBCConstants.QUEUE_ID + " INT, " +
//                            JDBCConstants.METADATA + " BINARY, " +
//                            "PRIMARY KEY (" + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.QUEUE_ID + "), " +
//                            "FOREIGN KEY (" + JDBCConstants.QUEUE_ID + ") " +
//                            "REFERENCES " + JDBCConstants.QUEUES_TABLE +" (" + JDBCConstants.QUEUE_ID + ") " +
//                            ");",
//
//                    "CREATE TABLE " + JDBCConstants.EXPIRATION_TABLE + "(" +
//                            JDBCConstants.MESSAGE_ID + " BIGINT UNIQUE," +
//                            JDBCConstants.EXPIRATION_TIME + " BIGINT, " +
//                            "FOREIGN KEY ("+ JDBCConstants.MESSAGE_ID + ") " +
//                            "REFERENCES " + JDBCConstants.METADATA_TABLE + " (" + JDBCConstants.MESSAGE_ID + ")" +
//                            "); "
            };
            Statement stmt = connection.createStatement();
            for(String q: queries) {
                stmt.addBatch(q);
            }
            stmt.executeBatch();
            stmt.close();
        } catch (SQLException e) {
            logger.error("Creating database schema failed!", e.getCause());
            throw e;
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
//                    " AND " + JDBCConstants.QUEUE_ID + "=" + String.valueOf(getQueueID(destinationQueue));
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
