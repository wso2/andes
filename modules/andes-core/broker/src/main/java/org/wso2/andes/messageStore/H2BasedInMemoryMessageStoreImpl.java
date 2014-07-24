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
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class H2BasedInMemoryMessageStoreImpl implements MessageStore {

    private static final Logger logger = Logger.getLogger(H2BasedInMemoryMessageStoreImpl.class);
    private Map<String, Integer> destinationQueueMap;
    private Map<String, Integer> mainQueueMap;
    private Connection connection;

    public H2BasedInMemoryMessageStoreImpl() {
        destinationQueueMap = new HashMap<String, Integer>();
        mainQueueMap = new HashMap<String, Integer>();
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

            PreparedStatement pstmt = connection.prepareStatement(insert);
            for(AndesMessagePart p: partList){
                pstmt.setLong(1, p.getMessageID());
                pstmt.setInt(2, p.getOffSet());
                pstmt.setBytes(3, p.getData());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            pstmt.close();
        } catch (SQLException e) {
            logger.error("Error occurred while adding message content to DB");
        }
    }

    @Override
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {
        String delete = "DELETE * FROM " + JDBCConstants.MESSAGES_TABLE + " AND " + JDBCConstants.METADATA_TABLE + " AND "
                + JDBCConstants.REF_COUNT_TABLE + " WHERE " + JDBCConstants.MESSAGE_ID + "=?";

        try {
            PreparedStatement pstmt = connection.prepareStatement(delete);
            for(Long msgId: messageIdList){
                pstmt.setLong(1, msgId);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            pstmt.close();

        } catch (SQLException e) {
            logger.error("Error occurred while deleting messages from DB");
        }

    }

    @Override
    public int getContent(String messageId, int offsetValue, ByteBuffer dst) {
        String select = "SELECT " + JDBCConstants.MESSAGE_CONTENT +
                " FROM " + JDBCConstants.MESSAGES_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=" + messageId +
                " AND " + JDBCConstants.MSG_OFFSET + "=" + String.valueOf(offsetValue);
        int offset = 0;
        try {
            Statement stmt = connection.createStatement();
            ResultSet results = stmt.executeQuery(select);

            while (results.next()){
                byte[] b = results.getBytes(JDBCConstants.MESSAGE_CONTENT);
                dst = ByteBuffer.wrap(b);
                offset = b.length;
            }
        } catch (SQLException e) {
            logger.error("Error occurred while retrieving message content from DB [msg_id=" + messageId + "]");
        }
        return offset;
    }

    @Override
    public void deleteMessageMetadataFromQueue(QueueAddress queueAddress, List<AndesRemovableMetadata> messagesToRemove) throws AndesException {

        try {
                Integer id = getMainQueueID(queueAddress.queueName);
                String sqlString = "DELETE * FROM " + JDBCConstants.METADATA_TABLE +
                        " WHERE " + JDBCConstants.QUEUE_ID + "=" + String.valueOf(id) +
                        " AND " + JDBCConstants.MESSAGE_ID + "=?";

                PreparedStatement pstmt = connection.prepareStatement(sqlString);
                for(AndesRemovableMetadata md: messagesToRemove){
                    pstmt.setLong(1, md.messageID);
                    pstmt.addBatch();
                    // todo: reference count update code
                }
                pstmt.executeBatch();
                pstmt.close();
        } catch (SQLException e) {
            logger.error("error occurred while deleting message metadata from queue");
        }
    }

    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(QueueAddress queueAddress, long startMsgID, int count) throws AndesException {

        String select = "SELECT * FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + ">" + (startMsgID - 1) +
                " AND " + JDBCConstants.QUEUE_ID + "=" + String.valueOf(getMainQueueID(queueAddress.queueName)) +
                " LIMIT " + count;

        List<AndesMessageMetadata> mdList = new ArrayList<AndesMessageMetadata>();
        try {
            Statement stmt = connection.createStatement();
            ResultSet results = stmt.executeQuery(select);
            while (results.next()){
                AndesMessageMetadata md = new AndesMessageMetadata(
                        results.getLong(JDBCConstants.MESSAGE_ID),
                        results.getBytes(JDBCConstants.METADATA),
                        true
                );
                mdList.add(md);
            }
        } catch (SQLException e) {
            logger.error("error occurred while retrieving message metadata from queue");
        }
        return mdList;
    }

    @Override
    public void ackReceived(List<AndesAckData> ackList) throws AndesException {

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
        long movedCount = 0;
        try {
            String sqlString = "UPDATE " + JDBCConstants.METADATA_TABLE +
                    " SET " + JDBCConstants.TNQ_NQ_GQ_ID + "=" + String.valueOf(getMainQueueID(targetAddress.queueName)) +
                    " WHERE " + JDBCConstants.TNQ_NQ_GQ_ID + "=" + String.valueOf(getMainQueueID(sourceAddress.queueName)) +
                    " AND " + JDBCConstants.QUEUE_ID + "=" + String.valueOf(getDestQueueID(destinationQueue));

            Statement stmt = connection.createStatement();
            movedCount = stmt.executeUpdate(sqlString);
            stmt.close();
        } catch (SQLException e) {
            logger.error("error occurred while trying to move metadata from " + sourceAddress.queueName + " to "
                            + targetAddress.queueName);
        }
        return movedCount;
    }

    @Override
    public int countMessagesOfQueue(QueueAddress queueAddress, String destinationQueueNameToMatch) throws AndesException {
        String select = "SELECT COUNT(" + JDBCConstants.TNQ_NQ_GQ_ID + ") AS count" +
                " WHERE " + JDBCConstants.TNQ_NQ_GQ_ID +
                " IN (SELECT " + JDBCConstants.TNQ_NQ_GQ_ID + " FROM " + JDBCConstants.TNQ_NQ_GQ_TABLE +
                " WHERE " + JDBCConstants.TNQ_NQ_GQ_NAME + "=" + queueAddress.queueName + ") " +
                " AND " + JDBCConstants.QUEUE_ID +
                " IN ( SELECT " + JDBCConstants.QUEUE_ID +
                " WHERE " + JDBCConstants.QUEUE_NAME + "=" + destinationQueueNameToMatch + ") ";

        int count = 0;
        try {
            Statement stmt = connection.createStatement();
            ResultSet results =stmt.executeQuery(select);
            while (results.next()){
                count = results.getInt("count");
            }
        } catch (SQLException e) {
            logger.error("error occurred while retrieving message count of queue");
        }
        return count;
    }

    @Override
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {
        String select = "SELECT COUNT(" + JDBCConstants.QUEUE_ID +") AS count" +
                " FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.QUEUE_ID +
                " IN ( SELECT " + JDBCConstants.QUEUE_ID + " FROM " + JDBCConstants.QUEUES_TABLE +
                " WHERE " + JDBCConstants.QUEUE_NAME +"=" + destinationQueueName +")";

        long count = 0;
        try {
            Statement stmt = connection.createStatement();
            ResultSet results = stmt.executeQuery(select);
            while (results.next()){
                count = results.getLong("count");
            }
            stmt.close();
        } catch (SQLException e) {
            logger.error("error occurred while retrieving message count for queue");
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
            Statement stmt = connection.createStatement();
            ResultSet results = stmt.executeQuery(select);
            while (results.next()){
                byte[] b = results.getBytes(JDBCConstants.METADATA);
                md = new AndesMessageMetadata(messageId, b, true);
            }
        } catch (SQLException e) {
            logger.error("error occurred while retrieving message metadata for msg id:" + messageId);
        }
        return md;
    }

    @Override
    public void close() {

    }

    @Override
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {

    }

    @Override
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime, boolean isMessageForTopic, String destination) throws CassandraDataAccessException {

    }

    @Override
    public void deleteMessageMetadata(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDLC) throws AndesException {

    }

    @Override
    public void moveToDeadLetterChannel(List<AndesRemovableMetadata> messageList) {
        // todo add a entry in tnq-nq-gq table then update
    }

    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(Long limit, String columnFamilyName, String keyspace) {
        return null;
    }

    private int getDestQueueID(final String destinationQueueName){

        Integer id = destinationQueueMap.get(destinationQueueName);
        if(id != null){
            return id;
        }

        String sqlString = "INSERT INTO " + JDBCConstants.QUEUES_TABLE + "( "+ JDBCConstants.QUEUE_NAME +")" +
                " VALUES (\"" + destinationQueueName + "\")";
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate(sqlString, Statement.RETURN_GENERATED_KEYS);
            ResultSet results = stmt.getGeneratedKeys();
            if(results.first()){
               id = results.getInt(1);
            }
            destinationQueueMap.put(destinationQueueName, id);
        } catch (SQLException e) {
            logger.error("Error occurred while inserting destination queue details to database");
        }
        return id;
    }

    private int getMainQueueID( final String mainQueueName) {
        Integer id = mainQueueMap.get(mainQueueName);
        if(id != null){
            return id;
        }

        String sqlString = "INSERT INTO " + JDBCConstants.TNQ_NQ_GQ_TABLE + "( "+ JDBCConstants.TNQ_NQ_GQ_NAME +")" +
                " VALUES (\"" + mainQueueName + "\")";
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate(sqlString, Statement.RETURN_GENERATED_KEYS);
            ResultSet results = stmt.getGeneratedKeys();
            if(results.first()){
               id = results.getInt(1);
            }
            mainQueueMap.put(mainQueueName, id);
        } catch (SQLException e) {
            logger.error("Error occurred while inserting queue details to database");
        }
        return id;
    }

    private void createTables() {
        try {
            String[] queries = {
                    "CREATE TABLE " + JDBCConstants.MESSAGES_TABLE + " (" +
                            JDBCConstants.MESSAGE_ID + " BIGINT, " +
                            JDBCConstants.MSG_OFFSET + " INT, "  +
                            JDBCConstants.MESSAGE_CONTENT + " BINARY NOT NULL, " +
                            "PRIMARY KEY (" + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.MSG_OFFSET + ")" +
                            ")",

                    "CREATE TABLE " + JDBCConstants.QUEUES_TABLE + " (" +
                            JDBCConstants.QUEUE_ID + " INT AUTO_INCREMENT, " +
                            JDBCConstants.QUEUE_NAME + " VARCHAR NOT NULL, " +
                            "UNIQUE (" + JDBCConstants.QUEUE_NAME + ")," +
                            "PRIMARY KEY (" + JDBCConstants.QUEUE_ID + ")" +
                            ")",

                    "CREATE TABLE " + JDBCConstants.REF_COUNT_TABLE + " (" +
                            JDBCConstants.MESSAGE_ID + " BIGINT, " +
                            JDBCConstants.REF_COUNT + " INT, " +
                            "PRIMARY KEY (" + JDBCConstants.MESSAGE_ID +")" +
                            ")",

                    "CREATE TABLE " + JDBCConstants.TNQ_NQ_GQ_TABLE + " (" +
                            JDBCConstants.TNQ_NQ_GQ_ID + " INT AUTO_INCREMENT, " +
                            JDBCConstants.TNQ_NQ_GQ_NAME + " varchar(255), " +
                            "UNIQUE (" + JDBCConstants.TNQ_NQ_GQ_NAME + ")," +
                            "PRIMARY KEY (" + JDBCConstants.TNQ_NQ_GQ_ID + ")" +
                            ")",

                    "CREATE TABLE " + JDBCConstants.METADATA_TABLE + "(" +
                            JDBCConstants.MESSAGE_ID + " BIGINT, " +
                            JDBCConstants.QUEUE_ID + " INT, " +
                            JDBCConstants.TNQ_NQ_GQ_ID + " INT, " +
                            JDBCConstants.METADATA + " BINARY, " +
                            "PRIMARY KEY (" + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.QUEUE_ID + "," + JDBCConstants.TNQ_NQ_GQ_ID + "), " +
                            "FOREIGN KEY (" + JDBCConstants.QUEUE_ID + ") REFERENCES queues (" + JDBCConstants.QUEUE_ID + "), " +
                            "FOREIGN KEY (" + JDBCConstants.TNQ_NQ_GQ_ID + ") REFERENCES tnq_nq_gnq (" + JDBCConstants.TNQ_NQ_GQ_ID + ")" +
                            ")"


            };
            Statement stmt = connection.createStatement();
            for(String q: queries) {
                stmt.addBatch(q);
            }
            stmt.executeBatch();
            stmt.close();
        } catch (SQLException e) {
            logger.error("Creating database schema failed!", e.getCause());
        }
    }
}
