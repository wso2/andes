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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameAlreadyBoundException;

import junit.framework.Assert;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.*;
import org.wso2.andes.store.rdbms.h2.H2MemAndesContextStoreImpl;

public class RDBMSMessageStoreImplTest {

    private MessageStore messageStore;
    private static Connection connection;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        try {
            // Create initial context
            System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
                    "org.apache.naming.java.javaURLContextFactory");
            System.setProperty(Context.URL_PKG_PREFIXES,
                    "org.apache.naming");

            InitialContext ic = new InitialContext();
            ic.createSubcontext("jdbc");
            JdbcDataSource ds = new JdbcDataSource();
            ds.setURL("jdbc:h2:mem:msg_store;DB_CLOSE_ON_EXIT=FALSE");
            ic.bind(RDBMSConstants.H2_MEM_JNDI_LOOKUP_NAME, ds);

            Class.forName("org.h2.Driver");
            connection = DriverManager.getConnection("jdbc:h2:mem:msg_store;DB_CLOSE_ON_EXIT=FALSE");
        } catch (NameAlreadyBoundException ignored) {
        }
    }

    @Before
    public void setUp() throws Exception {
        createTables();
        messageStore = new RDBMSMessageStoreImpl();
        AndesContextStore contextStore = new H2MemAndesContextStoreImpl();
        ConfigurationProperties connectionProperties = new ConfigurationProperties();
        connectionProperties.addProperty(RDBMSConstants.PROP_JNDI_LOOKUP_NAME,
                                            RDBMSConstants.H2_MEM_JNDI_LOOKUP_NAME);
        messageStore.initializeMessageStore(contextStore, connectionProperties);
    }

    @After
    public void tearDown() throws Exception {
        messageStore.close();
        dropTables();
    }

    @Test
    public void testStoreRetrieveMessagePart() throws Exception {

        // store messages
        List<AndesMessagePart> list = RDBMSTestHelper.getMessagePartList(0, 10);
        messageStore.storeMessagePart(list);

        // retrieve
        for (AndesMessagePart msgPart : list) {
            AndesMessagePart p = messageStore.getContent(msgPart.getMessageID(), msgPart.getOffSet());
            assert p != null;
            Assert.assertEquals(msgPart.getMessageID(), p.getMessageID());
            Assert.assertEquals(true, Arrays.equals(msgPart.getData(), p.getData()));
            Assert.assertEquals(msgPart.getDataLength(), p.getDataLength());
            Assert.assertEquals(msgPart.getOffSet(), p.getOffSet());
        }
    }
    
    @Test
    public void testGetMessageContent() {
        
        // store message part
        
        
    }

    @Test
    public void storeSingleMessagePart() throws Exception {

        // create message parts
        List<AndesMessagePart> list = new ArrayList<AndesMessagePart>(2);
        byte[] content = "test content".getBytes();
        int msgid = 1;
        int offset = 0;
        AndesMessagePart messagePart = new AndesMessagePart();
        messagePart.setMessageID(msgid);
        messagePart.setData(content);
        messagePart.setOffSet(offset);
        messagePart.setDataLength(content.length);
        list.add(messagePart);

        // store
        messageStore.storeMessagePart(list);

        String select = "SELECT * FROM " + RDBMSConstants.CONTENT_TABLE;
        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(select);

        // test
        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(msgid, resultSet.getLong(RDBMSConstants.MESSAGE_ID));
        Assert.assertEquals(true, Arrays.equals(content, resultSet.getBytes(RDBMSConstants.MESSAGE_CONTENT)));
        Assert.assertEquals(offset, resultSet.getInt(RDBMSConstants.MSG_OFFSET));

    }

    @Test
    public void storeMultipleMessagePartsWithSameMsgId() throws Exception {
        // create message parts
        List<AndesMessagePart> list = new ArrayList<AndesMessagePart>(2);
        byte[] content = "test content".getBytes();
        int msgId = 1;
        int offset = 0;
        AndesMessagePart messagePart = new AndesMessagePart();
        messagePart.setMessageID(msgId);
        messagePart.setData(content);
        messagePart.setOffSet(offset);
        messagePart.setDataLength(content.length);
        list.add(messagePart);

        int offset2 = 2;
        AndesMessagePart messagePart2 = new AndesMessagePart();
        messagePart2.setMessageID(msgId);
        messagePart2.setData(content);
        messagePart2.setOffSet(offset2);
        messagePart2.setDataLength(content.length);
        list.add(messagePart2);

        // store
        messageStore.storeMessagePart(list);

        String select = "SELECT * FROM " + RDBMSConstants.CONTENT_TABLE;
        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(select);

        resultSet.first();
        Assert.assertEquals(true, resultSet.next());
    }

    @Test
    public void testAckReceived() throws Exception {


    }

    @Test
    public void testAddMetaDataList() throws Exception {

        String destQueueName = "queue_";
        int firstMsgId = 1;
        int lastMsgId = firstMsgId + 10;
        List<AndesMessageMetadata> lst = RDBMSTestHelper.getMetadataList(destQueueName, firstMsgId, lastMsgId);

        // add metadata
        messageStore.addMetadata(lst);

        // TEST
        String sqlStr = "SELECT * FROM " + RDBMSConstants.METADATA_TABLE;

        PreparedStatement preparedStatement = connection.prepareStatement(sqlStr);
        ResultSet resultSet = preparedStatement.executeQuery();

        for (AndesMessageMetadata md : lst) {
            Assert.assertEquals(true, resultSet.next());
            Assert.assertEquals(md.getMessageID(), resultSet.getLong(RDBMSConstants.MESSAGE_ID));
            Assert.assertEquals(true, Arrays.equals(md.getMetadata(), resultSet.getBytes(RDBMSConstants.METADATA)));
        }

        sqlStr = "SELECT * FROM " + RDBMSConstants.EXPIRATION_TABLE;
        preparedStatement = connection.prepareStatement(sqlStr);
        resultSet = preparedStatement.executeQuery();
        int count = 0;
        while (resultSet.next()) {
            Assert.assertEquals(true, resultSet.getLong(RDBMSConstants.EXPIRATION_TIME) > 0);
            count++;
        }
        Assert.assertEquals(5, count);
    }

    @Test
    public void testAddMetaData() throws Exception {
        int msgId = 2; // JDBCTestHelper returns positive expiry values for even number message ids
        AndesMessageMetadata md = RDBMSTestHelper.getMetadata(msgId, "myQueue");
//        AndesMessageMetadata md2 = JDBCTestHelper.getMetadata(msgId, "myQueue2"); // to test ref count
        messageStore.addMetadata(md);
//        messageStore.addMetadata(md2);

        // Test Metadata
        String sql = "SELECT * FROM " + RDBMSConstants.METADATA_TABLE +
                " WHERE " + RDBMSConstants.MESSAGE_ID + "=?";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, msgId);
        ResultSet resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(true, Arrays.equals(md.getMetadata(), resultSet.getBytes(RDBMSConstants.METADATA)));
        Assert.assertEquals(msgId, resultSet.getLong(RDBMSConstants.MESSAGE_ID));

//        Assert.assertEquals(true, resultSet.next());
//        Assert.assertEquals(true, Arrays.equals(md2.getMetadata(), resultSet.getBytes(JDBCConstants.METADATA)));
//        Assert.assertEquals(msgId, resultSet.getLong(JDBCConstants.MESSAGE_ID));

        // todo check refcount

        // test expiry
        sql = " SELECT * FROM " + RDBMSConstants.EXPIRATION_TABLE +
                " WHERE " + RDBMSConstants.MESSAGE_ID + "=?";

        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, msgId);
        resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(msgId, resultSet.getLong(RDBMSConstants.MESSAGE_ID));
    }

    @Test
    public void testAddMetadataToQueue() throws Exception {
        AndesMessageMetadata md = new AndesMessageMetadata();
        int msgId = 1;
        String specificQueue = "DLC";
        byte[] content = "test content".getBytes();

        md.setMessageID(msgId);
        md.setDestination("my_queue");
        md.setStorageQueueName("my_queue");
        md.setMetadata(content);
        md.setExpirationTime(System.currentTimeMillis() + 10000);

        messageStore.addMetadataToQueue(specificQueue, md);

        // TEST
        String sql = "SELECT * FROM " + RDBMSConstants.METADATA_TABLE +
                " JOIN " + RDBMSConstants.QUEUES_TABLE +
                " WHERE " + RDBMSConstants.METADATA_TABLE + "." + RDBMSConstants.QUEUE_ID + "=" +
                RDBMSConstants.QUEUES_TABLE + "." + RDBMSConstants.QUEUE_ID +
                " AND " + RDBMSConstants.MESSAGE_ID + "=? ";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, msgId);
        ResultSet resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(true, Arrays.equals(content, resultSet.getBytes(RDBMSConstants.METADATA)));
        Assert.assertEquals(msgId, resultSet.getLong(RDBMSConstants.MESSAGE_ID));
        Assert.assertEquals(specificQueue, resultSet.getString(RDBMSConstants.QUEUE_NAME));
    }

    @Test
    public void testAddMetadataListToQueue() throws Exception {
        String destQueueName = "queue_";
        String specificQueue = "DLC";
        int firstMsgId = 1;
        int lastMsgId = firstMsgId + 10;

        // add metadata
        List<AndesMessageMetadata> lst = RDBMSTestHelper.getMetadataList(destQueueName, firstMsgId, lastMsgId);
        messageStore.addMetadataToQueue(specificQueue, lst);

        // TEST
        String sql = "SELECT * FROM " + RDBMSConstants.METADATA_TABLE +
                " JOIN " + RDBMSConstants.QUEUES_TABLE +
                " WHERE " + RDBMSConstants.METADATA_TABLE + "." + RDBMSConstants.QUEUE_ID + "=" +
                RDBMSConstants.QUEUES_TABLE + "." + RDBMSConstants.QUEUE_ID;

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        for (AndesMessageMetadata md : lst) {
            Assert.assertEquals(true, resultSet.next());
            Assert.assertEquals(md.getMessageID(), resultSet.getLong(RDBMSConstants.MESSAGE_ID));
            Assert.assertEquals(true, Arrays.equals(md.getMetadata(), resultSet.getBytes(RDBMSConstants.METADATA)));
            Assert.assertEquals(specificQueue, resultSet.getString(RDBMSConstants.QUEUE_NAME));
        }

        sql = "SELECT * FROM " + RDBMSConstants.EXPIRATION_TABLE;
        preparedStatement = connection.prepareStatement(sql);
        resultSet = preparedStatement.executeQuery();
        int count = 0;
        while (resultSet.next()) {
            Assert.assertEquals(true, resultSet.getLong(RDBMSConstants.EXPIRATION_TIME) > 0);
            count++;
        }
        Assert.assertEquals(5, count);
    }

    @Test
    public void testGetMetaData() throws Exception {

        int firstMsgId = 1;
        int lastMsgId = firstMsgId + 10;
        String destQueueName = "queue";
        List<AndesMessageMetadata> lst = RDBMSTestHelper.getMetadataList(destQueueName, firstMsgId, lastMsgId);
        messageStore.addMetadata(lst);

        //TEST
        for (AndesMessageMetadata md : lst) {
            AndesMessageMetadata retrieved = messageStore.getMetadata(md.getMessageID());
            Assert.assertEquals(md.getMessageID(), retrieved.getMessageID());
            Assert.assertEquals(true, Arrays.equals(md.getMetadata(), retrieved.getMetadata()));
            Assert.assertEquals(destQueueName, md.getStorageQueueName());
        }
    }

    @Test
    public void testGetMetaDataList() throws Exception {

        String destQueue_1 = "queue_1";
        String destQueue_2 = "queue_2";
        messageStore.addMetadata(RDBMSTestHelper.getMetadataList(destQueue_1, 0, 5));
        messageStore.addMetadata(RDBMSTestHelper.getMetadataList(destQueue_2, 5, 10));

        // Retrieve
        List<DeliverableAndesMetadata> list = messageStore.getMetadataList(null, destQueue_1, 0, 5);
        // Test
        Assert.assertEquals(5, list.size());
        for (AndesMessageMetadata andesMessageMetadata : list) {
            Assert.assertEquals(0, destQueue_1.compareTo(andesMessageMetadata.getStorageQueueName()));
        }

        // Retrieve
        list = messageStore.getMetadataList(null, destQueue_2, 5, 10);
        // Test
        Assert.assertEquals(5, list.size());
        for (AndesMessageMetadata andesMessageMetadata : list) {
            Assert.assertEquals(0, destQueue_2.compareTo(andesMessageMetadata.getStorageQueueName()));
        }
    }

    @Test
    public void testGetExpiredMessage() throws Exception {
        String destQueue_1 = "queue_1";
        String destQueue_2 = "queue_2";

        // only the even number msg ids will be given expiration values by this method
        List<AndesMessageMetadata> mdList1 = RDBMSTestHelper.getMetadataList(destQueue_1, 0, 5, 1);
        List<AndesMessageMetadata> mdList2 = RDBMSTestHelper.getMetadataList(destQueue_2, 5, 10, 1);

        messageStore.addMetadata(mdList1);
        messageStore.addMetadata(mdList2);

        Thread.sleep(500);

        // get first batch
        List<AndesMessageMetadata> list = messageStore.getExpiredMessages(5);
        Assert.assertEquals(5, list.size());

        list = messageStore.getExpiredMessages(3);
        Assert.assertEquals(3, list.size());
        for (int i = 0; i < list.size(); i++) {
            AndesMessageMetadata md = list.get(i);
            Assert.assertEquals(i * 2, md.getMessageID());
            Assert.assertEquals(destQueue_1, md.getStorageQueueName());
        }

        // delete them
        //messageStore.deleteMessages(list, false);

        // get second batch
        list = messageStore.getExpiredMessages(2);
        Assert.assertEquals(2, list.size());

        AndesMessageMetadata md = list.get(0);
        Assert.assertEquals(6, md.getMessageID());
        Assert.assertEquals(destQueue_2, md.getStorageQueueName());

        md = list.get(1);
        Assert.assertEquals(8, md.getMessageID());
        Assert.assertEquals(destQueue_2, md.getStorageQueueName());

    }

    @Test
    public void testGetNextNMessageMetadataFromQueue() throws Exception {

        String destQueues[] = {"queue_1", "queue_2"};
        messageStore.addMetadata(RDBMSTestHelper.getMetadataForMultipleQueues(destQueues, 2, 0, 10));

        // Retrieve
        List<AndesMessageMetadata> mdList =
                messageStore.getNextNMessageMetadataFromQueue(destQueues[0], 0, 3);
        // Test
        Assert.assertEquals(3, mdList.size());
        for (AndesMessageMetadata andesMessageMetadata : mdList) {
            Assert.assertEquals(0, destQueues[0].compareTo(andesMessageMetadata.getStorageQueueName()));
        }

        // Retrieve
        mdList = messageStore.getNextNMessageMetadataFromQueue(destQueues[1], 2, 5);
        // Test
        Assert.assertEquals(4, mdList.size());
        for (AndesMessageMetadata andesMessageMetadata : mdList) {
            Assert.assertEquals(0, destQueues[1].compareTo(andesMessageMetadata.getStorageQueueName()));
        }
    }

    private void createTables() throws SQLException {
        String[] queries = {
                "CREATE TABLE IF NOT EXISTS MB_CONTENT (" +
                        "MESSAGE_ID BIGINT, " +
                        "CONTENT_OFFSET INT, " +
                        "MESSAGE_CONTENT BLOB NOT NULL, " +
                        "PRIMARY KEY (MESSAGE_ID,CONTENT_OFFSET)" +
                        ");"
                ,

                "CREATE TABLE IF NOT EXISTS MB_QUEUE_MAPPING (" +
                        "QUEUE_ID INT AUTO_INCREMENT, " +
                        "QUEUE_NAME VARCHAR NOT NULL, " +
                        "UNIQUE (QUEUE_NAME)," +
                        "PRIMARY KEY (QUEUE_ID)" +
                        ");",

                "CREATE TABLE IF NOT EXISTS MB_METADATA (" +
                        "MESSAGE_ID BIGINT, " +
                        "QUEUE_ID INT, " +
                        "MESSAGE_METADATA BINARY, " +
                        "PRIMARY KEY (MESSAGE_ID, QUEUE_ID), " +
                        "FOREIGN KEY (QUEUE_ID) REFERENCES MB_QUEUE_MAPPING (QUEUE_ID) " +
                        ");"
                ,

                "CREATE TABLE IF NOT EXISTS MB_EXPIRATION_DATA (" +
                        "MESSAGE_ID BIGINT UNIQUE," +
                        "EXPIRATION_TIME BIGINT, " +
                        "MESSAGE_DESTINATION VARCHAR NOT NULL, " +
                        "FOREIGN KEY (MESSAGE_ID) REFERENCES MB_METADATA (MESSAGE_ID)" +
                        ");"


        };
        Statement stmt = connection.createStatement();
        for (String q : queries) {
            stmt.addBatch(q);
        }
        stmt.executeBatch();
        stmt.close();

    }

    public void dropTables() throws SQLException {

        String[] queries = {
                "DROP TABLE MB_CONTENT",
                "DROP TABLE MB_QUEUE_MAPPING ",
                "DROP TABLE MB_METADATA ",
                "DROP TABLE MB_EXPIRATION_DATA "
        };
        Statement stmt = connection.createStatement();
        for (String q : queries) {
            stmt.addBatch(q);
        }
        stmt.executeBatch();
        stmt.close();
    }
}
