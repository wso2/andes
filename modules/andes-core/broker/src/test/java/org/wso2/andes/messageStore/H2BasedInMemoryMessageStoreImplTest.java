package org.wso2.andes.messageStore;

import junit.framework.TestCase;
import org.h2.jdbcx.JdbcDataSource;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.MessageStore;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameAlreadyBoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class H2BasedInMemoryMessageStoreImplTest extends TestCase {

    private MessageStore messageStore;
    private Connection connection;
    private static boolean isInitialised = false;
    private static InitialContext ic;

    public void setUp() throws Exception {
        super.setUp();
        Class.forName("org.h2.Driver");
        connection = DriverManager.getConnection("jdbc:h2:mem:msg_store;mode=mysql");

        try {
            if (!isInitialised) {
                // Create initial context
                System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
                        "org.apache.naming.java.javaURLContextFactory");
                System.setProperty(Context.URL_PKG_PREFIXES,
                        "org.apache.naming");

                ic = new InitialContext();
                ic.createSubcontext("jdbc");
                JdbcDataSource ds = new JdbcDataSource();
                ds.setURL("jdbc:h2:mem:msg_store;mode=mysql");
                ic.bind("jdbc/InMemoryMessageStoreDB", ds);
                isInitialised = true;
            }
        } catch (NameAlreadyBoundException ignored) {
        }

        messageStore = new H2BasedInMemoryMessageStoreImpl();
        messageStore.initializeMessageStore(null);

    }

    public void tearDown() throws Exception {
//        messageStore.close();
    }

    public void testStoreRetrieveMessagePart() throws Exception {

        // store messages
        List<AndesMessagePart> list = JDBCTestHelper.getMessagePartList(0, 10);
        messageStore.storeMessagePart(list);

        // retrieve
        for (AndesMessagePart msgPart : list) {
            AndesMessagePart p = messageStore.getContent(msgPart.getMessageID(), msgPart.getOffSet());
            assert p != null;
            assertEquals(msgPart.getMessageID(), p.getMessageID());
            assertEquals(true, Arrays.equals(msgPart.getData(), p.getData()));
            assertEquals(msgPart.getDataLength(), p.getDataLength());
            assertEquals(msgPart.getOffSet(), p.getOffSet());
        }
    }

    public void testDeleteMessageParts() throws Exception {

        int firstMsgId = 10;
        int lastMsgId = 20;
        // store messages
        List<AndesMessagePart> list = JDBCTestHelper.getMessagePartList(firstMsgId, lastMsgId);
        messageStore.storeMessagePart(list);

        List<Long> longList = new ArrayList<Long>(lastMsgId - firstMsgId);
        for (int i = firstMsgId; i < lastMsgId; i++) {
            longList.add((long) i);
        }

        // Delete
        messageStore.deleteMessageParts(longList);

        // check for deletion
        String sqlStr = "SELECT * " +
                " FROM " + JDBCConstants.MESSAGES_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=?";
        PreparedStatement preparedStatement = connection.prepareStatement(sqlStr);
        for (int i = firstMsgId; i < lastMsgId; i++) {
            preparedStatement.setLong(1, i);
            preparedStatement.addBatch();
        }
        ResultSet resultSet = preparedStatement.executeQuery();
        assertEquals(false, resultSet.next());
    }

    public void testAckReceived() throws Exception {


    }

//    public void testInputThroughput() throws Exception{
//        List<AndesMessagePart> msgList = JDBCTestHelper.getMessagePartList(0, 100000);
//
//        String destQueueName = "queue_";
//        int firstMsgId = 1;
//        int lastMsgId = firstMsgId + 100000;
//        List<AndesMessageMetadata> lst = JDBCTestHelper.getMetadataList(destQueueName, firstMsgId, lastMsgId);
//
//        long t = System.currentTimeMillis();
//        messageStore.storeMessagePart(msgList);
//        messageStore.addMetaData(lst);
//
//        System.out.println("time " + (System.currentTimeMillis() - t));
//
//    }

    public void testAddMetaDataList() throws Exception {

        String destQueueName = "queue_";
        int firstMsgId = 1;
        int lastMsgId = firstMsgId + 10;
        List<AndesMessageMetadata> lst = JDBCTestHelper.getMetadataList(destQueueName, firstMsgId, lastMsgId);

        // add metadata
        messageStore.addMetaData(lst);

        // TEST
        String sqlStr = "SELECT * FROM " + JDBCConstants.METADATA_TABLE;

        PreparedStatement preparedStatement = connection.prepareStatement(sqlStr);
        ResultSet resultSet = preparedStatement.executeQuery();

        for (AndesMessageMetadata md : lst) {
            assertEquals(true, resultSet.next());
            assertEquals(md.getMessageID(), resultSet.getLong(JDBCConstants.MESSAGE_ID));
            assertEquals(true, Arrays.equals(md.getMetadata(), resultSet.getBytes(JDBCConstants.METADATA)));
        }

        sqlStr = "SELECT * FROM " + JDBCConstants.EXPIRATION_TABLE;
        preparedStatement = connection.prepareStatement(sqlStr);
        resultSet = preparedStatement.executeQuery();
        int count = 0;
        while (resultSet.next()) {
            assertEquals(true, resultSet.getLong(JDBCConstants.EXPIRATION_TIME) > 0);
            count++;
        }
        assertEquals(5, count);
    }

    public void testAddMetaData() throws Exception {
        int msgId = 2; // JDBCTestHelper returns positive expiry values for even number message ids
        AndesMessageMetadata md = JDBCTestHelper.getMetadata(msgId, "myQueue");
        AndesMessageMetadata md2 = JDBCTestHelper.getMetadata(msgId, "myQueue2"); // to test ref count
        messageStore.addMetaData(md);
        messageStore.addMetaData(md2);

        // Test Metadata
        String sql = "SELECT * FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=?";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, msgId);
        ResultSet resultSet = preparedStatement.executeQuery();

        assertEquals(true, resultSet.first());
        assertEquals(true, Arrays.equals(md.getMetadata(), resultSet.getBytes(JDBCConstants.METADATA)));
        assertEquals(msgId, resultSet.getLong(JDBCConstants.MESSAGE_ID));

//        assertEquals(true, resultSet.next());
//        assertEquals(true, Arrays.equals(md2.getMetadata(), resultSet.getBytes(JDBCConstants.METADATA)));
//        assertEquals(msgId, resultSet.getLong(JDBCConstants.MESSAGE_ID));

        // todo check refcount

        // test expiry
        sql = " SELECT * FROM " + JDBCConstants.EXPIRATION_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=?";

        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, msgId);
        resultSet = preparedStatement.executeQuery();

        assertEquals(true, resultSet.first());
        assertEquals(msgId, resultSet.getLong(JDBCConstants.MESSAGE_ID));
    }

    public void testAddMetadataToQueue() throws Exception {
        AndesMessageMetadata md = new AndesMessageMetadata();
        int msgId = 1;
        String specificQueue = "DLC";
        byte[] content = "test content".getBytes();

        md.setMessageID(msgId);
        md.setDestination("my_queue");
        md.setMetadata(content);
        md.setExpirationTime(System.currentTimeMillis() + 10000);

        messageStore.addMetaDataToQueue(specificQueue, md);

        // TEST
        String sql = "SELECT * FROM " + JDBCConstants.METADATA_TABLE +
                " JOIN " + JDBCConstants.QUEUES_TABLE +
                " WHERE " + JDBCConstants.METADATA_TABLE + "." + JDBCConstants.QUEUE_ID + "=" +
                JDBCConstants.QUEUES_TABLE + "." + JDBCConstants.QUEUE_ID +
                " AND " + JDBCConstants.MESSAGE_ID + "=? ";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, msgId);
        ResultSet resultSet = preparedStatement.executeQuery();

        assertEquals(true, resultSet.first());
        assertEquals(true, Arrays.equals(content, resultSet.getBytes(JDBCConstants.METADATA)));
        assertEquals(msgId, resultSet.getLong(JDBCConstants.MESSAGE_ID));
        assertEquals(specificQueue, resultSet.getString(JDBCConstants.QUEUE_NAME));
    }

    public void testAddMetadataListToQueue() throws Exception {
        String destQueueName = "queue_";
        String specificQueue = "DLC";
        int firstMsgId = 1;
        int lastMsgId = firstMsgId + 10;

        // add metadata
        List<AndesMessageMetadata> lst = JDBCTestHelper.getMetadataList(destQueueName, firstMsgId, lastMsgId);
        messageStore.addMetadataToQueue(specificQueue, lst);

        // TEST
        String sql = "SELECT * FROM " + JDBCConstants.METADATA_TABLE +
                " JOIN " + JDBCConstants.QUEUES_TABLE +
                " WHERE " + JDBCConstants.METADATA_TABLE + "." + JDBCConstants.QUEUE_ID + "=" +
                JDBCConstants.QUEUES_TABLE + "." + JDBCConstants.QUEUE_ID;

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        for (AndesMessageMetadata md : lst) {
            assertEquals(true, resultSet.next());
            assertEquals(md.getMessageID(), resultSet.getLong(JDBCConstants.MESSAGE_ID));
            assertEquals(true, Arrays.equals(md.getMetadata(), resultSet.getBytes(JDBCConstants.METADATA)));
            assertEquals(specificQueue, resultSet.getString(JDBCConstants.QUEUE_NAME));
        }

        sql = "SELECT * FROM " + JDBCConstants.EXPIRATION_TABLE;
        preparedStatement = connection.prepareStatement(sql);
        resultSet = preparedStatement.executeQuery();
        int count = 0;
        while (resultSet.next()) {
            assertEquals(true, resultSet.getLong(JDBCConstants.EXPIRATION_TIME) > 0);
            count++;
        }
        assertEquals(5, count);
    }

    public void testGetMessageCountForQueue() throws Exception {

        String[] destQueues = {"queue_1", "queue_2"};
        int firstMsgId = 10;
        int lastMsgId = firstMsgId + 10;

        List<AndesMessageMetadata> lst = JDBCTestHelper.getMetadataForMultipleQueues(destQueues, 2, firstMsgId, lastMsgId);
        messageStore.addMetaData(lst);

        //TEST
        long count = messageStore.getMessageCountForQueue(destQueues[0] + "noQueue");
        assertEquals(0, count); // no such queue
        count = messageStore.getMessageCountForQueue(destQueues[0]);
        assertEquals(5, count);
        count = messageStore.getMessageCountForQueue(destQueues[1]);
        assertEquals(5, count);
    }

    public void testGetMetaData() throws Exception {

        int firstMsgId = 1;
        int lastMsgId = firstMsgId + 10;
        String destQueueName = "queue";
        List<AndesMessageMetadata> lst = JDBCTestHelper.getMetadataList(destQueueName, firstMsgId, lastMsgId);
        messageStore.addMetaData(lst);

        //TEST
        for (AndesMessageMetadata md : lst) {
            AndesMessageMetadata retrieved = messageStore.getMetaData(md.getMessageID());
            assertEquals(md.getMessageID(), retrieved.getMessageID());
            assertEquals(true, Arrays.equals(md.getMetadata(), retrieved.getMetadata()));
            assertEquals(destQueueName, md.getDestination());
        }
    }

    public void testGetMetaDataList() throws Exception {

        String destQueue_1 = "queue_1";
        String destQueue_2 = "queue_2";
        messageStore.addMetaData(JDBCTestHelper.getMetadataList(destQueue_1, 0, 5));
        messageStore.addMetaData(JDBCTestHelper.getMetadataList(destQueue_2, 5, 10));

        // Retrieve
        List<AndesMessageMetadata> list = messageStore.getMetaDataList(destQueue_1, 0, 5);
        // Test
        assertEquals(5, list.size());
        for (AndesMessageMetadata andesMessageMetadata : list) {
            assertEquals(0, destQueue_1.compareTo(andesMessageMetadata.getDestination()));
        }

        // Retrieve
        list = messageStore.getMetaDataList(destQueue_2, 5, 10);
        // Test
        assertEquals(5, list.size());
        for (AndesMessageMetadata andesMessageMetadata : list) {
            assertEquals(0, destQueue_2.compareTo(andesMessageMetadata.getDestination()));
        }
    }

    public void testGetNextNMessageMetadataFromQueue() throws Exception {

        String destQueues[] = {"queue_1", "queue_2"};
        messageStore.addMetaData(JDBCTestHelper.getMetadataForMultipleQueues(destQueues, 2, 0, 10));

        // Retrieve
        List<AndesMessageMetadata> mdList =
                messageStore.getNextNMessageMetadataFromQueue(destQueues[0], 0, 3);
        // Test
        assertEquals(3, mdList.size());
        for (AndesMessageMetadata andesMessageMetadata : mdList) {
            assertEquals(0, destQueues[0].compareTo(andesMessageMetadata.getDestination()));
        }

        // Retrieve
        mdList = messageStore.getNextNMessageMetadataFromQueue(destQueues[1], 2, 5);
        // Test
        assertEquals(4, mdList.size());
        for (AndesMessageMetadata andesMessageMetadata : mdList) {
            assertEquals(0, destQueues[1].compareTo(andesMessageMetadata.getDestination()));
        }
    }
}