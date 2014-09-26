package org.wso2.andes.store.jdbc;

import junit.framework.Assert;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.*;
import org.wso2.andes.configuration.ConfigurationProperties;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.MessageStore;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameAlreadyBoundException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class H2BasedMessageStoreImplTest {

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
            ic.bind(JDBCConstants.H2_MEM_JNDI_LOOKUP_NAME, ds);

            Class.forName("org.h2.Driver");
            connection = DriverManager.getConnection("jdbc:h2:mem:msg_store;DB_CLOSE_ON_EXIT=FALSE");
        } catch (NameAlreadyBoundException ignored) {
        }
    }

    @Before
    public void setUp() throws Exception {
        createTables();
        messageStore = new H2BasedMessageStoreImpl();
        ConfigurationProperties connectionProperties = new ConfigurationProperties();
        connectionProperties.addProperty(JDBCConstants.PROP_JNDI_LOOKUP_NAME,
                                            JDBCConstants.H2_MEM_JNDI_LOOKUP_NAME);
        messageStore.initializeMessageStore(connectionProperties);
        ((H2BasedMessageStoreImpl)messageStore).createTables();
    }

    @After
    public void tearDown() throws Exception {
        messageStore.close();
        dropTables();
    }

    public void dropTables() throws SQLException {

        String[] queries = {
                "DROP TABLE messages",
                "DROP TABLE queues ",
                "DROP TABLE reference_counts ",
                "DROP TABLE metadata ",
                "DROP TABLE expiration_data "
        };
        Statement stmt = connection.createStatement();
        for (String q : queries) {
            stmt.addBatch(q);
        }
        stmt.executeBatch();
        stmt.close();
    }

    @Test
    public void testStoreRetrieveMessagePart() throws Exception {

        // store messages
        List<AndesMessagePart> list = JDBCTestHelper.getMessagePartList(0, 10);
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

        String select = "SELECT * FROM " + JDBCConstants.MESSAGES_TABLE;
        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(select);

        // test
        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(msgid, resultSet.getLong(JDBCConstants.MESSAGE_ID));
        Assert.assertEquals(true, Arrays.equals(content, resultSet.getBytes(JDBCConstants.MESSAGE_CONTENT)));
        Assert.assertEquals(offset, resultSet.getInt(JDBCConstants.MSG_OFFSET));

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

        String select = "SELECT * FROM " + JDBCConstants.MESSAGES_TABLE;
        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(select);

        resultSet.first();
        Assert.assertEquals(true, resultSet.next());
    }

    @Test
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
        Assert.assertEquals(false, resultSet.next());
    }

    @Test
    public void testAckReceived() throws Exception {


    }

    @Test
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
            Assert.assertEquals(true, resultSet.next());
            Assert.assertEquals(md.getMessageID(), resultSet.getLong(JDBCConstants.MESSAGE_ID));
            Assert.assertEquals(true, Arrays.equals(md.getMetadata(), resultSet.getBytes(JDBCConstants.METADATA)));
        }

        sqlStr = "SELECT * FROM " + JDBCConstants.EXPIRATION_TABLE;
        preparedStatement = connection.prepareStatement(sqlStr);
        resultSet = preparedStatement.executeQuery();
        int count = 0;
        while (resultSet.next()) {
            Assert.assertEquals(true, resultSet.getLong(JDBCConstants.EXPIRATION_TIME) > 0);
            count++;
        }
        Assert.assertEquals(5, count);
    }

    @Test
    public void testAddMetaData() throws Exception {
        int msgId = 2; // JDBCTestHelper returns positive expiry values for even number message ids
        AndesMessageMetadata md = JDBCTestHelper.getMetadata(msgId, "myQueue");
//        AndesMessageMetadata md2 = JDBCTestHelper.getMetadata(msgId, "myQueue2"); // to test ref count
        messageStore.addMetaData(md);
//        messageStore.addMetaData(md2);

        // Test Metadata
        String sql = "SELECT * FROM " + JDBCConstants.METADATA_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=?";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, msgId);
        ResultSet resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(true, Arrays.equals(md.getMetadata(), resultSet.getBytes(JDBCConstants.METADATA)));
        Assert.assertEquals(msgId, resultSet.getLong(JDBCConstants.MESSAGE_ID));

//        Assert.assertEquals(true, resultSet.next());
//        Assert.assertEquals(true, Arrays.equals(md2.getMetadata(), resultSet.getBytes(JDBCConstants.METADATA)));
//        Assert.assertEquals(msgId, resultSet.getLong(JDBCConstants.MESSAGE_ID));

        // todo check refcount

        // test expiry
        sql = " SELECT * FROM " + JDBCConstants.EXPIRATION_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=?";

        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, msgId);
        resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(msgId, resultSet.getLong(JDBCConstants.MESSAGE_ID));
    }

    @Test
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

        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(true, Arrays.equals(content, resultSet.getBytes(JDBCConstants.METADATA)));
        Assert.assertEquals(msgId, resultSet.getLong(JDBCConstants.MESSAGE_ID));
        Assert.assertEquals(specificQueue, resultSet.getString(JDBCConstants.QUEUE_NAME));
    }

    @Test
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
            Assert.assertEquals(true, resultSet.next());
            Assert.assertEquals(md.getMessageID(), resultSet.getLong(JDBCConstants.MESSAGE_ID));
            Assert.assertEquals(true, Arrays.equals(md.getMetadata(), resultSet.getBytes(JDBCConstants.METADATA)));
            Assert.assertEquals(specificQueue, resultSet.getString(JDBCConstants.QUEUE_NAME));
        }

        sql = "SELECT * FROM " + JDBCConstants.EXPIRATION_TABLE;
        preparedStatement = connection.prepareStatement(sql);
        resultSet = preparedStatement.executeQuery();
        int count = 0;
        while (resultSet.next()) {
            Assert.assertEquals(true, resultSet.getLong(JDBCConstants.EXPIRATION_TIME) > 0);
            count++;
        }
        Assert.assertEquals(5, count);
    }

    @Test
    public void testGetMessageCountForQueue() throws Exception {

        String[] destQueues = {"queue_1", "queue_2"};
        int firstMsgId = 10;
        int lastMsgId = firstMsgId + 10;

        List<AndesMessageMetadata> lst = JDBCTestHelper.getMetadataForMultipleQueues(destQueues, 2, firstMsgId, lastMsgId);
        messageStore.addMetaData(lst);

        //TEST
        long count = messageStore.getMessageCountForQueue(destQueues[0] + "noQueue");
        Assert.assertEquals(0, count); // no such queue
        count = messageStore.getMessageCountForQueue(destQueues[0]);
        Assert.assertEquals(5, count);
        count = messageStore.getMessageCountForQueue(destQueues[1]);
        Assert.assertEquals(5, count);
    }

    @Test
    public void testGetMetaData() throws Exception {

        int firstMsgId = 1;
        int lastMsgId = firstMsgId + 10;
        String destQueueName = "queue";
        List<AndesMessageMetadata> lst = JDBCTestHelper.getMetadataList(destQueueName, firstMsgId, lastMsgId);
        messageStore.addMetaData(lst);

        //TEST
        for (AndesMessageMetadata md : lst) {
            AndesMessageMetadata retrieved = messageStore.getMetaData(md.getMessageID());
            Assert.assertEquals(md.getMessageID(), retrieved.getMessageID());
            Assert.assertEquals(true, Arrays.equals(md.getMetadata(), retrieved.getMetadata()));
            Assert.assertEquals(destQueueName, md.getDestination());
        }
    }

    @Test
    public void testGetMetaDataList() throws Exception {

        String destQueue_1 = "queue_1";
        String destQueue_2 = "queue_2";
        messageStore.addMetaData(JDBCTestHelper.getMetadataList(destQueue_1, 0, 5));
        messageStore.addMetaData(JDBCTestHelper.getMetadataList(destQueue_2, 5, 10));

        // Retrieve
        List<AndesMessageMetadata> list = messageStore.getMetaDataList(destQueue_1, 0, 5);
        // Test
        Assert.assertEquals(5, list.size());
        for (AndesMessageMetadata andesMessageMetadata : list) {
            Assert.assertEquals(0, destQueue_1.compareTo(andesMessageMetadata.getDestination()));
        }

        // Retrieve
        list = messageStore.getMetaDataList(destQueue_2, 5, 10);
        // Test
        Assert.assertEquals(5, list.size());
        for (AndesMessageMetadata andesMessageMetadata : list) {
            Assert.assertEquals(0, destQueue_2.compareTo(andesMessageMetadata.getDestination()));
        }
    }

    @Test
    public void testGetExpiredMessage() throws Exception {
        String destQueue_1 = "queue_1";
        String destQueue_2 = "queue_2";

        // only the even number msg ids will be given expiration values by this method
        List<AndesMessageMetadata> mdList1 = JDBCTestHelper.getMetadataList(destQueue_1, 0, 5, 1);
        List<AndesMessageMetadata> mdList2 = JDBCTestHelper.getMetadataList(destQueue_2, 5, 10, 1);

        messageStore.addMetaData(mdList1);
        messageStore.addMetaData(mdList2);

        Thread.sleep(500);

        // get first batch
        List<AndesRemovableMetadata> list = messageStore.getExpiredMessages(5);
        Assert.assertEquals(5, list.size());

        list = messageStore.getExpiredMessages(3);
        Assert.assertEquals(3, list.size());
        for (int i = 0; i < list.size(); i++) {
            AndesRemovableMetadata md = list.get(i);
            Assert.assertEquals(i * 2, md.messageID);
            Assert.assertEquals(destQueue_1, md.destination);
        }

        // delete them
        messageStore.deleteMessages(list, false);

        // get second batch
        list = messageStore.getExpiredMessages(2);
        Assert.assertEquals(2, list.size());

        AndesRemovableMetadata md = list.get(0);
        Assert.assertEquals(6, md.messageID);
        Assert.assertEquals(destQueue_2, md.destination);

        md = list.get(1);
        Assert.assertEquals(8, md.messageID);
        Assert.assertEquals(destQueue_2, md.destination);

    }

    @Test
    public void testGetNextNMessageMetadataFromQueue() throws Exception {

        String destQueues[] = {"queue_1", "queue_2"};
        messageStore.addMetaData(JDBCTestHelper.getMetadataForMultipleQueues(destQueues, 2, 0, 10));

        // Retrieve
        List<AndesMessageMetadata> mdList =
                messageStore.getNextNMessageMetadataFromQueue(destQueues[0], 0, 3);
        // Test
        Assert.assertEquals(3, mdList.size());
        for (AndesMessageMetadata andesMessageMetadata : mdList) {
            Assert.assertEquals(0, destQueues[0].compareTo(andesMessageMetadata.getDestination()));
        }

        // Retrieve
        mdList = messageStore.getNextNMessageMetadataFromQueue(destQueues[1], 2, 5);
        // Test
        Assert.assertEquals(4, mdList.size());
        for (AndesMessageMetadata andesMessageMetadata : mdList) {
            Assert.assertEquals(0, destQueues[1].compareTo(andesMessageMetadata.getDestination()));
        }
    }

    private void createTables() throws SQLException {
        String[] queries = {
                "CREATE TABLE messages (" +
                        "message_id BIGINT, " +
                        "offset INT, " +
                        "content BINARY NOT NULL, " +
                        "PRIMARY KEY (message_id,offset)" +
                        ");"
                ,

                "CREATE TABLE queues (" +
                        "queue_id INT AUTO_INCREMENT, " +
                        "name VARCHAR NOT NULL, " +
                        "UNIQUE (name)," +
                        "PRIMARY KEY (queue_id)" +
                        ");",

                "CREATE TABLE reference_counts ( " +
                        "message_id BIGINT, " +
                        "reference_count INT, " +
                        "PRIMARY KEY (message_id)" +
                        ");"
                ,

                "CREATE TABLE metadata (" +
                        "message_id BIGINT, " +
                        "queue_id INT, " +
                        "data BINARY, " +
                        "PRIMARY KEY (message_id, queue_id), " +
                        "FOREIGN KEY (queue_id) " +
                        "REFERENCES queues (queue_id) " +
                        ");",

                "CREATE TABLE expiration_data (" +
                        "message_id BIGINT UNIQUE," +
                        "expiration_time BIGINT, " +
                        "destination VARCHAR NOT NULL, " +
                        "FOREIGN KEY (message_id) " +
                        "REFERENCES metadata (message_id)" +
                        "); "
        };
        Statement stmt = connection.createStatement();
        for (String q : queries) {
            stmt.addBatch(q);
        }
        stmt.executeBatch();
        stmt.close();

    }
}
