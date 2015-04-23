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

import junit.framework.Assert;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.*;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesExchange;
import org.wso2.andes.kernel.AndesQueue;

import javax.naming.Context;
import javax.naming.InitialContext;
import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Unit test class for JDBCAndesContextStoreImpl
 * Basic functionality of implemented methods are tested in this test class
 */
public class RDBMSAndesContextStoreImplTest {

    private static Connection connection;
    private RDBMSAndesContextStoreImpl contextStore;

    @BeforeClass
    public static void BeforeClass() throws Exception {
            // Create initial context
            String jdbcUrl = "jdbc:h2:mem:msg_store;DB_CLOSE_ON_EXIT=FALSE";
            System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
                    "org.apache.naming.java.javaURLContextFactory");
            System.setProperty(Context.URL_PKG_PREFIXES,
                    "org.apache.naming");

            InitialContext ic = new InitialContext();
            ic.createSubcontext("jdbc");
            JdbcDataSource ds = new JdbcDataSource();
            ds.setURL(jdbcUrl);
            ic.bind(RDBMSConstants.H2_MEM_JNDI_LOOKUP_NAME, ds);

            Class.forName("org.h2.Driver");
            connection = DriverManager.getConnection(jdbcUrl);
    }

    @Before
    public void setup() throws Exception {
        createTables();
        contextStore = new RDBMSAndesContextStoreImpl();

        // start in memory mode
        ConfigurationProperties configurationProperties = new ConfigurationProperties();
        configurationProperties.addProperty(RDBMSConstants.PROP_JNDI_LOOKUP_NAME, RDBMSConstants.H2_MEM_JNDI_LOOKUP_NAME);
        contextStore.init(configurationProperties);
    }

    @After
    public void tearDown() throws Exception {
        dropTables();
    }

    /**
     * Test storing a durable subscription is done properly through method
     * storeDurableSubscription(...)
     * @throws Exception
     */
    @Test
    public void testStoreDurableSubscription() throws Exception {
        String destIdentifier = "destination";
        String subId = "sub-0";
        String subEncodedAsStr = "data";
        contextStore.storeDurableSubscription(destIdentifier, subId, subEncodedAsStr);

        String select = "SELECT * FROM " + RDBMSConstants.DURABLE_SUB_TABLE;
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(select);

        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(subId, resultSet.getString(RDBMSConstants.DURABLE_SUB_ID));
        Assert.assertEquals(destIdentifier, resultSet.getString(RDBMSConstants.DESTINATION_IDENTIFIER));
        Assert.assertEquals(subEncodedAsStr, resultSet.getString(RDBMSConstants.DURABLE_SUB_DATA));

    }

    /**
     * Test whether the the queue counter is added with the count of 0
     *
     * @throws Exception
     */
    @Test
    public void testAddQueueCounter() throws Exception {

        String queueName = "queue1";
        int count = 0;
        contextStore.addMessageCounterForQueue(queueName);

        String select = "SELECT *  FROM " + RDBMSConstants.QUEUE_COUNTER_TABLE + " WHERE " +
                RDBMSConstants.QUEUE_NAME + "=?";
        PreparedStatement preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, queueName);
        ResultSet resultSet = preparedStatement.executeQuery();

        Assert.assertTrue("Entry should exist on DB for queue counter", resultSet.first());
        Assert.assertEquals(queueName, resultSet.getString(RDBMSConstants.QUEUE_NAME));
        Assert.assertEquals(count, resultSet.getInt(RDBMSConstants.MESSAGE_COUNT));
    }

    /**
     * Test with two counter creation calls. This should only create the counter once and for the
     * second call should do nothing
     */
    @Test
    public void testAddCounterTwiceWithSameQueueName() throws Exception {

        String queueName = "queue1";
        int count = 0;
        // try to add twice
        contextStore.addMessageCounterForQueue(queueName);
        contextStore.addMessageCounterForQueue(queueName);

        String select = "SELECT *  FROM " + RDBMSConstants.QUEUE_COUNTER_TABLE + " WHERE " +
                RDBMSConstants.QUEUE_NAME + "=?";
        PreparedStatement preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, queueName);
        ResultSet resultSet = preparedStatement.executeQuery();

        Assert.assertTrue("Entry should exist on DB for queue counter", resultSet.first());
        Assert.assertEquals(queueName, resultSet.getString(RDBMSConstants.QUEUE_NAME));
        Assert.assertEquals(count, resultSet.getInt(RDBMSConstants.MESSAGE_COUNT));
        Assert.assertFalse("Only one entry should exist on table", resultSet.next());
    }

    /**
     * Test message count for queue using the queue counter
     *
     * @throws Exception
     */
    @Test
    public void testGetMessageCountForQueue() throws Exception {

        String queueName = "queue1";
        int count = 20;

        // add counter for queue and update the data base for test
        String insert = "INSERT INTO " + RDBMSConstants.QUEUE_COUNTER_TABLE + " (" +
                RDBMSConstants.QUEUE_NAME + "," +
                RDBMSConstants.MESSAGE_COUNT + ") " +
                " VALUES ( ?,?)";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, queueName);
        preparedStatement.setLong(2, count);
        preparedStatement.executeUpdate();

        // test for correct queue count
        Assert.assertEquals(count, contextStore.getMessageCountForQueue(queueName));
    }

    /**
     * Test remove the counter for the given queue. Add multiple queue counters to table and
     * test for deletion of the given queue
     *
     * @throws Exception
     */
    @Test
    public void testRemoveMessageCounterForQueue() throws Exception {

        String queueName1 = "queue1";
        String queueName2 = "queue2";
        int count = 20;

        // add counter for queue and update the data base for test
        String insert = "INSERT INTO " + RDBMSConstants.QUEUE_COUNTER_TABLE + " (" +
                RDBMSConstants.QUEUE_NAME + "," +
                RDBMSConstants.MESSAGE_COUNT + ") " +
                " VALUES ( ?,?)";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, queueName1);
        preparedStatement.setLong(2, count);
        preparedStatement.addBatch();

        preparedStatement.setString(1, queueName2);
        preparedStatement.setLong(2, count);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();


        // delete queueName1
        contextStore.removeMessageCounterForQueue(queueName1);

        // test for database state
        String select = "SELECT *  FROM " + RDBMSConstants.QUEUE_COUNTER_TABLE + " WHERE " +
                RDBMSConstants.QUEUE_NAME + "=?";
        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, queueName1); // try to retrieve queueName1
        ResultSet resultSet = preparedStatement.executeQuery();

        // test whether deleted queueName1 still exist in DB
        Assert.assertFalse("Error: Entry available for already deleted counter", resultSet.first());

        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, queueName2); // try to retrieve queueName2
        resultSet = preparedStatement.executeQuery();

        // test for queueName2 existence
        Assert.assertTrue("Entry should exist in DB for not deleted counter", resultSet.first());
        Assert.assertEquals(queueName2, resultSet.getString(RDBMSConstants.QUEUE_NAME));
        Assert.assertEquals(count, resultSet.getLong(RDBMSConstants.MESSAGE_COUNT));

    }

    /**
     * Test increment message queue count through context store method and then test the DB state
     */
    @Test
    public void testIncrementMessageCountForQueue() throws Exception {
        String queueName = "queue1";
        long startValue = 0;
        long incrementBy = 20;

        // add counter for queue and update the data base for test
        String insert = "INSERT INTO " + RDBMSConstants.QUEUE_COUNTER_TABLE + " (" +
                RDBMSConstants.QUEUE_NAME + "," +
                RDBMSConstants.MESSAGE_COUNT + ") " +
                " VALUES ( ?,?)";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, queueName);
        preparedStatement.setLong(2, startValue);
        preparedStatement.executeUpdate();

        // increment message count
        contextStore.incrementMessageCountForQueue(queueName, incrementBy);

        // test for database state
        String select = "SELECT *  FROM " + RDBMSConstants.QUEUE_COUNTER_TABLE + " WHERE " +
                RDBMSConstants.QUEUE_NAME + "=?";
        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, queueName); // try to retrieve queueName1
        ResultSet resultSet = preparedStatement.executeQuery();

        resultSet.first();
        Assert.assertEquals(startValue + incrementBy, resultSet.getLong(RDBMSConstants.MESSAGE_COUNT));
    }

    /**
     * Test increment message queue count through context store method and then test the DB state
     */
    @Test
    public void testDecrementMessageCountForQueue() throws Exception {
        String queueName = "queue1";
        long startValue = 50;
        long decrementBy = 20;

        // add counter for queue and update the data base for test
        String insert = "INSERT INTO " + RDBMSConstants.QUEUE_COUNTER_TABLE + " (" +
                RDBMSConstants.QUEUE_NAME + "," +
                RDBMSConstants.MESSAGE_COUNT + ") " +
                " VALUES ( ?,?)";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, queueName);
        preparedStatement.setLong(2, startValue);
        preparedStatement.executeUpdate();

        // increment message count
        contextStore.decrementMessageCountForQueue(queueName, decrementBy);

        // test for database state
        String select = "SELECT *  FROM " + RDBMSConstants.QUEUE_COUNTER_TABLE + " WHERE " +
                RDBMSConstants.QUEUE_NAME + "=?";
        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, queueName); // try to retrieve queueName1
        ResultSet resultSet = preparedStatement.executeQuery();

        resultSet.first();
        Assert.assertEquals(startValue - decrementBy, resultSet.getLong(RDBMSConstants.MESSAGE_COUNT));
    }

    /**
     * Test functionality of getAllStoredDurableSubscriptions() method by populating the db and
     * then retrieving the durableSubscriptions using the method
     * @throws Exception
     */
    @Test
    public void testGetAllDurableSubscriptions() throws Exception {
        String insert = "INSERT INTO " + RDBMSConstants.DURABLE_SUB_TABLE + " (" +
                RDBMSConstants.DESTINATION_IDENTIFIER + ", " +
                RDBMSConstants.DURABLE_SUB_ID + ", " +
                RDBMSConstants.DURABLE_SUB_DATA + ") " +
                " VALUES ( ?,?,? )";

        // populate data
        String destinationIdOne = "destination1";
        String subscriptionIdOne = "sub-0";
        String dataOne = "data1";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, destinationIdOne);
        preparedStatement.setString(2, subscriptionIdOne);
        preparedStatement.setString(3, dataOne);
        preparedStatement.addBatch();

        String destinationIdTwo = "destination2";
        String subscriptionIdTwo = "sub-1";
        String dataTwo = "data2";

        preparedStatement.setString(1, destinationIdTwo);
        preparedStatement.setString(2, subscriptionIdTwo);
        preparedStatement.setString(3, dataTwo);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();

        // retrieve data
        Map<String, List<String>> subscriberMap = contextStore.getAllStoredDurableSubscriptions();

        // test
        int destinationCount = 2;
        Assert.assertEquals(destinationCount, subscriberMap.size());

        List<String> subscriberList = subscriberMap.get(destinationIdOne);
        Assert.assertEquals(dataOne, subscriberList.get(0));

        subscriberList = subscriberMap.get(destinationIdTwo);
        Assert.assertEquals(dataTwo, subscriberList.get(0));
    }

    /**
     * Test functionality of getAllStoredDurableSubscriptions() with populating the database with
     * multiple durable subscriptions for the same destination.
     * Since map returned use the destination as the key this tests whether multiple values for the
     * same destination is returned properly.
     * @throws Exception
     */
    @Test
    public void testGetAllSubscribersWithSameDestinationForALL() throws Exception {
        String insert = "INSERT INTO " + RDBMSConstants.DURABLE_SUB_TABLE + " (" +
                RDBMSConstants.DESTINATION_IDENTIFIER + ", " +
                RDBMSConstants.DURABLE_SUB_ID + ", " +
                RDBMSConstants.DURABLE_SUB_DATA + ") " +
                " VALUES ( ?,?,? )";

        // populate data
        String destinationIdOne = "destination1";
        String subscriptionIdOne = "sub-0";
        String dataOne = "data1";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, destinationIdOne);
        preparedStatement.setString(2, subscriptionIdOne);
        preparedStatement.setString(3, dataOne);
        preparedStatement.addBatch();

        String subscriptionIdTwo = "sub-1";
        String dataTwo = "data2";

        preparedStatement.setString(1, destinationIdOne);
        preparedStatement.setString(2, subscriptionIdTwo);
        preparedStatement.setString(3, dataTwo);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();

        // retrieve data
        Map<String, List<String>> subscriberMap = contextStore.getAllStoredDurableSubscriptions();

        // test
        int destinationCount = 1;
        Assert.assertEquals(destinationCount, subscriberMap.size());

        List<String> subscriberList = subscriberMap.get(destinationIdOne);
        Assert.assertEquals(dataOne, subscriberList.get(0));
        Assert.assertEquals(dataTwo, subscriberList.get(1));
    }

    /**
     * Test whether already existing durable subscriptions removed from database properly
     * @throws Exception
     */
    @Test
    public void testRemoveDurableSubscription() throws Exception {
        String insert = "INSERT INTO " + RDBMSConstants.DURABLE_SUB_TABLE + " (" +
                RDBMSConstants.DESTINATION_IDENTIFIER + ", " +
                RDBMSConstants.DURABLE_SUB_ID + ", " +
                RDBMSConstants.DURABLE_SUB_DATA + ") " +
                " VALUES ( ?,?,? )";

        // populate data
        String destinationIdOne = "destination1";
        String subscriptionIdOne = "sub-0";
        String dataOne = "data1";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, destinationIdOne);
        preparedStatement.setString(2, subscriptionIdOne);
        preparedStatement.setString(3, dataOne);
        preparedStatement.executeUpdate();
        preparedStatement.close();

        // delete
        contextStore.removeDurableSubscription(destinationIdOne, subscriptionIdOne);

        String select = "SELECT * FROM " + RDBMSConstants.DURABLE_SUB_TABLE +
                " WHERE " + RDBMSConstants.DESTINATION_IDENTIFIER + "=? " +
                " AND " + RDBMSConstants.DURABLE_SUB_ID + "=?";

        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, destinationIdOne);
        preparedStatement.setString(2, subscriptionIdOne);
        ResultSet resultSet = preparedStatement.executeQuery();

        // there should be no entries in result set.
        Assert.assertEquals(false, resultSet.first());
    }

    /**
     * Test a node detail is stored properly to disk
     * @throws Exception
     */
    @Test
    public void testStoreNodeDetails() throws Exception {
        String nodeId = "nodeId";
        String data = "node data";

        // store data
        contextStore.storeNodeDetails(nodeId, data);

        // retrieve to test
        String select = "SELECT * FROM " + RDBMSConstants.NODE_INFO_TABLE +
                " WHERE " + RDBMSConstants.NODE_ID + "=?";

        PreparedStatement preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, nodeId);
        ResultSet resultSet = preparedStatement.executeQuery();

        // test
        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(nodeId, resultSet.getString(RDBMSConstants.NODE_ID));
        Assert.assertEquals(data, resultSet.getString(RDBMSConstants.NODE_INFO));
    }

    /**
     * Test all the nodes details present in database are retrieved properly through
     * getAllStoredNodeData() method
     * @throws Exception
     */
    @Test
    public void testGetAllNodeDetails() throws Exception {
        String insert = "INSERT INTO " + RDBMSConstants.NODE_INFO_TABLE + "( " +
                RDBMSConstants.NODE_ID + "," +
                RDBMSConstants.NODE_INFO + ") " +
                " VALUES (?,?) ";

        int nodeCount = 2;
        String nodeIdOne = "node1";
        String nodeDataOne = "data1";
        String nodeIdTwo = "node2";
        String nodeDataTwo = "data2";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        // add first data set
        preparedStatement.setString(1, nodeIdOne);
        preparedStatement.setString(2, nodeDataOne);
        preparedStatement.addBatch();

        // add second data set
        preparedStatement.setString(1, nodeIdTwo);
        preparedStatement.setString(2, nodeDataTwo);
        preparedStatement.addBatch();

        preparedStatement.executeBatch();
        Map<String, String> nodeDataMap = contextStore.getAllStoredNodeData();

        Assert.assertEquals(nodeCount, nodeDataMap.size());
        Assert.assertEquals(nodeDataOne, nodeDataMap.get(nodeIdOne));
        Assert.assertEquals(nodeDataTwo, nodeDataMap.get(nodeIdTwo));
    }

    /**
     * Test node data are removed properly from database on method call removeNodeData()
     * @throws Exception
     */
    @Test
    public void testRemoveNodeData() throws Exception {
        String insert = "INSERT INTO " + RDBMSConstants.NODE_INFO_TABLE + "( " +
                RDBMSConstants.NODE_ID + "," +
                RDBMSConstants.NODE_INFO + ") " +
                " VALUES (?,?) ";

        String nodeIdOne = "node1";
        String nodeDataOne = "data1";
        String nodeIdTwo = "node2";
        String nodeDataTwo = "data2";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        // add first data set
        preparedStatement.setString(1, nodeIdOne);
        preparedStatement.setString(2, nodeDataOne);
        preparedStatement.addBatch();

        // add second data set
        preparedStatement.setString(1, nodeIdTwo);
        preparedStatement.setString(2, nodeDataTwo);
        preparedStatement.addBatch();

        preparedStatement.executeBatch();

        // remove node data
        contextStore.removeNodeData(nodeIdOne);

        // query DB and try to retrieve deleted node information
        String select = "SELECT * FROM " + RDBMSConstants.NODE_INFO_TABLE +
                " WHERE " + RDBMSConstants.NODE_ID + "=?";

        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, nodeIdOne);
        ResultSet resultSet = preparedStatement.executeQuery();

        // result set should be empty.
        Assert.assertEquals(false, resultSet.first());

    }

    /**
     * Test exchanges are stored properly to database by method call storeExchangeInformation(...)
     * @throws Exception
     */
    @Test
    public void testStoreExchange() throws Exception {
        String exchange1 = "exchange1";
        String exchange2 = "exchange2";
        String exchangeInfo1 = "exchangeInfo1";
        String exchangeInfo2 = "exchangeInfo2";

        contextStore.storeExchangeInformation(exchange1, exchangeInfo1);
        contextStore.storeExchangeInformation(exchange2, exchangeInfo2);

        // query from db
        String select = "SELECT * FROM " + RDBMSConstants.EXCHANGES_TABLE +
                " WHERE " + RDBMSConstants.EXCHANGE_NAME + "=?";

        PreparedStatement preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, exchange1);
        ResultSet resultSet = preparedStatement.executeQuery();

        // test for exchange 1 data
        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(exchangeInfo1, resultSet.getString(RDBMSConstants.EXCHANGE_DATA));
        preparedStatement.close();

        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, exchange2);
        resultSet = preparedStatement.executeQuery();

        // test for exchange 2 data
        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(exchangeInfo2, resultSet.getString(RDBMSConstants.EXCHANGE_DATA));
    }

    /**
     * Test all the exchanges stored in database are returned properly by method
     * getAllExchangesStored(...)
     * @throws Exception
     */
    @Test
    public void testGetAllExchangesStored() throws Exception {
        String exchange1 = "exchange1";
        String exchange2 = "exchange2";
        String exchangeInfo1 = "exchangeName=" + exchange1 + ",type=none," +
                "autoDelete=false";
        String exchangeInfo2 = "exchangeName=" + exchange2 + ",type=none," +
                "autoDelete=false";
        int exchangeCount = 2;

        // setup database with exchange information
        String insert = "INSERT INTO " + RDBMSConstants.EXCHANGES_TABLE + " ( " +
                RDBMSConstants.EXCHANGE_NAME + "," +
                RDBMSConstants.EXCHANGE_DATA + ") " +
                " VALUES (?, ?)";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, exchange1);
        preparedStatement.setString(2, exchangeInfo1);
        preparedStatement.addBatch();

        preparedStatement.setString(1, exchange2);
        preparedStatement.setString(2, exchangeInfo2);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();

        List<AndesExchange> exchangeList = contextStore.getAllExchangesStored();

        Assert.assertEquals(exchangeCount, exchangeList.size());
    }

    /**
     * Test exchanges are removed properly from database by method
     * deleteExchangeInformation(...)
     * @throws Exception
     */
    @Test
    public void testDeleteExchange() throws Exception {
        String exchange1 = "exchange1";
        String exchange2 = "exchange2";
        String exchangeInfo1 = "exchangeName=" + exchange1 + ",type=none," +
                "autoDelete=false";
        String exchangeInfo2 = "exchangeName=" + exchange2 + ",type=none," +
                "autoDelete=false";

        // setup database with exchange information
        String insert = "INSERT INTO " + RDBMSConstants.EXCHANGES_TABLE + " ( " +
                RDBMSConstants.EXCHANGE_NAME + "," +
                RDBMSConstants.EXCHANGE_DATA + ") " +
                " VALUES (?, ?)";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, exchange1);
        preparedStatement.setString(2, exchangeInfo1);
        preparedStatement.addBatch();

        preparedStatement.setString(1, exchange2);
        preparedStatement.setString(2, exchangeInfo2);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();
        preparedStatement.close();

        // delete one exchange
        contextStore.deleteExchangeInformation(exchange1);

        // test for database state
        String select = "SELECT *  FROM " + RDBMSConstants.EXCHANGES_TABLE;

        preparedStatement = connection.prepareStatement(select);
        ResultSet resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.first()); //  one entry should be there
        Assert.assertEquals(exchange2, resultSet.getString(RDBMSConstants.EXCHANGE_NAME));
        Assert.assertEquals(false, resultSet.next());
    }

    /**
     * Test queue information are stored properly to database though method
     * storeQueueInformation(...)
     * @throws Exception
     */
    @Test
    public void testStoreQueueInformation() throws Exception {

        String queueName1 = "queue1";
        String queueInfo1 = "queueInfo1";
        String queueName2 = "queue2";
        String queueInfo2 = "queueInfo2";
        // store queue info
        contextStore.storeQueueInformation(queueName1, queueInfo1);
        contextStore.storeQueueInformation(queueName2, queueInfo2);

        // retrieve directly from db and test
        String select = "SELECT * FROM " + RDBMSConstants.QUEUE_INFO_TABLE +
                " WHERE " + RDBMSConstants.QUEUE_NAME + "=?";

        PreparedStatement preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, queueName1);
        ResultSet resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.next());
        Assert.assertEquals(queueInfo1, resultSet.getString(RDBMSConstants.QUEUE_DATA));

        preparedStatement.close();
        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, queueName2);
        resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.next());
        Assert.assertEquals(queueInfo2, resultSet.getString(RDBMSConstants.QUEUE_DATA));
    }

    /**
     * Test queue information stored in database are retrieved properly through method
     * getAllQueuesStored(...)
     * @throws Exception
     */
    @Test
    public void testGetAllQueuesStored() throws Exception {

        AndesQueue andesQueue1 = new AndesQueue("queue1", "owner1", true, false, false);
        AndesQueue andesQueue2 = new AndesQueue("queue2", "owner2", true, false, false);
        int queueCount = 2;

        // insert
        String insert = "INSERT INTO " + RDBMSConstants.QUEUE_INFO_TABLE + " (" +
                RDBMSConstants.QUEUE_NAME + "," + RDBMSConstants.QUEUE_DATA + " ) " +
                " VALUES (?,?)";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, andesQueue1.queueName);
        preparedStatement.setString(2, andesQueue1.encodeAsString());
        preparedStatement.executeUpdate();

        // retrieve data
        List<AndesQueue> queueList = contextStore.getAllQueuesStored();

        //test
        AndesQueue returnedAndesQueue = queueList.get(0);
        Assert.assertEquals(andesQueue1.queueName, returnedAndesQueue.queueName);
        Assert.assertEquals(andesQueue1.queueOwner, returnedAndesQueue.queueOwner);
        Assert.assertEquals(andesQueue1.isExclusive, returnedAndesQueue.isExclusive);
        Assert.assertEquals(andesQueue1.isDurable, returnedAndesQueue.isDurable);

        // add another entry
        preparedStatement.close();
        preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, andesQueue2.queueName);
        preparedStatement.setString(2, andesQueue2.encodeAsString());
        preparedStatement.executeUpdate();

        // retrieve data
        queueList = contextStore.getAllQueuesStored();

        // test
        Assert.assertEquals(queueCount, queueList.size());
    }

    /**
     * Test queue information in database are deleted properly through method
     * deleteQueueInformation(...)
     * @throws Exception
     */
    @Test
    public void testDeleteQueueInformation() throws Exception {
        AndesQueue andesQueue1 = new AndesQueue("queue1", "owner1", true, false, false);
        AndesQueue andesQueue2 = new AndesQueue("queue2", "owner2", true, false, false);

        // insert
        String insert = "INSERT INTO " + RDBMSConstants.QUEUE_INFO_TABLE + " (" +
                RDBMSConstants.QUEUE_NAME + "," + RDBMSConstants.QUEUE_DATA + " ) " +
                " VALUES (?,?)";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, andesQueue1.queueName);
        preparedStatement.setString(2, andesQueue1.encodeAsString());
        preparedStatement.addBatch();

        preparedStatement.setString(1, andesQueue2.queueName);
        preparedStatement.setString(2, andesQueue2.encodeAsString());
        preparedStatement.addBatch();
        preparedStatement.executeBatch();

        // delete a queue
        contextStore.deleteQueueInformation(andesQueue1.queueName);

        // retrieve directly from db and test
        String select = "SELECT * FROM " + RDBMSConstants.QUEUE_INFO_TABLE;

        preparedStatement.close();
        preparedStatement = connection.prepareStatement(select);
        ResultSet resultSet = preparedStatement.executeQuery();

        // queue2 entry should remain in db
        Assert.assertEquals(true, resultSet.next());
        Assert.assertEquals(andesQueue2.queueName, resultSet.getString(RDBMSConstants.QUEUE_NAME));
        Assert.assertEquals(andesQueue2.encodeAsString(), resultSet.getString(RDBMSConstants.QUEUE_DATA));

        // there should be only one entry remaining in db
        Assert.assertEquals(false, resultSet.next());

    }

    /**
     * Test binding are stored to database properly with method storeBindingInformation(...)
     * @throws Exception
     */
    @Test
    public void testStoreBinding() throws Exception {
        String exchange1 = "exchange1";
        String boundQueue1 = "boundQueue1";
        String routingKey1 = "routingKey1";

        // store queue and exchange information accordingly in db before storing binging.
        AndesQueue andesQueue1 = new AndesQueue(boundQueue1, "owner1", true, false, false);

        // setup database with queue information
        String insert = "INSERT INTO " + RDBMSConstants.QUEUE_INFO_TABLE + " (" +
                RDBMSConstants.QUEUE_NAME + "," + RDBMSConstants.QUEUE_DATA + " ) " +
                " VALUES (?,?)";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, andesQueue1.queueName);
        preparedStatement.setString(2, andesQueue1.encodeAsString());
        preparedStatement.executeUpdate();
        preparedStatement.close();

        // setup database with exchange information
        String exchangeInfo1 = "exchangeName=" + exchange1 + ",type=none," +
                "autoDelete=false";

        insert = "INSERT INTO " + RDBMSConstants.EXCHANGES_TABLE + " ( " +
                RDBMSConstants.EXCHANGE_NAME + "," +
                RDBMSConstants.EXCHANGE_DATA + ") " +
                " VALUES (?, ?)";

        preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, exchange1);
        preparedStatement.setString(2, exchangeInfo1);
        preparedStatement.executeUpdate();
        preparedStatement.close();

        // add binding to db
        contextStore.storeBindingInformation(exchange1, boundQueue1, routingKey1);

        // check if stored binding is in db
        String select = "SELECT *  FROM " + RDBMSConstants.BINDINGS_TABLE +
                " WHERE " + RDBMSConstants.BINDING_QUEUE_NAME + "=? AND " +
                RDBMSConstants.BINDING_EXCHANGE_NAME + "=?";

        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, boundQueue1);
        preparedStatement.setString(2, exchange1);
        ResultSet resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.next());
        Assert.assertEquals(routingKey1, resultSet.getString(RDBMSConstants.BINDING_INFO));

    }

    /**
     * Test all the binding information stored in database for a given exchange is retrieved from
     * method getBindingsStoredForExchange(...)
     * @throws Exception
     */
    @Test
    public void testGetBindingStoredForExchange() throws Exception {
        String exchange1 = "exchange1";
        String boundQueue1 = "boundQueue1";
        String routingKey1 = "routingKey1";
        String qOwner1 = "queueOwner1";
        String exchange2 = "exchange2";
        String boundQueue2 = "boundQueue2";
        String routingKey2 = "routingKey2";
        String qOwner2 = "queueOwner2";

        // setup binding information in db
        RDBMSTestHelper.storeBindingInfo(connection, exchange1, boundQueue1, routingKey1, qOwner1);
        RDBMSTestHelper.storeBindingInfo(connection, exchange2, boundQueue2, routingKey2, qOwner2);

        // get binding data through context store method
        List<AndesBinding> bindingList = contextStore.getBindingsStoredForExchange(exchange1);

        // test
        int resultCount = 1; // only one entry for exchange1 should be returned

        AndesBinding andesBinding = bindingList.get(0);
        Assert.assertEquals(resultCount, bindingList.size());
        Assert.assertEquals(exchange1, andesBinding.boundExchangeName);
        Assert.assertEquals(boundQueue1, andesBinding.boundQueue.queueName);
        Assert.assertEquals(routingKey1, andesBinding.routingKey);

    }

    /**
     * Test binding information stored in database for a particular queue in particular exchange can
     * be deleted properly through method deleteBindingInformation(...)
     * @throws Exception
     */
    @Test
    public void testDeleteBindingInformation() throws Exception {
        String exchange1 = "exchange1";
        String boundQueue1 = "boundQueue1";
        String routingKey1 = "routingKey1";
        String qOwner1 = "queueOwner1";
        String exchange2 = "exchange2";
        String boundQueue2 = "boundQueue2";
        String routingKey2 = "routingKey2";
        String qOwner2 = "queueOwner2";

        // setup binding information in db
        RDBMSTestHelper.storeBindingInfo(connection, exchange1, boundQueue1, routingKey1, qOwner1);
        RDBMSTestHelper.storeBindingInfo(connection, exchange2, boundQueue2, routingKey2, qOwner2);

        // delete entry
        contextStore.deleteBindingInformation(exchange1, boundQueue1);

        // test
        String select = "SELECT * FROM " + RDBMSConstants.BINDINGS_TABLE;
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(select);

        resultSet.next();
        Assert.assertEquals(exchange2, resultSet.getString(RDBMSConstants.BINDING_EXCHANGE_NAME));
        Assert.assertEquals(boundQueue2, resultSet.getString(RDBMSConstants.BINDING_QUEUE_NAME));
        Assert.assertEquals(false, resultSet.next());

    }

    /**
     * Create tables used by JDBCAndesContextStoreImpl
     * @throws Exception
     */
    private void createTables() throws Exception {

        String[] queries = {
                "CREATE TABLE IF NOT EXISTS durable_subscriptions (" +
                        "sub_id VARCHAR NOT NULL," +
                        "destination_identifier VARCHAR NOT NULL," +
                        "data VARCHAR NOT NULL" +
                        ");",

                "CREATE TABLE IF NOT EXISTS node_info (" +
                        "node_id VARCHAR NOT NULL," +
                        "data VARCHAR NOT NULL," +
                        "PRIMARY KEY(node_id)" +
                        ");",

                "CREATE TABLE IF NOT EXISTS exchanges (" +
                        "name VARCHAR NOT NULL," +
                        "data VARCHAR NOT NULL," +
                        "PRIMARY KEY(name)" +
                        ");",

                "CREATE TABLE IF NOT EXISTS queue_info (" +
                        "name VARCHAR NOT NULL," +
                        "data VARCHAR NOT NULL," +
                        "PRIMARY KEY(name)" +
                        ");",

                "CREATE TABLE IF NOT EXISTS bindings (" +
                        "exchange_name VARCHAR NOT NULL," +
                        "queue_name VARCHAR NOT NULL," +
                        "binding_info VARCHAR NOT NULL," +
                        "FOREIGN KEY (exchange_name) REFERENCES exchanges (name)," +
                        "FOREIGN KEY (queue_name) REFERENCES queue_info (name)" +
                        ");",

                "CREATE TABLE IF NOT EXISTS " + RDBMSConstants.QUEUE_COUNTER_TABLE + " (" +
                        RDBMSConstants.QUEUE_NAME + " VARCHAR NOT NULL," +
                        RDBMSConstants.MESSAGE_COUNT + " BIGINT, " +
                        " PRIMARY KEY (" + RDBMSConstants.QUEUE_NAME + ") " +
                        ");"

        };
        Statement stmt = connection.createStatement();
        for (String q : queries) {
            stmt.addBatch(q);
        }
        stmt.executeBatch();
        stmt.close();
    }

    /**
     * Drop all the tables used by JDBCAndesContextStoreImpl
     * @throws Exception
     */
    private void dropTables() throws Exception {
        String[] queries = {
                "DROP TABLE durable_subscriptions;",
                "DROP TABLE node_info;",
                "DROP TABLE exchanges",
                "DROP TABLE queue_info",
                "DROP TABLE bindings",
                "DROP TABLE " + RDBMSConstants.QUEUE_COUNTER_TABLE
        };
        Statement stmt = connection.createStatement();
        for (String q : queries) {
            stmt.addBatch(q);
        }
        stmt.executeBatch();
        stmt.close();
    }
}
