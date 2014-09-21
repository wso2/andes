package org.wso2.andes.store.jdbc;

import junit.framework.Assert;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.*;
import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesExchange;
import org.wso2.andes.kernel.AndesQueue;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameAlreadyBoundException;
import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Unit test class for H2BasedAndesContextStoreImpl
 * Basic implementation of methods are tested in this class
 */
public class H2BasedAndesContextStoreImplTest {

    private static Connection connection;
    private H2BasedAndesContextStoreImpl contextStore;

    @BeforeClass
    public static void BeforeClass() throws Exception {
        try {
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
            ic.bind(JDBCConstants.H2_MEM_JNDI_LOOKUP_NAME, ds);

            Class.forName("org.h2.Driver");
            connection = DriverManager.getConnection(jdbcUrl);
        } catch (NameAlreadyBoundException ignored) {
        }
    }

    @Before
    public void setup() throws Exception {
        createTables();
        contextStore = new H2BasedAndesContextStoreImpl(true); // in memory mode mode
        contextStore.init();
    }

    @After
    public void tearDown() throws Exception {
        dropTables();
    }

    @AfterClass
    public static void afterClass() {

    }

    @Test
    public void testStoreDurableSubscription() throws Exception {
        String destIdentifier = "destination";
        String subId = "sub-0";
        String subEncodedAsStr = "data";
        contextStore.storeDurableSubscription(destIdentifier, subId, subEncodedAsStr);

        String select = "SELECT * FROM " + JDBCConstants.DURABLE_SUB_TABLE;
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(select);

        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(subId, resultSet.getString(JDBCConstants.DURABLE_SUB_ID));
        Assert.assertEquals(destIdentifier, resultSet.getString(JDBCConstants.DESTINATION_IDENTIFIER));
        Assert.assertEquals(subEncodedAsStr, resultSet.getString(JDBCConstants.DURABLE_SUB_DATA));

    }

    @Test
    public void testGetAllDurableSubscriptions() throws Exception {
        String insert = "INSERT INTO " + JDBCConstants.DURABLE_SUB_TABLE + " (" +
                JDBCConstants.DESTINATION_IDENTIFIER + ", " +
                JDBCConstants.DURABLE_SUB_ID + ", " +
                JDBCConstants.DURABLE_SUB_DATA + ") " +
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

    @Test
    public void testGetAllSubscribersWithSameDestinationForALL() throws Exception {
        String insert = "INSERT INTO " + JDBCConstants.DURABLE_SUB_TABLE + " (" +
                JDBCConstants.DESTINATION_IDENTIFIER + ", " +
                JDBCConstants.DURABLE_SUB_ID + ", " +
                JDBCConstants.DURABLE_SUB_DATA + ") " +
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

    @Test
    public void testRemoveDurableSubscription() throws Exception {
        String insert = "INSERT INTO " + JDBCConstants.DURABLE_SUB_TABLE + " (" +
                JDBCConstants.DESTINATION_IDENTIFIER + ", " +
                JDBCConstants.DURABLE_SUB_ID + ", " +
                JDBCConstants.DURABLE_SUB_DATA + ") " +
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

        String select = "SELECT * FROM " + JDBCConstants.DURABLE_SUB_TABLE +
                " WHERE " + JDBCConstants.DESTINATION_IDENTIFIER + "=? " +
                " AND " + JDBCConstants.DURABLE_SUB_ID + "=?";

        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, destinationIdOne);
        preparedStatement.setString(2, subscriptionIdOne);
        ResultSet resultSet = preparedStatement.executeQuery();

        // there should be no entries in result set.
        Assert.assertEquals(false, resultSet.first());
    }

    @Test
    public void testStoreNodeDetails() throws Exception {
        String nodeId = "nodeId";
        String data = "node data";

        // store data
        contextStore.storeNodeDetails(nodeId, data);

        // retrieve to test
        String select = "SELECT * FROM " + JDBCConstants.NODE_INFO_TABLE +
                " WHERE " + JDBCConstants.NODE_ID + "=?";

        PreparedStatement preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, nodeId);
        ResultSet resultSet = preparedStatement.executeQuery();

        // test
        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(nodeId, resultSet.getString(JDBCConstants.NODE_ID));
        Assert.assertEquals(data, resultSet.getString(JDBCConstants.NODE_INFO));
    }

    @Test
    public void testGetAllNodeDetails() throws Exception {
        String insert = "INSERT INTO " + JDBCConstants.NODE_INFO_TABLE + "( " +
                JDBCConstants.NODE_ID + "," +
                JDBCConstants.NODE_INFO + ") " +
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

    @Test
    public void testRemoveNodeData() throws Exception {
        String insert = "INSERT INTO " + JDBCConstants.NODE_INFO_TABLE + "( " +
                JDBCConstants.NODE_ID + "," +
                JDBCConstants.NODE_INFO + ") " +
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
        String select = "SELECT * FROM " + JDBCConstants.NODE_INFO_TABLE +
                " WHERE " + JDBCConstants.NODE_ID + "=?";

        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, nodeIdOne);
        ResultSet resultSet = preparedStatement.executeQuery();

        // result set should be empty.
        Assert.assertEquals(false, resultSet.first());

    }

    @Test
    public void testStoreExchange() throws Exception {
        String exchange1 = "exchange1";
        String exchange2 = "exchange2";
        String exchangeInfo1 = "exchangeInfo1";
        String exchangeInfo2 = "exchangeInfo2";

        contextStore.storeExchangeInformation(exchange1, exchangeInfo1);
        contextStore.storeExchangeInformation(exchange2, exchangeInfo2);

        // query from db
        String select = "SELECT * FROM " + JDBCConstants.EXCHANGES_TABLE +
                " WHERE " + JDBCConstants.EXCHANGE_NAME + "=?";

        PreparedStatement preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, exchange1);
        ResultSet resultSet = preparedStatement.executeQuery();

        // test for exchange 1 data
        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(exchangeInfo1, resultSet.getString(JDBCConstants.EXCHANGE_DATA));
        preparedStatement.close();

        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, exchange2);
        resultSet = preparedStatement.executeQuery();

        // test for exchange 2 data
        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(exchangeInfo2, resultSet.getString(JDBCConstants.EXCHANGE_DATA));
    }

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
        String insert = "INSERT INTO " + JDBCConstants.EXCHANGES_TABLE + " ( " +
                JDBCConstants.EXCHANGE_NAME + "," +
                JDBCConstants.EXCHANGE_DATA + ") " +
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

    @Test
    public void testDeleteExchange() throws Exception {
        String exchange1 = "exchange1";
        String exchange2 = "exchange2";
        String exchangeInfo1 = "exchangeName=" + exchange1 + ",type=none," +
                "autoDelete=false";
        String exchangeInfo2 = "exchangeName=" + exchange2 + ",type=none," +
                "autoDelete=false";

        // setup database with exchange information
        String insert = "INSERT INTO " + JDBCConstants.EXCHANGES_TABLE + " ( " +
                JDBCConstants.EXCHANGE_NAME + "," +
                JDBCConstants.EXCHANGE_DATA + ") " +
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
        String select = "SELECT *  FROM " + JDBCConstants.EXCHANGES_TABLE;

        preparedStatement = connection.prepareStatement(select);
        ResultSet resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.first()); //  one entry should be there
        Assert.assertEquals(exchange2, resultSet.getString(JDBCConstants.EXCHANGE_NAME));
        Assert.assertEquals(false, resultSet.next());
    }

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
        String select = "SELECT * FROM " + JDBCConstants.QUEUE_INFO_TABLE +
                " WHERE " + JDBCConstants.QUEUE_NAME + "=?";

        PreparedStatement preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, queueName1);
        ResultSet resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.next());
        Assert.assertEquals(queueInfo1, resultSet.getString(JDBCConstants.QUEUE_INFO));

        preparedStatement.close();
        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, queueName2);
        resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.next());
        Assert.assertEquals(queueInfo2, resultSet.getString(JDBCConstants.QUEUE_INFO));
    }

    @Test
    public void testAllQueuesStored() throws Exception {

        AndesQueue andesQueue1 = new AndesQueue("queue1", "owner1", true, false);
        AndesQueue andesQueue2 = new AndesQueue("queue2", "owner2", true, false);
        int queueCount = 2;

        // insert
        String insert = "INSERT INTO " + JDBCConstants.QUEUE_INFO_TABLE + " (" +
                JDBCConstants.QUEUE_NAME + "," + JDBCConstants.QUEUE_INFO + " ) " +
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

    @Test
    public void testDeleteQueueInformation() throws Exception {
        AndesQueue andesQueue1 = new AndesQueue("queue1", "owner1", true, false);
        AndesQueue andesQueue2 = new AndesQueue("queue2", "owner2", true, false);

        // insert
        String insert = "INSERT INTO " + JDBCConstants.QUEUE_INFO_TABLE + " (" +
                JDBCConstants.QUEUE_NAME + "," + JDBCConstants.QUEUE_INFO + " ) " +
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
        String select = "SELECT * FROM " + JDBCConstants.QUEUE_INFO_TABLE;

        preparedStatement.close();
        preparedStatement = connection.prepareStatement(select);
        ResultSet resultSet = preparedStatement.executeQuery();

        // queue2 entry should remain in db
        Assert.assertEquals(true, resultSet.next());
        Assert.assertEquals(andesQueue2.queueName, resultSet.getString(JDBCConstants.QUEUE_NAME));
        Assert.assertEquals(andesQueue2.encodeAsString(), resultSet.getString(JDBCConstants.QUEUE_INFO));

        // there should be only one entry remaining in db
        Assert.assertEquals(false, resultSet.next());

    }

    @Test
    public void testStoreBinding() throws Exception {
        String exchange1 = "exchange1";
        String boundQueue1 = "boundQueue1";
        String routingKey1 = "routingKey1";

        // store queue and exchange information accordingly in db before storing binging.
        AndesQueue andesQueue1 = new AndesQueue(boundQueue1, "owner1", true, false);

        // setup database with queue information
        String insert = "INSERT INTO " + JDBCConstants.QUEUE_INFO_TABLE + " (" +
                JDBCConstants.QUEUE_NAME + "," + JDBCConstants.QUEUE_INFO + " ) " +
                " VALUES (?,?)";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, andesQueue1.queueName);
        preparedStatement.setString(2, andesQueue1.encodeAsString());
        preparedStatement.executeUpdate();
        preparedStatement.close();

        // setup database with exchange information
        String exchangeInfo1 = "exchangeName=" + exchange1 + ",type=none," +
                "autoDelete=false";

        insert = "INSERT INTO " + JDBCConstants.EXCHANGES_TABLE + " ( " +
                JDBCConstants.EXCHANGE_NAME + "," +
                JDBCConstants.EXCHANGE_DATA + ") " +
                " VALUES (?, ?)";

        preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, exchange1);
        preparedStatement.setString(2, exchangeInfo1);
        preparedStatement.executeUpdate();
        preparedStatement.close();

        // add binding to db
        contextStore.storeBindingInformation(exchange1, boundQueue1, routingKey1);

        // check if stored binding is in db
        String select = "SELECT *  FROM " + JDBCConstants.BINDINGS_TABLE +
                " WHERE " + JDBCConstants.BINDING_QUEUE_NAME + "=? AND " +
                JDBCConstants.BINDING_EXCHANGE_NAME + "=?";

        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, boundQueue1);
        preparedStatement.setString(2, exchange1);
        ResultSet resultSet = preparedStatement.executeQuery();

        Assert.assertEquals(true, resultSet.next());
        Assert.assertEquals(routingKey1, resultSet.getString(JDBCConstants.ROUTING_KEY));

    }

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
        JDBCTestHelper.storeBindingInfo(connection, exchange1, boundQueue1, routingKey1, qOwner1);
        JDBCTestHelper.storeBindingInfo(connection, exchange2, boundQueue2, routingKey2, qOwner2);

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
        JDBCTestHelper.storeBindingInfo(connection, exchange1, boundQueue1, routingKey1, qOwner1);
        JDBCTestHelper.storeBindingInfo(connection, exchange2, boundQueue2, routingKey2, qOwner2);

        // delete entry
        contextStore.deleteBindingInformation(exchange1, boundQueue1);

        // test
        String select = "SELECT * FROM " + JDBCConstants.BINDINGS_TABLE;
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(select);

        resultSet.next();
        Assert.assertEquals(exchange2, resultSet.getString(JDBCConstants.BINDING_EXCHANGE_NAME));
        Assert.assertEquals(boundQueue2, resultSet.getString(JDBCConstants.BINDING_QUEUE_NAME));
        Assert.assertEquals(routingKey2, resultSet.getString(JDBCConstants.ROUTING_KEY));
        Assert.assertEquals(false, resultSet.next());

    }


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
                        "routing_key VARCHAR NOT NULL," +
                        "FOREIGN KEY (exchange_name) REFERENCES exchanges (name)," +
                        "FOREIGN KEY (queue_name) REFERENCES queue_info (name)" +
                        ");"
        };
        Statement stmt = connection.createStatement();
        for (String q : queries) {
            stmt.addBatch(q);
        }
        stmt.executeBatch();
        stmt.close();
    }

    private void dropTables() throws Exception {
        String[] queries = {
                "DROP TABLE durable_subscriptions;",
                "DROP TABLE node_info;",
                "DROP TABLE exchanges",
                "DROP TABLE queue_info",
                "DROP TABLE bindings"
        };
        Statement stmt = connection.createStatement();
        for (String q : queries) {
            stmt.addBatch(q);
        }
        stmt.executeBatch();
        stmt.close();
    }
}