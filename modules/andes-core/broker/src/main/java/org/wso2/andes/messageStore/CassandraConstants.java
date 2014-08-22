package org.wso2.andes.messageStore;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;

public class CassandraConstants {

	public static final String USERNAME_KEY = "username";
    public static final String PASSWORD_KEY = "password";
    public static final String CONNECTION_STRING = "connectionString";
    public static final String REPLICATION_FACTOR = "advanced.replicationFactor";
    public static final String READ_CONSISTENCY_LEVEL = "advanced.readConsistencyLevel";
    public static final String WRITE_CONSISTENCY_LEVEL = "advanced.writeConsistencyLevel";
    public static final String STRATERGY_CLASS = "advanced.strategyClass";
    public static final String GC_GRACE_SECONDS = "advanced.GCGraceSeconds";
    public static final String CLUSTER_KEY = "cluster";
    public static final String ID_GENENRATOR = "idGenerator";

    public final static String KEYSPACE = "QpidKeySpace";
    public final static String LONG_TYPE = "LongType";
    public final static String UTF8_TYPE = "UTF8Type";
    public final static String INTEGER_TYPE = "IntegerType";
    public final static String STRING_TYPE = "StringType";

    public static StringSerializer stringSerializer = StringSerializer.get();
    public static LongSerializer longSerializer = LongSerializer.get();
    public static BytesArraySerializer bytesArraySerializer = BytesArraySerializer.get();
    public static IntegerSerializer integerSerializer = IntegerSerializer.get();
    public static ByteBufferSerializer byteBufferSerializer = ByteBufferSerializer.get();

    //column family to keep track of loaded exchanges
    public final static String EXCHANGE_COLUMN_FAMILY = "ExchangeColumnFamily";
    public final static String EXCHANGE_ROW = "ExchangesRow";

    //column family to add and remove message content with their <messageID,offset> values
    public final static String MESSAGE_CONTENT_COLUMN_FAMILY = "MessageContent";

    //column family to keep messages for node queues (<nodequeue,messageID>)
    public final static String NODE_QUEUES_COLUMN_FAMILY = "NodeQueues";

    //column family to keep messages for global queues (<global-queue,messageID>)
    public final static String GLOBAL_QUEUES_COLUMN_FAMILY = "GlobalQueue";

    //column family to keep track of message IDs for topics <nodeQueueName,MessageID>
    public final static String PUB_SUB_MESSAGE_IDS_COLUMN_FAMILY = "pubSubMessages";
    
    public final static String SUBSCRIPTIONS_COLUMN_FAMILY = "Subscriptions";

    //column family to keep track of nodes and their syncing info (i.e bind IP Address) under NODE_DETAIL_ROW.
    public final static String NODE_DETAIL_COLUMN_FAMILY = "CusterNodeDetails";
    public final static String NODE_DETAIL_ROW = "NodeDetailsRow";

    //column family to keep track of message properties (count) under MESSAGE_COUNTERS_RAW_NAME
    public final static String MESSAGE_COUNTERS_COLUMN_FAMILY = "MessageCountDetails";
    public final static String MESSAGE_COUNTERS_RAW_NAME = "QueueMessageCountRow";

    public final static String MESSAGES_FOR_EXPIRY_COLUMN_FAMILY="MessagesForExpiration";

    public final static String MESSAGE_STATUS_CHANGE_COLUMN_FAMILY = "MessageStatusChange";

}
