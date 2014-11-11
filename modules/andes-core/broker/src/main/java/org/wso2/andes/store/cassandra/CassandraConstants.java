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

package org.wso2.andes.store.cassandra;

import me.prettyprint.cassandra.serializers.*;

/**
 * Constants used by Cassandra Stores related classes
 */
public class CassandraConstants {

    // connection properties
    /**
     * Connection property to get the jndi lookup name (value) of the data source
     */
    public static final String PROP_JNDI_LOOKUP_NAME = "dataSource";

    /**
     * Cassandra cluster objects replication factor for key space.
     */
    public static final String PROP_REPLICATION_FACTOR = "replicationFactor";

    /**
     * GC grace seconds for Cassandra. ( Specifies the time to wait before garbage collecting
     * tombstones in Cassandra )
     */
    public static final String PROP_GC_GRACE_SECONDS = "GCGraceSeconds";

    /**
     * Read Consistency level. From How many replicas to be read before satisfying the read request
     */
    public static final String PROP_READ_CONSISTENCY = "readConsistencyLevel";

    /**
     * Write consistency level. How many replicas to be successfully written before acknowledging
     */
    public static final String PROP_WRITE_CONSISTENCY = "writeConsistencyLevel";

    /**
     * Replication placement strategy (algorithm) to be used is defined in this class
     */
    public static final String PROP_STRATEGY_CLASS = "strategyClass";

    /**
     * Keysapce to be used by MB
     */
    public final static String KEYSPACE = "QpidKeySpace";

    /**
     * Long data type for Cassandra
     */
    public final static String LONG_TYPE = "LongType";

    /**
     * Integer Data type for Cassandra
     */
    public final static String INTEGER_TYPE = "IntegerType";

    /**
     * String data type for Cassandra
     */
    public final static String STRING_TYPE = "StringType";

    /**
     * UTF8 data type for Cassandra
     */
    public final static String UTF8_TYPE = "UTF8Type";

    /**
     * Username property to access Cassandra DB through Hector client.
     */
    public static final String PROP_USERNAME = "username";

    /**
     * Password property to access Cassandra DB through Hector client.
     */
    public static final String PROP_PASSWORD = "password";

    public static StringSerializer stringSerializer = StringSerializer.get();
    public static LongSerializer longSerializer = LongSerializer.get();
    public static BytesArraySerializer bytesArraySerializer = BytesArraySerializer.get();
    public static IntegerSerializer integerSerializer = IntegerSerializer.get();
    public static ByteBufferSerializer byteBufferSerializer = ByteBufferSerializer.get();

    //column family to keep track of loaded exchanges
    public final static String EXCHANGE_COLUMN_FAMILY = "ExchangeColumnFamily";
    public final static String EXCHANGE_ROW = "ExchangesRow";

    //column family to keep track of queues created
    public final static String QUEUE_COLUMN_FAMILY = "QueueColumnFamily";
    public final static String QUEUE_ROW = "QueuesRow";

    //column family to keep track of bindings
    public final static String BINDING_COLUMN_FAMILY = "BindingColumnFamily";

    //column family to add and remove message content with their <messageID,offset> values
    public final static String MESSAGE_CONTENT_COLUMN_FAMILY = "MessageContent";

    //column family to keep messages for node queues (<nodequeue,messageID>)
    public final static String NODE_QUEUES_COLUMN_FAMILY = "NodeQueues";

    //column family to keep messages for global queues (<global-queue,messageID>)
    public final static String GLOBAL_QUEUES_COLUMN_FAMILY = "GlobalQueue";

    //column family to keep message metadata for queues
    public final static String META_DATA_COLUMN_FAMILY = "MetaData";

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

    /**
     * Number of seconds to configure as the GC Grace seconds when creating Cassandra column
     * families. It is set to 10days by default.
     */
    public final static String DEFAULT_GC_GRACE_SECONDS = "864000";

    /**
     * Default replication factor for Cassandra.
     */
    public final static String DEFAULT_REPLICATION_FACTOR = "1";

    /**
     * Default strategy class for Cassandra.
     */
    public final static String DEFAULT_STRATEGY_CLASS = "org.apache.cassandra.locator" +
            ".SimpleStrategy";
}
