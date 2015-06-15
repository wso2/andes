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

import me.prettyprint.cassandra.serializers.StringSerializer;

/**
 * Constants used by Hector related classes
 */
public class HectorConstants {

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
    public static final String PROP_GC_GRACE_SECONDS = "gcGraceSeconds";

    /**
     * Read Consistency level. From How many replicas to be read before satisfying the read request
     */
    public static final String PROP_READ_CONSISTENCY = "readConsistencyLevel";

    /**
     * Write consistency level. How many replicas to be successfully written before acknowledging
     */
    public static final String PROP_WRITE_CONSISTENCY = "writeConsistencyLevel";

    /**
     * Key space is similar to the database name in RDBMS
     */
    public static final String PROP_KEYSPACE = "keyspace";

    /**
     * Replication placement strategy (algorithm) to be used is defined in this class
     */
    public static final String PROP_STRATEGY_CLASS = "strategyClass";

    /**
     * Default keysapce used by MB
     */
    public final static String DEFAULT_KEYSPACE = "QpidKeySpace";

    /**
     * Long data type for Cassandra
     */
    public final static String LONG_TYPE = "LongType";

    /**
     * Integer Data type for Cassandra
     */
    public final static String INTEGER_TYPE = "IntegerType";

    /**
     * UTF8 data type for Cassandra
     */
    public final static String UTF8_TYPE = "UTF8Type";

    public static StringSerializer stringSerializer = StringSerializer.get();

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

    //column family to keep message metadata for queues
    public final static String META_DATA_COLUMN_FAMILY = "MetaData";
    
    public final static String SUBSCRIPTIONS_COLUMN_FAMILY = "Subscriptions";

    //column family to keep track of nodes and their syncing info (i.e bind IP Address) under NODE_DETAIL_ROW.
    public final static String NODE_DETAIL_COLUMN_FAMILY = "CusterNodeDetails";
    public final static String NODE_DETAIL_ROW = "NodeDetailsRow";

    //column family to keep track of message properties (count) under MESSAGE_COUNTERS_RAW_NAME
    public final static String MESSAGE_COUNTERS_COLUMN_FAMILY = "MessageCountDetails";
    public final static String MESSAGE_COUNTERS_RAW_NAME = "QueueMessageCountRow";

    public final static String MESSAGES_FOR_EXPIRY_COLUMN_FAMILY="MessagesForExpiration";

    /**
     * Used to write/read/delete 'test-data' in order to check cassandra can be
     * accessed with specified
     * read/write consistency level
     */
    public final static String MESSAGE_STORE_STATUS_COLUMN_FAMILY = "MessageStoreStatus";
    
    /**
     * Column stores a long value (current time) while checking for connectivity
     */
    public final static String MESSAGE_STORE_STATUS_COLUMN_TIME_STAMP = "TimeStamp";

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

    /**
     * Default read consistency for Cassandra.
     */
    public final static String DEFAULT_READ_CONSISTENCY = "ONE";

    /**
     * Default write consistency for Cassandra.
     */
    public final static String DEFAULT_WRITE_CONSISTENCY = "ONE";

}
