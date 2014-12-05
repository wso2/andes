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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.configuration.enums;

import org.wso2.andes.configuration.util.ConfigurationProperty;
import org.wso2.andes.configuration.util.ImmutableMetaProperties;
import org.wso2.andes.configuration.util.MetaProperties;

import java.util.List;

/**
 * All Andes specific config properties directly defined in broker.xml (plus any linked configuration files) are
 * defined here.
 * If this is getting crowded, please break into multiple purposeful enums for ease of use. Kept in one class for now.
 */
public enum AndesConfiguration implements ConfigurationProperty {

    /**
     * The host IP to be used by the Thrift server. Thrift is used to coordinate message slots between MB nodes.
     */
    COORDINATION_THRIFT_SERVER_HOST
            ("coordination/thriftServerHost", "127.0.0.1", String.class),

    /**
     * Port dedicated to be used for the thrift server
     */
    COORDINATION_THRIFT_SERVER_PORT
            ("coordination/thriftServerPort", "7611", Integer.class),

    /**
     * Node ID is the unique identifier of a node within a cluster. By default, its generated using the IP of the node.
     * However, with this property, the Node ID can be explicitly set.
     */
    COORDINATION_NODE_ID("coordination/nodeID", "default", String.class),

    /**
     * The IP address to which mqtt/amqp channels should be bound.
     */
    TRANSPORTS_BIND_ADDRESS("transports/bindAddress", "*", String.class),

    /**
     * Enable this to support JMS messaging with added AMQP behaviour.
     */
    TRANSPORTS_AMQP_ENABLED("transports/amqp/@enabled", "true", Boolean.class),

    /**
     * The port used to listen for amqp messages/commands by the MB server.
     */
    TRANSPORTS_AMQP_PORT("transports/amqp/port", "5672", Integer.class),

    /**
     * The SSL port used to listen for amqp messages/commands by the MB server.
     */
    TRANSPORTS_AMQP_SSL_PORT("transports/amqp/sslPort", "8672", Integer.class),

    /**
     * By default, expired messages are sent to the Dead Letter Channel for later revival/reference. But,
     * in cases where expired messages can pile up in the DLC, this behaviour can be disabled.
     */
    TRANSPORTS_AMQP_SEND_EXPIRED_MESSAGES_TO_DLC("transports/amqp/sendExpiredMessagesToDLC",
            "true", Boolean.class),

    /**
     * By default, in case of a failure during message publishing, MB will re-attempt to publish for 9 more times.
     * This can be modified according to your reliability requirements.
     */
    TRANSPORTS_AMQP_MAXIMUM_REDELIVERY_ATTEMPTS("transports/amqp/maximumRedeliveryAttempts",
            "10", Integer.class),

    /**
     * For durable topics there can be only one topic subscriber cluster-wide per a particular
     * client id. Enabling this configuration, multiple subscribers can use same client id and
     * share the messages
     */
    ALLOW_SHARED_SHARED_SUBSCRIBERS("transports/amqp/allowSharedTopicSubscriptions",
             "false", Boolean.class),

    /**
     * Enable this to support lightweight messaging with the MQTT protocol.
     */
    TRANSPORTS_MQTT_ENABLED("transports/mqtt/@enabled", "true", Boolean.class),

    /**
     * The port used to listen for mqtt messages/commands by the MB server.
     */
    TRANSPORTS_MQTT_PORT("transports/mqtt/port", "1883", Integer.class),

    /**
     * The SSL port used to listen for mqtt messages/commands by the MB server.
     */
    TRANSPORTS_MQTT_SSL_PORT("transports/mqtt/sslPort", "1884", Integer.class),

    /**
     * This is a temporary list of user elements to enable user-authentication for MQTT.
     */
    LIST_TRANSPORTS_MQTT_USERNAMES("transports/mqtt/users/user/@userName", "",
            List.class),

    /**
     * Request the username of a user based on its index in the transports/mqtt/users/user collection.
     */
    TRANSPORTS_MQTT_USERNAME("transports/mqtt/users/user[{i}]/@userName", "",
            String.class),

    /**
     * Request the password of a user based on its index in the transports/mqtt/users/user collection.
     */
    TRANSPORTS_MQTT_PASSWORD("transports/mqtt/users/user[{i}]/@password", "",
            String.class),

    /**
     * The class that is used to access an external RDBMS database to operate on messages.
     */
    PERSISTENCE_MESSAGE_STORE_HANDLER("persistence/messageStore/@class",
            "org.wso2.andes.store.jdbc.JDBCMessageStoreImpl", String.class),

    /**
     * List of properties that can define how the server will access the store.
     * For now, the following properties are used.
     * 1. asyncStoring : if set to true, all database operations will be done asynchronously.
     * 2. dataSource : the dataSource identifier specified at the MB_HOME/conf/datasources/master-datasources.xml.
     */
    LIST_PERSISTENCE_MESSAGE_STORE_PROPERTIES("persistence/messageStore/property/@name", "", List.class),

    /**
     * This can be used to access a property by giving its key. e.g. dataSource
     */
    PERSISTENCE_MESSAGE_STORE_PROPERTY("persistence/messageStore/property[@name = '{key}']", "", String.class),

    /**
     * The class that is used to access an external RDBMS database to operate on server context. e.g. subscriptions
     */
    PERSISTENCE_CONTEXT_STORE_HANDLER("persistence/contextStore/@class",
            "org.wso2.andes.store.jdbc.JDBCAndesContextStoreImpl", String.class),

    /**
     * List of properties that can define how the server will access the store.
     * For now, the following properties are used.
     * 1. asyncStoring : if set to true, all context store operations will be done asynchronously.
     * 2. dataSource : the dataSource identifier specified at the MB_HOME/conf/datasources/master-datasources.xml.
     */
    LIST_PERSISTENCE_CONTEXT_STORE_PROPERTIES("persistence/contextStore/property/@name", "", List.class),

    /**
     * This can be used to access a property of the context store by giving its key. e.g. dataSource
     */
    PERSISTENCE_CONTEXT_STORE_PROPERTY("persistence/contextStore/property[@name = '{key}']", "", String.class),

    /**
     * The ID generation class that is used to maintain unique IDs for each message that arrives at the server.
     */
    PERSISTENCE_ID_GENERATOR("persistence/idGenerator", "org.wso2.andes.server.cluster" +
            ".coordination.TimeStampBasedMessageIdGenerator", String.class),

    /**
     * Maximum time interval until which a slot can be retained in memory before updating to the cluster.
     * NOTE : specified in milliseconds.
     */
    PERFORMANCE_TUNING_SLOTS_SLOT_RETAIN_TIME_IN_MEMORY("performanceTuning/slots" +
            "/slotRetainTimeInMemory", "1000", Long.class),

    /**
     * Rough estimate for size of a slot. e.g. If the slot window size is 1000, given 3 nodes, it can expand up to 3000.
     */
    PERFORMANCE_TUNING_SLOTS_SLOT_WINDOW_SIZE("performanceTuning/slots/windowSize", "1000",
            Integer.class),

    /**
     * Number of Slot Delivery Worker threads that should be started.
     */
    PERFORMANCE_TUNING_SLOTS_WORKER_THREAD_COUNT("performanceTuning/slots/workerThreadCount", "5",
            Integer.class),

    /**
     * Maximum number of undelivered messages that can be in memory. Increasing this value could cause out of memory
     * scenarios, but performance will be improved
     */
    PERFORMANCE_TUNING_DELIVERY_MAX_READ_BUT_UNDELIVERED_MESSAGES("performanceTuning/delivery" +
            "/maxNumberOfReadButUndeliveredMessages", "1000", Integer.class),

    /**
     * This is the Thread pool size which will be used by the queue delivery workers. Make this to a higher number if
     * there are lots of unique queues to the system at a given time.
     */
    PERFORMANCE_TUNING_DELIVERY_PUBLISHER_POOL_SIZE("performanceTuning/delivery" +
            "/publisherPoolSize", "50", Integer.class),

    /**
     * This is the ring buffer size of the delivery disruptor. This value should be a power of 2 (E.g. 1024, 2048,
     * 4096). Use a small ring size if you want to reduce the memory usage.
     */
    PERFORMANCE_TUNING_DELIVERY_RING_BUFFER_SIZE("performanceTuning/delivery/ringBufferSize", "4096", Integer.class),

    /**
     * Number of parallel readers used to read content from message store. Increasing this value will speedup
     * the message sending mechanism. But the load on the data store will increase.
     */
    PERFORMANCE_TUNING_DELIVERY_PARALLEL_CONTENT_READERS("performanceTuning/delivery/parallelContentReaders", "5",
                                                         Integer.class),

    /**
     * Number of parallel delivery handlers used to send messages to subscribers. Increasing this value will speedup
     * the message sending mechanism. But the system load will increase.
     */
    PERFORMANCE_TUNING_DELIVERY_PARALLEL_DELIVERY_HANDLERS("performanceTuning/delivery/parallelDeliveryHandlers", "5",
                                                         Integer.class),

    /**
     * Number of parallel writers used to write content to message store. Increasing this value will speedup
     * the message receiving mechanism. But the load on the data store will increase.
     */
    PERFORMANCE_TUNING_PARALLEL_CONTENT_WRITERS("performanceTuning/inbound/parallelContentWriters", "2",
            Integer.class),

    /**
     * Size of the Disruptor ring buffer for inbound event handling. Buffer size should be a value of power of two
     * For publishing at higher rates increasing the buffer size may give some advantage to keep messages in memory and
     * write.
     */
    PERFORMANCE_TUNING_PUBLISHING_BUFFER_SIZE("performanceTuning/inbound/bufferSize", "65536", Integer.class),

    /**
     * Message delivery from server to the client will be paused temporarily if number of delivered but
     * unacknowledged message count reaches this size. Should be set considering message consume rate.
     */
    PERFORMANCE_TUNING_ACK_HANDLING_MAX_UNACKED_MESSAGES("performanceTuning/ackHandling" +
            "/maxUnackedMessages", "1000", Integer.class),

    /**
     * Time interval after which the Virtual host syncing Task can sync host details across the cluster.
     * specified in seconds.
     */
    PERFORMANCE_TUNING_FAILOVER_VHOST_SYNC_TASK_INTERVAL("performanceTuning/failover" +
            "/vHostSyncTaskInterval", "3600", Integer.class),

    /**
     * Number of parallel threads that will handle routing the messages received at the broker.
     */
    PERFORMANCE_TUNING_ROUTING_WORKER_THREAD_COUNT("performanceTuning/messageRouting/workerThreadCount",
            "5", Integer.class),

    /**
     * Number of parallel threads that will handle acknowledgement of a message receipt from a consumer.
     */
    PERFORMANCE_TUNING_ACK_HANDLING_WORKER_THREAD_COUNT
            ("performanceTuning/ackHandling/workerThreadCount", "50", Integer.class),

    /**
     * Ack handler count for disruptor based event handling.
     */
    PERFORMANCE_TUNING_ACK_HANDLER_COUNT("performanceTuning/ackHandling/handlerCount", "8",
            Integer.class ),
    /**
     * Time interval after which the server will remove message content from the store in the background. If the
     * message rate is very high users can set this to a lower value.
     * Specified in seconds.
     */
    PERFORMANCE_TUNING_DELETION_CONTENT_REMOVAL_TASK_INTERVAL
            ("performanceTuning/messageDeletion/contentRemovalTaskInterval", "600", Integer.class),

    /**
     * Time to wait before removing a message from the store in PubSub implementation.
     * Specified in milliseconds.
     */
    PERFORMANCE_TUNING_DELETION_CONTENT_REMOVAL_TIME_DIFFERENCE
            ("performanceTuning/messageDeletion/contentRemovalTimeDifference", "600000",
                    Integer.class),

    /**
     * Since server startup, whenever this interval elapses, the expired messages will be cleared from the store.
     */
    PERFORMANCE_TUNING_MESSAGE_EXPIRATION_CHECK_INTERVAL
            ("performanceTuning/messageExpiration/checkInterval", "10000", Integer.class),

    /**
     * The number of expired messages to be cleared in one store operation.
     */
    PERFORMANCE_TUNING_MESSAGE_EXPIRATION_BATCH_SIZE
            ("performanceTuning/messageExpiration/messageBatchSize", "1000", Integer.class),

    /**
     * The number of messages to be cached in-memory
     */
    PERFORMANCE_TUNING_STORE_OPERATIONS_MESSAGE_CONTENT_CACHE
            ("performanceTuning/storeOperations/messageContentCache", "1000", Integer.class),

    /**
     * The number of messages to be handled in a single operation related to browser subscriptions.
     */
    MANAGEMENT_CONSOLE_MESSAGE_BATCH_SIZE_FOR_BROWSER_SUBSCRIPTIONS("managementConsole" +
            "/messageBatchSizeForBrowserSubscriptions", "200", Integer.class),

    /**
     * The time interval at which the server should check for memory consumption and apply flow control to recover.
     */
    FLOW_CONTROL_MEMORY_BASED_MEMORY_CHECK_INTERVAL("flowControl/memoryBased" +
            "/memoryCheckInterval", "20000", Long.class),

    /**
     * The maximum ratio of memory allowed to be used by the server.
     * Specified as a decimal between 0 and 1. (Safe-Memory-Limit/Allocated-Memory)
     */
    FLOW_CONTROL_MEMORY_BASED_GLOBAL_MEMORY_THRESHOLD_RATIO("flowControl/memoryBased" +
            "/globalMemoryThresholdRatio", "0.4", Double.class),

    /**
     * The ratio at which the server should apply flow control to recover.
     * Specified as a decimal between 0 and 1. (Used-Memory/Allocated-Memory)
     */
    FLOW_CONTROL_MEMORY_BASED_GLOBAL_MEMORY_RECOVERY_THRESHOLD_RATIO("flowControl/memoryBased" +
            "/globalMemoryRecoveryThresholdRatio", "0.5", Double.class),

    /**
     * This allows you to apply flow control based on the message count on a given connection.
     * NOT USED FOR NOW.
     */
    FLOW_CONTROL_CONNECTION_BASED_PER_CONNECTION_MESSAGE_THRESHOLD("flowControl/connectionBased" +
            "/perConnectionMessageThreshold", "1000", Integer.class);


    private final MetaProperties metaProperties;

    private AndesConfiguration(String keyInFile, String defaultValue, Class<?> dataType) {
        // We need to pass the enum name as the identifier : therefore this.name()
        this.metaProperties = new ImmutableMetaProperties(this.name(),keyInFile, defaultValue, dataType);
    }

    @Override
    public MetaProperties get() {
        return metaProperties;
    }
}
