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

import org.wso2.andes.configuration.modules.JKSStore;
import org.wso2.andes.configuration.util.ConfigurationProperty;
import org.wso2.andes.configuration.util.ImmutableMetaProperties;
import org.wso2.andes.configuration.util.MetaProperties;
import org.wso2.andes.configuration.util.TopicMessageDeliveryStrategy;
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
     * Thrift server reconnect timeout. Value specified in SECONDS
     */
    COORDINATOR_THRIFT_RECONNECT_TIMEOUT("coordination/thriftServerReconnectTimeout", "5", Long.class),

    /**
     * We use Hazelcast reliable topics to share all notifications across the cluster (e.g. subscription changes).
     * And this property defines the time-to-live for a notification since its creation. (in Seconds)
     */
    COORDINATION_CLUSTER_NOTIFICATION_TIMEOUT("coordination/clusterNotificationTimeout", "10", Integer.class),

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
     * The IP address to which mqtt channels should be bound.
     */
    TRANSPORTS_MQTT_BIND_ADDRESS("transports/mqtt/bindAddress", "*", String.class),

    /**
     * The IP address to which amqp channels should be bound.
     */
    TRANSPORTS_AMQP_BIND_ADDRESS("transports/amqp/bindAddress", "*", String.class),

    /**
     * Enable this to support JMS messaging with added AMQP behavior.
     */
    TRANSPORTS_AMQP_ENABLED("transports/amqp/@enabled", "true", Boolean.class),

    /**
     * The port used to listen for non-secure amqp messages/commands by the MB server.
     */
    TRANSPORTS_AMQP_DEFAULT_CONNECTION_PORT("transports/amqp/defaultConnection/@port", "5672", Integer.class),

    /**
     * Enable/disable the default, non-secure AMQP connection.
     */
    TRANSPORTS_AMQP_DEFAULT_CONNECTION_ENABLED("transports/amqp/defaultConnection/@enabled", "true", Boolean.class),

    /**
     * Enable/disable the secure AMQP connection.
     */
    TRANSPORTS_AMQP_SSL_CONNECTION_ENABLED("transports/amqp/sslConnection/@enabled", "true", Boolean.class),

    /**
     * The port used to listen for secure amqp messages/commands by the MB server.
     */
    TRANSPORTS_AMQP_SSL_CONNECTION_PORT("transports/amqp/sslConnection/@port", "8672", Integer.class),

    /**
     * The key store used for AMQP SSL connection.
     */
    TRANSPORTS_AMQP_SSL_CONNECTION_KEYSTORE("transports/amqp/sslConnection/keyStore", "", JKSStore.class),

    /**
     * The trust store used for AMQP SSL connection.
     */
    TRANSPORTS_AMQP_SSL_CONNECTION_TRUSTSTORE("transports/amqp/sslConnection/trustStore", "", JKSStore.class),

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
     * Topics and queue names are validated according to AMQP specification. Therefore special
     * characters are not allowed in the topic and queue names. By enabling this property strict
     * validation will not be done against topic and queue names.
     */
    ALLOW_STRICT_NAME_VALIDATION("transports/amqp/allowStrictNameValidation",
            "true", Boolean.class),

    /**
     * Enable this to support lightweight messaging with the MQTT protocol.
     */
    TRANSPORTS_MQTT_ENABLED("transports/mqtt/@enabled", "true", Boolean.class),

    /**
     * The port used to listen for mqtt messages/commands by the MB server.
     */
    TRANSPORTS_MQTT_DEFAULT_CONNECTION_PORT("transports/mqtt/defaultConnection/@port", "1883", Integer.class),

    /**
     * Enable/disable the default, non-secure MQTT connection.
     */
    TRANSPORTS_MQTT_DEFAULT_CONNECTION_ENABLED("transports/mqtt/defaultConnection/@enabled", "true", Boolean.class),

    /**
     * The SSL port used to listen for mqtt messages/commands by the MB server.
     */
    TRANSPORTS_MQTT_SSL_CONNECTION_PORT("transports/mqtt/sslConnection/@port", "8883", Integer.class),

    /**
     * Enable/disable the secure MQTT connection.
     */
    TRANSPORTS_MQTT_SSL_CONNECTION_ENABLED("transports/mqtt/sslConnection/@enabled", "true", Boolean.class),

    /**
     * The key store used for MQTT SSL connection.
     */
    TRANSPORTS_MQTT_SSL_CONNECTION_KEYSTORE("transports/mqtt/sslConnection/keyStore", "", JKSStore.class),

    /**
     * The trust store used for MQTT SSL connection.
     */
    TRANSPORTS_MQTT_SSL_CONNECTION_TRUSTSTORE("transports/mqtt/sslConnection/trustStore", "", JKSStore.class),

    /**
     * Ring buffer size of MQTT inbound event Disruptor. Default is set to 32768 (1024 * 32)
     */
    TRANSPORTS_MQTT_INBOUND_BUFFER_SIZE("transports/mqtt/inboundBufferSize", "32768", Integer.class),

    /**
     * Ring buffer size of MQTT delivery event Disruptor. Default is set to 32768 (1024 * 32)
     */
    TRANSPORTS_MQTT_DELIVERY_BUFFER_SIZE("transports/mqtt/deliveryBufferSize", "32768", Integer.class),

    /**
     * This is a temporary list of user elements to enable user-authentication for MQTT.
     */
    LIST_TRANSPORTS_MQTT_USERNAMES("transports/mqtt/users/user/@userName", "",
            List.class),

    /**
     * Class name of the authenticator to use. class should inherit from {@link org.dna.mqtt.moquette.server.IAuthenticator}
     * <p>Note: default implementation authenticates against carbon user store based on supplied username/password     
     */
    TRANSPORTS_MQTT_USER_AUTHENTICATOR_CLASS("transports/mqtt/security/authenticator", 
                                             "org.wso2.carbon.andes.authentication.andes.CarbonBasedMQTTAuthenticator", String.class),
                                              
    
    /**
     * Instructs the MQTT server to sending credential is required or optional. 
     * This behavior is subject to change in mqtt specification v 3.1.1
     */
    TRANSPORTS_MQTT_USER_ATHENTICATION("transports/mqtt/security/authentication", "OPTIONAL", MQTTUserAuthenticationScheme.class),

    /**
     * The class that is used to access an external RDBMS database to operate on messages.
     */
    PERSISTENCE_MESSAGE_STORE_HANDLER("persistence/messageStore/@class",
            "JDBCMessageStoreImpl", String.class),

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
            "JDBCAndesContextStoreImpl", String.class),

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
     * Size of the messages cache in MBs. Setting '0' will disable the cache. defaults to 256 MB.
     */
    PERSISTENCE_CACHE_SIZE("persistence/cache/size", "256", Integer.class),
    
    /**
     * Expected concurrency for the cache (4 is guava default)
     */
    PERSISTENCE_CACHE_CONCURRENCY_LEVEL("persistence/cache/concurrencyLevel", "4", Integer.class),
    
    /**
     * Number of seconds cache will keep messages after they are
     * added (unless they are consumed and deleted).
     */
    PERSISTENCE_CACHE_EXPIRY_SECONDS("persistence/cache/expirySeconds", "2", Integer.class),

    /**
     * Reference type used to hold messages in memory.
     * 
     * <p>
     * <ul>
     *  <li>weak   - Using java weak references ( - results higher cache misses)</li>
     *  <li>strong - ordinary references ( - higher cache hits, but not good if
     *               server is going to run with limited heap size + under severe load).
     *  </li>
     * </ul>
     * </p>
     */
    PERSISTENCE_CACHE_VALUE_REFERENCE_TYPE("persistence/cache/valueReferenceType", "strong", String.class),

    /**
     * Indicates weather print cache related statistics in 2 minutes interval in carbon log.
     */
    PERSISTENCE_CACHE_PRINT_STATS("persistence/cache/printStats", "false", Boolean.class),

    
    /**
     * The ID generation class that is used to maintain unique IDs for each message that arrives at the server.
     */
    PERSISTENCE_ID_GENERATOR("persistence/idGenerator", "org.wso2.andes.server.cluster" +
            ".coordination.TimeStampBasedMessageIdGenerator", String.class),

    /**
     * This is the Task interval (in SECONDS) to check weather communication
     * is healthy between message store (/Database) and this server instance.
     */
    PERSISTENCE_STORE_HEALTH_CHECK_INTERVAL("persistence/storeHealthCheckInterval", "10", Integer.class),

    /**
     * Andes core will store message content chunks according to this chunk size. Different database will
     * have limits and performance gains by tuning this parameter.
     * <p/>
     * For instance in MySQL the maximum table column size for content is less than 65534, which is the default
     * chunk size of AMQP. By changing this parameter to a lesser value we can store large content
     * chunks converted to smaller content chunks within the DB with this parameter.
     */
    PERFORMANCE_TUNING_MAX_CONTENT_CHUNK_SIZE
            ("performanceTuning/contentHandling/maxContentChunkSize", "65500", Integer.class),

    /**
     * Within Andes there are content chunk handlers which convert incoming large content chunks
     * into max content chunk size allowed by Andes core. These handlers run in parallel converting
     * large content chunks to smaller chunks.
     * <p/>
     * If the protocol specific content chunk size is different from the max chunk size allowed by Andes core
     * and there are significant number of large messages published, then having multiple handlers will
     * increase performance.
     */
    PERFORMANCE_TUNING_CONTENT_CHUNK_HANDLER_COUNT
            ("performanceTuning/contentHandling/contentChunkHandlerCount", "3", Integer.class),

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
     * Published message information is sent to slot coordinator by the node when it either reaches the slot window
     * size or the window creation timeout in milliseconds. This configures the timeout for slot window creation task.
     * <p>
     * In a slow message publishing scenario, this is the delay for each message for delivery.
     * For instance if we publish one message per minute then each message will have to wait
     * till this timeout before the messages are submitted to the slot coordinator.
     */
    PERFORMANCE_TUNING_SUBMIT_SLOT_TIMEOUT (
            "performanceTuning/slots/windowCreationTimeout", "3000", Integer.class),

    /**
     * Maximum number of undelivered messages that can be in memory. Increasing this value could cause out of memory
     * scenarios, but performance will be improved
     */
    PERFORMANCE_TUNING_DELIVERY_MAX_READ_BUT_UNDELIVERED_MESSAGES("performanceTuning/delivery" +
            "/maxNumberOfReadButUndeliveredMessages", "1000", Integer.class),

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
     * Content batch size for each content batch read. This is a loose guarantee.
     */
    PERFORMANCE_TUNING_DELIVERY_CONTENT_READ_BATCH_SIZE("performanceTuning/delivery/contentReadBatchSize", "65000",
            Integer.class),

    /**
     * Specify the maximum number of entries the cache may contain
     */
    PERFORMANCE_TUNING_DELIVERY_CONTENT_CACHE_MAXIMUM_SIZE("performanceTuning/delivery/contentCache/maximumSize", "100",
                                                           Integer.class),

    /**
     * Specify the time in minutes that each entry should be automatically removed from the cache after the entry's
     * creation
     */
    PERFORMANCE_TUNING_DELIVERY_CONTENT_CACHE_EXPIRY_TIME("performanceTuning/delivery/contentCache/expiryTime", "120",
                                                          Integer.class),

    /**
     * Number of parallel writers used to write content to message store. Increasing this value will speedup
     * the message receiving mechanism. But the load on the data store will increase.
     */
    PERFORMANCE_TUNING_PARALLEL_MESSAGE_WRITERS("performanceTuning/inboundEvents/parallelMessageWriters", "1",
            Integer.class),

    /**
     * Number of parallel writers used to write content to message store for transaction based publishing.
     * Increasing this value will speedup commit duration for a transaction.
     * But the load on the data store will increase.
     */
    PERFORMANCE_TUNING_PARALLEL_TRANSACTION_MESSAGE_WRITERS(
            "performanceTuning/inboundEvents/transactionMessageWriters", "1", Integer.class),

    /**
     * Size of the Disruptor ring buffer for inbound event handling. Buffer size should be a value of power of two
     * For publishing at higher rates increasing the buffer size may give some advantage to keep messages in memory and
     * write.
     */
    PERFORMANCE_TUNING_PUBLISHING_BUFFER_SIZE("performanceTuning/inboundEvents/bufferSize", "65536", Integer.class),

    /**
     * Maximum batch size of the batch write operation for inbound messages. Batch write of a message will vary around
     * this number.
     */
    PERFORMANCE_TUNING_MESSAGE_WRITER_BATCH_SIZE
            ("performanceTuning/inboundEvents/messageWriterBatchSize", "70", Integer.class),

    /**
     * Timeout for waiting for a queue purge event to end to get the purged count. Doesn't affect actual purging.
     * If purge takes time, increasing the value will improve the possibility of retrieving the correct purged count.
     * Having a lower value doesn't stop purge event. Getting the purged count is affected by this
     */
    PERFORMANCE_TUNING_PURGED_COUNT_TIMEOUT
            ("performanceTuning/inboundEvents/purgedCountTimeout", "180", Integer.class),
    
    /**
     * Average batch size of the batch acknowledgement handling for message acknowledgements. Andes will be updated
     * of acknowledgements batched around this number.
     */
    PERFORMANCE_TUNING_ACKNOWLEDGEMENT_HANDLER_BATCH_SIZE
            ("performanceTuning/ackHandling/ackHandlerBatchSize", "30", Integer.class),

    /**
     * Ack handler count for disruptor based event handling.
     */
    PERFORMANCE_TUNING_ACK_HANDLER_COUNT("performanceTuning/ackHandling/ackHandlerCount", "8",
            Integer.class ),

    /**
     * Message delivery from server to the client will be paused temporarily if number of delivered but
     * unacknowledged message count reaches this size. Should be set considering message consume rate.
     */
    PERFORMANCE_TUNING_ACK_HANDLING_MAX_UNACKED_MESSAGES("performanceTuning/ackHandling" +
            "/maxUnackedMessages", "1000", Integer.class),

    /**
     * When delivering topic messages to multiple topic subscribers a strategy can be chosen.
     */
    PERFORMANCE_TUNING_TOPIC_MESSAGE_DELIVERY_STRATEGY("performanceTuning/delivery/"
            + "topicMessageDeliveryStrategy/strategyName", TopicMessageDeliveryStrategy.DISCARD_NONE.toString() ,
            TopicMessageDeliveryStrategy.class),

    /**
     * If you choose DISCARD_ALLOWED topic message delivery strategy, we keep messages in memory
     * until ack is done until this timeout. If an ack is not received under this timeout, ack will
     * be simulated internally and real acknowledgement is discarded.
     */
    PERFORMANCE_TUNING_TOPIC_MESSAGE_DELIVERY_TIMEOUT("performanceTuning/delivery/"
            + "topicMessageDeliveryStrategy/deliveryTimeout", "60" , Integer.class),

    /**
     * Time interval after which the Virtual host syncing Task can sync host details across the cluster.
     * specified in seconds.
     */
    PERFORMANCE_TUNING_FAILOVER_VHOST_SYNC_TASK_INTERVAL("performanceTuning/failover" +
            "/vHostSyncTaskInterval", "3600", Integer.class),


     /**
     * Since server startup, whenever this interval elapses, the expired messages will be cleared from the store.
      * This messageExpiration configuration was disabled in the broker.xml configuration file. This is for future use.
     */
    PERFORMANCE_TUNING_MESSAGE_EXPIRATION_CHECK_INTERVAL
            ("performanceTuning/messageExpiration/checkInterval", "10000", Integer.class),

    /**
     * The number of expired messages to be cleared in one store operation.
     * This messageExpiration configuration was disabled in the broker.xml configuration file. This is for future use.
     */
    PERFORMANCE_TUNING_MESSAGE_EXPIRATION_BATCH_SIZE
            ("performanceTuning/messageExpiration/messageBatchSize", "1000", Integer.class),

    /**
     * Maximum batch size (Messages) for a transaction. Exceeding this limit will result in a failure in the subsequent
     * commit request. Default is set to 10MB. Limit is calculated considering the payload of messages
     */
    MAX_TRANSACTION_BATCH_SIZE ("transaction/maxBatchSizeInBytes", "10000000", Integer.class),

    /**
     * The number of messages to be fetched per page when browsing message in management console.
     */
    MANAGEMENT_CONSOLE_MESSAGE_BROWSE_PAGE_SIZE("managementConsole" +
            "/messageBrowsePageSize", "100", Integer.class),

    /**
     * This property defines the maximum message content length that can be displayed at the management console when
     * browsing queues. If the message length exceeds the value, a truncated content will be displayed with a statement
     * "message content too large to display." at the end. default value is 100000 (can roughly display a 100KB message.)
     * NOTE : Increasing this value could cause delays when loading the message content page.
     * (Introduced as per jira MB_939. )
     */
    MANAGEMENT_CONSOLE_MAX_DISPLAY_LENGTH_FOR_MESSAGE_CONTENT("managementConsole" +
            "/maximumMessageDisplayLength", "100000", Integer.class),

    /**
     * This is the per publisher buffer size low limit which disable the flow control for a channel if the flow-control
     * was enabled previously.
     */
    FLOW_CONTROL_BUFFER_BASED_LOW_LIMIT("flowControl/bufferBased" +
                                        "/lowLimit", "100", Integer.class),

    /**
     * This is the per publisher buffer size high limit which enable the flow control for a channel.
     */
    FLOW_CONTROL_BUFFER_BASED_HIGH_LIMIT("flowControl/bufferBased" +
                                         "/highLimit", "1000", Integer.class),

    /**
     * This is the global buffer low limit that disable the flow control globally if the flow-control
     * was enabled previously.
     */
    FLOW_CONTROL_GLOBAL_LOW_LIMIT("flowControl/global" +
                                        "/lowLimit", "800", Integer.class),

    /**
     *  This is the global buffer high limit which enable the flow control globally.
     */
    FLOW_CONTROL_GLOBAL_HIGH_LIMIT("flowControl/global" +
                                         "/highLimit", "8000", Integer.class),

    /**
     * This allows you to apply flow control based on the message count on a given connection.
     * NOT USED FOR NOW.
     */
    FLOW_CONTROL_CONNECTION_BASED_PER_CONNECTION_MESSAGE_THRESHOLD("flowControl/connectionBased" +
            "/perConnectionMessageThreshold", "1000", Integer.class),

    /**
     * Concurrently reads storage queues to make warm startup faster. But increasing concurrent value to big number
     * may cause heavy load to Cassandra.
     */
    RECOVERY_MESSAGES_CONCURRENT_STORAGE_QUEUE_READS("recovery/concurrentStorageQueueReads", "5", Integer.class),

    /**
     * Enable RDBMS slot information store
     */
    SLOT_MANAGEMENT_STORAGE("slotManagement/storage", "RDBMS", String.class);

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
