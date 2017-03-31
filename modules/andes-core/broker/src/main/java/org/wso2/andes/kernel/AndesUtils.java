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

package org.wso2.andes.kernel;

import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.metrics.MetricsConstants;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.store.MessageMetaDataType;
import org.wso2.andes.tools.utils.MessageTracer;
import org.wso2.carbon.metrics.manager.Counter;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.Meter;
import org.wso2.carbon.metrics.manager.MetricManager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * This class holds utility methods for Andes. Commonly
 * accessible methods for the whole broker are piled
 * here
 */
public class AndesUtils {

    private static Log log = LogFactory.getLog(AndesUtils.class);

    //this constant will be used to prefix storage queue name for AMQP topics
    public final static String AMQP_TOPIC_STORAGE_QUEUE_PREFIX = "AMQP_Topic";

    //this constant will be used to prefix storage queue name for MQTT topics
    public final static String MQTT_TOPIC_STORAGE_QUEUE_PREFIX = "MQTT_Topic";

    //This will be used to co-relate between the message id used in the browser and the message id used internally in MB
    private static ConcurrentHashMap<String, Long> browserMessageIdCorrelater = new ConcurrentHashMap<>();


    /**
     * Register a mapping between browser message Id and Andes message Id. This is expected to be invoked
     * whenever messages are passed to the browser via a browser subscription and is expecting a return from browser
     * with browser message Id which needs to be resolved to Andes Message Id.
     * <p>
     * These mappings should be cleaned after they have served their purpose.
     *
     * @param browserMessageId The browser message Id / External message Id
     * @param andesMessageId   Respective Andes message Id
     */
    public static synchronized void registerBrowserMessageId(String browserMessageId, long andesMessageId) {
        browserMessageIdCorrelater.put(browserMessageId, andesMessageId);
    }

    /**
     * Get the respective Andes message Id for a given browser message Id.
     *
     * @param browserMessageId The browser message Id / External message Id
     * @return Andes message Id
     */
    public static synchronized Long getAndesMessageId(String browserMessageId) {
        Long andesMessageId;
        if (browserMessageIdCorrelater.containsKey(browserMessageId)) {
            andesMessageId = browserMessageIdCorrelater.get(browserMessageId);
        } else {
            andesMessageId = -1L;
        }
        return andesMessageId;
    }

    /**
     * Generate storage queue name for given internal queue information
     * @param routingKey    routing key by which queue is bound
     * @param messageRouterName name of queue bound message router
     * @param queueName internal queue name
     * @param isQueueDurable is queue durable
     * @return name that should be used as storage queue
     */
    public static String getStorageQueueForDestination(String routingKey, String messageRouterName, String
            queueName, boolean isQueueDurable) {
        String storageQueueName;
        String nodeID = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        // We need to add a prefix so that we could differentiate if queue is created under the same name
        //as topic
        if (AMQPUtils.TOPIC_EXCHANGE_NAME.equals(messageRouterName)) {
            if (!isQueueDurable) {
                storageQueueName = AMQP_TOPIC_STORAGE_QUEUE_PREFIX + "_" + routingKey + "_" + nodeID;
            } else {
                storageQueueName = queueName;
            }
        } else if(MQTTUtils.MQTT_EXCHANGE_NAME.equals(messageRouterName)) {
            if (!isQueueDurable) {
                storageQueueName = MQTT_TOPIC_STORAGE_QUEUE_PREFIX + "_" + routingKey + "_" + nodeID;
            } else {
                storageQueueName = queueName;
            }
        } else if(AMQPUtils.DIRECT_EXCHANGE_NAME.equals(messageRouterName)){
            storageQueueName = routingKey;
        } else {
            storageQueueName = queueName;
        }
        return storageQueueName;
    }
}
