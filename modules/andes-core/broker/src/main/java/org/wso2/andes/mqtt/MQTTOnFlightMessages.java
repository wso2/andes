/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.mqtt;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Holds the messages on flight, message identifier vs its subscriptions
 * So that when an ack is receive these information could be used to co-relate
 */
public class MQTTOnFlightMessages {
    /**
     * Holds the messages that are on flight - in the process of delivering them to its subscribers
     * Key - the id of the message, at a given time the id of the message will be unique across all subscriptions
     * Value - mqtt subscription which holds information relevant to co-relate to identify the cluster specific info
     * A given channel (subscription could be involved in multiple topics)
     */
    private Map<Integer, MQTTSubscription> messages = new ConcurrentHashMap<Integer, MQTTSubscription>();
    /**
     * At a given time the message id should be unique a across a given channel
     * This will be used to maintain a counter
     * In MQTT a message id should be a short value, need to ensure the id will not exceed this limit
     */
    private AtomicInteger currentMessageID = new AtomicInteger(0);

    private static Log log = LogFactory.getLog(MQTTOnFlightMessages.class);

    /**
     * Adds a message for the tracker, for the purpose of co-relating message information
     *
     * @param subscription the info related to the subscription involved in the message exchange
     */
    public Integer addMessage(MQTTSubscription subscription) {

        currentMessageID.incrementAndGet();
        if (currentMessageID.get() == Short.MAX_VALUE) {
            currentMessageID.set(1);
            log.info("Message id count has breached its maximum, refreshing the message ids");
        }

        int messageID = currentMessageID.get();
        messages.put(messageID, subscription);
        return messageID;
    }

    /**
     * removes a message from the tracker, this will be called when an ack is received for a given message
     *
     * @param messageID the id of the message required to be removed, id < SHORT.MAX
     * @return MQTTSubscriber
     */
    public MQTTSubscription removeMessage(Integer messageID) {
        return messages.remove(messageID);
    }
}
