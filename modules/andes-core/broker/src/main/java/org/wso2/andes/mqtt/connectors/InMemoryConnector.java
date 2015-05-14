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
package org.wso2.andes.mqtt.connectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.SubscriptionAlreadyExistsException;
import org.wso2.andes.kernel.distruptor.inbound.PubAckHandler;
import org.wso2.andes.mqtt.MQTTException;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.mqtt.MQTTopicManager;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Will be used to handle the incoming messages through the in-memory store, this will be supported only for QoS 0
 */
public class InMemoryConnector implements MQTTConnector {

    //Stores the subscriptions relevant for the topic
    //MAP<Topic,Channels> - Key - the list of topics and value - the list of subscribed channels
    private Map<String, List<String>> messageSubscription = new HashMap<String, List<String>>();
    private static Log log = LogFactory.getLog(InMemoryConnector.class);


    /**
     * {@inheritDoc}
     */
    @Override
    public void messageAck(long messageID, String topicName, String storageName, UUID subChannelID)
            throws AndesException {
        //Fully in-memory mode will only be compatible for messages with QoS 0 therefore a message ack will not be,
        //received
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessage(ByteBuffer message, String topic, int qosLevel, int mqttLocalMessageID,
                           boolean retain, String publisherID, PubAckHandler pubAckHandler) throws MQTTException {
        broadcastMessages(topic, message, mqttLocalMessageID, qosLevel, retain, publisherID, pubAckHandler);
        if (log.isDebugEnabled()) {
            log.debug("Message published to topic " + topic + " with publisher id " + mqttLocalMessageID);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addSubscriber(MQTTopicManager channel, String topic, String clientID, String mqttClientID,
                              boolean isCleanSession, int qos, UUID subscriptionChannelID) throws MQTTException,
            SubscriptionAlreadyExistsException {

        List<String> subscribers = messageSubscription.get(topic);
        //If this is the first subscriber
        if (null == subscribers) {
            subscribers = new ArrayList<String>();
        }
        subscribers.add(mqttClientID);
        //Will add the final list to the subscription
        messageSubscription.put(topic, subscribers);

        log.info("Subscription with id " + clientID + " registered to topic " + topic);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeSubscriber(MQTTopicManager channel, String subscribedTopic, String subscriptionChannelID,
                                 UUID subscriberChannel, boolean isCleanSession, String mqttClientID)
            throws MQTTException {

        handleSubscriptionRemoval(subscribedTopic, mqttClientID);
        log.info("Subscription with id " + mqttClientID + " registered to topic " + subscribedTopic);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void disconnectSubscriber(MQTTopicManager channel, String subscribedTopic, String subscriptionChannelID,
                                     UUID subscriberChannel, boolean isCleanSession, String mqttClientID)
            throws MQTTException {

        handleSubscriptionRemoval(subscribedTopic, mqttClientID);
        log.info("Subscription with id " + mqttClientID + " removed from " + subscribedTopic);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID removePublisher(String mqttClientChannelID) {
        return null;
    }

    /**
     * Will broadcast a message among its subscribers
     *
     * @param topic        published topic name
     * @param messages     message content
     * @param messageID    unique message identifier
     * @param publishedQoS the level of published QoS
     * @param retain       should the message be retain
     * @param clientID     the client identifier
     * @param ackHandler   the acknowledgment handling engine
     */
    private void broadcastMessages(String topic, ByteBuffer messages, int messageID, int publishedQoS, boolean retain,
                                   String clientID, PubAckHandler ackHandler) {
        //In case if the message was published at QoS level 0 we send the ack back to the client
        //NOTE : the in-memory mode will only support QoS level 0 subscriptions since a store will not get involved
        sendPublisherAck(publishedQoS, messageID, clientID, ackHandler);
        List<String> subscribers = messageSubscription.get(topic);
        //Will distribute among the subscribers
        for (String subChannel : subscribers) {
            try {
                //We allow only QoS 0 messages to be exchanged in-memory
                int memoryQoSLevel = 0;
                MQTTopicManager.getInstance().distributeMessageToSubscriber(topic, messages, messageID,
                        memoryQoSLevel, retain, subChannel, memoryQoSLevel);
                if (log.isDebugEnabled()) {
                    log.debug("Message " + messageID + " Delivered to subscription " + subChannel + " to topic " + topic);
                }
            } catch (MQTTException e) {
                String message = "Error occurred while sending the message to subscriber";
                log.error(message, e);
                //We do not throw the exception any further, the process should not hand here
            }
        }
    }

    /**
     * Sends acknowledgments to the publisher
     *
     * @param publishedQoS   the level of published qos
     * @param localMessageID the unique message identifier
     * @param clientID       the client identifier
     * @param ackHandler     the acknowledgment handler
     */
    private void sendPublisherAck(int publishedQoS, int localMessageID, String clientID, PubAckHandler ackHandler) {
        AndesMessageMetadata metaData = new AndesMessageMetadata();
        metaData.addProperty(MQTTUtils.QOSLEVEL, publishedQoS);
        metaData.addProperty(MQTTUtils.CLIENT_ID, clientID);
        metaData.addProperty(MQTTUtils.MESSAGE_ID, localMessageID);
        ackHandler.ack(metaData);
        if (log.isDebugEnabled()) {
            log.debug("Publisher ack sent to " + clientID + " for message id " + localMessageID);
        }
    }

    /**
     * handles removal of subscriptions
     *
     * @param subscribedTopic the name of the topic subscription
     * @param mqttClientID    the unique client identifier
     * @throws MQTTException
     */
    private void handleSubscriptionRemoval(String subscribedTopic, String mqttClientID) throws MQTTException {
        List<String> subscribers = messageSubscription.get(subscribedTopic);

        if (null == subscribers) {
            throw new MQTTException("There're no subscribers for topic " + subscribedTopic);
        }

        subscribers.remove(mqttClientID.intern());

        //After removal if the subscriber list is empty let's remove the subscription itself off
        if (subscribers.isEmpty()) {
            messageSubscription.remove(subscribedTopic);
        }

        if (log.isDebugEnabled()) {
            log.debug("Subscriber with id " + mqttClientID + " removed from topic " + subscribedTopic);
        }

    }

}
