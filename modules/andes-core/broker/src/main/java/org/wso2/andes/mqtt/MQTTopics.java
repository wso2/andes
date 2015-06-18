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
package org.wso2.andes.mqtt;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.dna.mqtt.wso2.AndesMQTTBridge.QOSLevel;

/**
 * Mapped against the channel id, a given channel could subscribe to 1..* topics
 * a channel cannot subscribe to the same topic more than once
 * The class would maintain the topics the channel is bound to
 */
public class MQTTopics {

    /**
     * Will log the onFlightMessages generated through the class
     */
    private static Log log = LogFactory.getLog(MQTTopics.class);

    /**
     * Holds the name of the channelID that is of relevance
     */
    private String channelID;

    /**
     * The list of topics the channel has bound to will be maintained
     * The key - The name of the topic
     * Value - The subscriptions which holds information {@link org.wso2.andes.mqtt.MQTTSubscription}
     * <p><b>Note: </b> A given channel cannot be bound to the same topic more than once</p>
     */
    private Map<String, MQTTSubscription> subscriptions = new HashMap<String, MQTTSubscription>();

    /**
     * Holds the onFlightMessages that are on flight - in the process of delivering them to its subscribers
     * Key - the id of the message, at a given time the id of the message will be unique across all subscriptions
     * Value - mqtt subscription which holds information relevant to co-relate to identify the cluster specific info
     * A given channel (subscription could be involved in multiple topics)
     */
    private Map<Integer, MQTTSubscription> onFlightMessages = new ConcurrentHashMap<Integer, MQTTSubscription>();

    /**
     * At a given time the message id should be unique a across a given channel
     * This will be used to maintain a counter
     * In MQTT a message id should be a short value, need to ensure the id will not exceed this limit
     */
    private AtomicInteger currentMessageID = new AtomicInteger(0);



    /**
     * Will construct the channel which will hold references to all subscriptions
     * @param channelID the name of the channelID registered
     */
    public MQTTopics(String channelID) {
        this.channelID = channelID;
    }

    /**
     * @return name of the channelID
     */
    public String getChannelID() {
        return channelID;
    }

    /**
     * Will create a new subscriber for the channelID
     *
     * @param mqttClientChannelID the channel identity of the subscriber bound to the channelID
     * @param qos                 the level of qos which can be of value 0,1 or 2
     * @param isCleanSession      the durability of the session
     * @param clusterSpecificID   the id generated for the subscriber which is unique across the cluster
     * @param subscriptionChannel  will hold the unique cluster wide subscription identifier
     * @throws MQTTException if the subscriber with the same channel id exist
     */
    public void addSubscriber(String mqttClientChannelID, QOSLevel qos, boolean isCleanSession,
                              String clusterSpecificID, UUID subscriptionChannel,String topicName)
    throws MQTTException {
        MQTTSubscription subscriber = subscriptions.get(topicName);
        //Will create a new subscriber if the subscriber do not exist
        if (null == subscriber) {
            subscriber = new MQTTSubscription();
            //Will set the level of QOS of the subscriber
            subscriber.setQOSLevel(qos);
            //Will specify the durability of the session
            subscriber.setCleanSession(isCleanSession);
            //Will set the subscriber channel id
            subscriber.setSubscriberChannelID(clusterSpecificID);
            //Will set the subscription channel
            subscriber.setSubscriptionChannel(subscriptionChannel);
            //Will set the channelID name
            subscriber.setTopicName(topicName);
            //Will register the subscriber
            subscriptions.put(topicName, subscriber);
            if (log.isDebugEnabled()) {
                log.debug("Subscriber with channel id :" + mqttClientChannelID + " with qos :" + qos +
                        " having clean session :" + isCleanSession);
            }

        } else {
            //If the subscriber with the same channel id exists
            final String message = "Subscriber with channel id " + mqttClientChannelID + " is already bound to "
                    +topicName;
            throw new MQTTException(message);
        }
    }

    /**
     * Will add the subscriber object directly to the channelID, this method was designed to be called during callback
     *
     * @param topicName     the name of the topic the subscription will be bound
     * @param subscriber    the subscription
     */
    public void addSubscriber(String topicName, MQTTSubscription subscriber) {
        //Will not do a check here, since this operation will mostly be called during the call back
        subscriptions.put(topicName, subscriber);
    }

    /**
     * Removes the subscription entry that is bound with the topic
     *
     * @param mqttTopic the topic name of the subscription to be removed
     * @return the subscriber object which will be removed
     * @throws MQTTException will indicate if an error occurs during subscription entry removal
     */
    public MQTTSubscription removeSubscriber(String mqttTopic) throws MQTTException {
        //Will get the subscription
        MQTTSubscription subscriber = subscriptions.remove(mqttTopic);
        //If there was no subscriber to be removed
        if (null == subscriber) {
            final String message = "Subscriber for topic " + mqttTopic + " cannot be found";
            throw new MQTTException(message);
        }

        return subscriber;
    }

    /**
     * Returns the subscription relevant for a particular topic
     *
     * @param topic the name of the topic the subscriptions is bound to
     * @return the subscription
     */
    public MQTTSubscription getSubscription(String topic) {
        return subscriptions.get(topic);
    }

    /**
     * Returns all the topic subscriptions a given channel is bound to
     * @return Collection<MQTTSubscription>
     */
    public Collection<MQTTSubscription> getAllSubscriptionsForChannel(){
        return subscriptions.values();
    }

    /**
     * Generates a unique message id for a message that's ready for distribution
     * Across a channel this id should be unique until the ack is received, the maximum value the id could holds is
     * SHORT.MAX
     *
     * @return the unique id generated for on-flight message, this should be < SHORT.MAX
     */
    private int getNextMessageID() {

        currentMessageID.incrementAndGet();
        if (currentMessageID.get() == Short.MAX_VALUE) {
            currentMessageID.set(1);
            log.info("Message id count has breached its maximum, refreshing the message ids");
        }

        return currentMessageID.get();
    }

    /**
     * When a message is sent out for distribution among the subscribers the cluster specific id will be maintained
     * <p><b>Note : <b/>MQTT message id could contain a maximum length of SHORT.MAX and the cluster id will be LONG
     * When the acknowledgment for a particular message is received the cluster specific id needs to be taken out to
     * indicate the acknowledgment to the cluster</p>
     *
     * @param topic                  the destination in which the message should be distributed
     * @param clusterMessageID       the id generated by andes for cluster representation of the message
     * @param storageQueueIdentifier the unique identifier of the storage queue (cluster representation)
     * @return the mqtt specific id generated co-relating the cluster specific id < SHORT.MAX
     * @throws org.wso2.andes.mqtt.MQTTException
     */
    public int addOnFlightMessage(String topic, String storageQueueIdentifier, long clusterMessageID)
            throws MQTTException {
        //Generates a unique id for the message, this should be
        int messageID = getNextMessageID();
        MQTTSubscription subscription = subscriptions.get(topic);

        if (null != subscription) {
            //Sets the storage queue identifier of the subscription
            subscription.setStorageIdentifier(storageQueueIdentifier);
            //Indicates that the message was dispatched for the distribution
            onFlightMessages.put(messageID, subscription);
            //Each subscription holds the cluster id <-> messages id in order to co-relate when ack is received
            subscription.markSend(clusterMessageID, messageID);
        } else {
            String error = "Error occurred while dispatching the message to the subscriber for channel id " +
                    channelID + " for topic " + topic;
            throw new MQTTException(error);
        }

        return messageID;
    }

    /**
     * Removes a given message from the tacking list
     * This operation is called when an ack is received for a particular message
     *
     * @param messageID the id of the message
     * @return MQTTSubscription holds information relevant for the subscription
     */
    public MQTTSubscription removeOnFlightMessage(int messageID) {
        return onFlightMessages.remove(messageID);
    }

}
