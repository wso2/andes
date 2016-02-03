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

import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dna.mqtt.wso2.QOSLevel;
import org.wso2.andes.kernel.DeliverableAndesMetadata;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;

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
     * Holds the name of the channelId that is of relevance
     */
    private String channelId;

    /**
     * The list of topics the channel has bound to will be maintained
     * The key - The name of the topic
     * Value - The subscriptions which holds information {@link org.wso2.andes.mqtt.MQTTSubscription}
     * <p><b>Note: </b> A given channel cannot be bound to the same topic more than once</p>
     */
    private Map<String, MQTTSubscription> subscriptions = new ConcurrentHashMap<String, MQTTSubscription>();

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
    //private AtomicInteger currentMessageId = new AtomicInteger(0);

    private Set<Integer> messageIds = new ConcurrentSkipListSet<Integer>();

    /**
     * Will construct the channel which will hold references to all subscriptions
     *
     * @param channelID the name of the channelId registered
     * @param mIds      the list of possibilities for message ids set of Integers with numbers < SHORT.MAX
     */
    public MQTTopics(String channelID, Set<Integer> mIds) {
        this.channelId = channelID;
        this.messageIds.addAll(mIds);
    }

    /**
     * @return name of the channelId
     */
    public String getChannelId() {
        return channelId;
    }

    /**
     * Will create a new subscriber for the channelId
     *
     * @param mqttClientChannelID the channel identity of the subscriber bound to the channelId
     * @param qos                 the level of qos which can be of value 0,1 or 2
     * @param isCleanSession      the durability of the session
     * @param clusterSpecificID   the id generated for the subscriber which is unique across the cluster
     * @param subscriptionChannel will hold the unique cluster wide subscription identifier
     * @throws MQTTException if the subscriber with the same channel id exist
     */
    public void addSubscriber(String mqttClientChannelID, QOSLevel qos, boolean isCleanSession,
            String clusterSpecificID, UUID subscriptionChannel, String topicName) throws MQTTException {
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
            //Will set the channelId name
            subscriber.setTopicName(topicName);
            //Will register the subscriber
            subscriptions.put(topicName, subscriber);
            if (log.isDebugEnabled()) {
                log.debug("Subscriber with channel id :" + mqttClientChannelID + " with qos :" + qos +
                        " having clean session :" + isCleanSession);
            }

        } else {
            //If the subscriber with the same channel id exists
            final String message =
                    "Subscriber with channel id " + mqttClientChannelID + " is already bound to " + topicName;
            throw new MQTTException(message);
        }
    }

    /**
     * Will add the subscriber object directly to the channelId, this method was designed to be called during callback
     *
     * @param topicName  the name of the topic the subscription will be bound
     * @param subscriber the subscription
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
     *
     * @return Collection<MQTTSubscription>
     */
    public Collection<MQTTSubscription> getAllSubscriptionsForChannel() {
        return subscriptions.values();
    }

    /**
     * Generates a unique message id for a message that's ready for distribution
     * Across a channel this id should be unique until the ack is received, the maximum value the id could holds is
     * SHORT.MAX
     *
     * @return the unique id generated for on-flight message, this should be < SHORT.MAX
     */
    private Integer getNextMessageID() {

        Integer messageId = null;

        if (!messageIds.isEmpty()) {
            messageId = messageIds.iterator().next();
            messageIds.remove(messageId);
        } else {
            log.warn("Message ids cannot be generated, since it has reached its maximum");
        }

        return messageId;
    }

    /**
     * When a message is sent out for distribution among the subscribers the cluster specific id will be maintained
     * <p><b>Note : <b/>MQTT message id could contain a maximum length of SHORT.MAX and the cluster id will be LONG
     * When the acknowledgment for a particular message is received the cluster specific id needs to be taken out to
     * indicate the acknowledgment to the cluster</p>
     *
     * @param topic            the destination in which the message should be distributed
     * @param clusterMessageID the id generated by andes for cluster representation of the message
     * @param metadata         message information relevant that should be used when sending a nack
     * @return the mqtt specific id generated co-relating the cluster specific id < SHORT.MAX
     * @throws org.wso2.andes.mqtt.MQTTException
     */
    public int addOnFlightMessage(String topic, long clusterMessageID, DeliverableAndesMetadata metadata)
            throws MQTTException {

        MQTTSubscription subscription = subscriptions.get(topic);

        Integer messageID;

        if (null != subscription) {
            //If the message is being resent
            messageID = subscription.getMessageID(clusterMessageID);

            if (null == messageID) {
                //This means the message id has not be generated for this message
                //Here a new message id will be generated
                messageID = getNextMessageID();
            }

            //Indicates that the message was dispatched for the distribution
            onFlightMessages.put(messageID, subscription);
            //unAckedMessages.add(messageID);
            //messageIdTime.put(System.currentTimeMillis(), messageID);
            //Each subscription holds the cluster id <-> messages id in order to co-relate when ack is received
            subscription.markSent(clusterMessageID, messageID, metadata);
        } else {
            String error = "A subscriber has been disconnected while dispatching the message for channel id " +
                    channelId + " for topic " + topic;
            throw new MQTTException(error);
        }

        return messageID;
    }

    /**
     * This is called when retrieving values to send rejection ack
     *
     * @param allMessages list of messages that has all the integer values < SHORT.MAX
     * @return the set messages that are on flight and had not being
     */
    public Set<Integer> getUnackedMessages(Set<Integer> allMessages) {
        //We take the allMessages/messageIds since message id holds messages which have not being dispatched
        Set<Integer> unAcknowledgedMessages = new LinkedHashSet<Integer>();
        unAcknowledgedMessages.addAll(messageIds);
        unAcknowledgedMessages.removeAll(allMessages);
        return unAcknowledgedMessages;
    }

    /**
     * Returns the subscription information, the information will include the correlations between the locally generated
     * message ids and cluster specific ids
     *
     * @param messageID the id of the message the information is required
     * @return the subscription information
     */
    public MQTTSubscription getSubscription(Integer messageID) {
        return onFlightMessages.get(messageID);
    }

    /**
     * Removes a given message from the tacking list
     * This operation is called when an ack is received for a particular message
     *
     * @param messageId the id of the message
     * @return MQTTSubscription holds information relevant for the subscription
     */
    public MQTTSubscription removeOnFlightMessage(int messageId) {
        return onFlightMessages.remove(messageId);
    }

    /**
     * Adds the message id to be reused
     * <p>This method is invoked when an ack is received for a given message, so that its id could be reused
     * </p>
     * <p><b>Note: </b> This should be called after calling call necessary operations related to ack handling, so that
     * there will not be any state inconsistencies</p>
     *
     * @param messageId the id the message which was acked
     */
    public void addMessageId(int messageId) {
        messageIds.add(messageId);
    }

}
