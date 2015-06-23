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


import org.dna.mqtt.wso2.QOSLevel;
import org.wso2.andes.kernel.AndesMessageMetadata;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * All the topicOccurrences relation to a topic will be maintained though the following class, attributes such as QOS
 * levels will be maintained here, a given channel could occur in multiple topics, the subscription will be unique
 */
public class MQTTSubscription {
    /**
     * The level of QOS the subscriber is bound to
     */
    private QOSLevel QOSLevel;
    /**
     * Specifies whether the subscription is durable or not
     */
    private boolean isCleanSession;
    /**
     * Specifies the channel id of the subscriber
     */
    private String subscriberChannelID;
    /**
     * Specifies the storage identifier of the subscription
     */
    private String storageIdentifier;
    /**
     * Specifies the subscription channel
     */
    private UUID subscriptionChannel;

    /**
     * The name of the topic the subscription is bound to
     */
    private String topicName;

    /**
     * The map maintains the relation between the cluster ids to the local ids, MQTT ids will be the type int
     * Each subscription object will maintain the ids of the messages that were sent out for delivery
     * Upon relieving of an ack the message element will be removed
     * Cluster message id to local messages the key will be the local id of the map and the value will be cluster id
     * We use a concurrent hash-map since this map is accessible by multiple threads. Accessed by both andes kernel for
     * put operations and remove is done when the ack arrives
     */
    private Map<Integer, Long> localMessageToClusterMessage = new ConcurrentHashMap<Integer, Long>();

    /**
     * co-relates between the cluster representation of the message to its message information
     * key- the cluster message id
     * value - message information {@link org.wso2.andes.mqtt.MQTTSubscriptionInformation}
     */
    private Map<Long, MQTTSubscriptionInformation> clusterMessageToMessageInformation =
            new ConcurrentHashMap<Long, MQTTSubscriptionInformation>();

    /**
     * Will add the details of the message that will be delivered among the topicOccurrences
     *
     * @param clusterMessageID the unique cluster identifier of the message
     * @param mid              a locally generated id for the subscriber
     * @param metaInfo         holds message information relevant to the message
     */
    public void markSent(long clusterMessageID, int mid, AndesMessageMetadata metaInfo) {
        localMessageToClusterMessage.put(mid, clusterMessageID);

        MQTTSubscriptionInformation subscriptionInfo = new MQTTSubscriptionInformation();
        subscriptionInfo.setLocalMessageID(mid);
        subscriptionInfo.setMetadata(metaInfo);

        clusterMessageToMessageInformation.put(clusterMessageID, subscriptionInfo);
    }

    /**
     * Will get an id for the message if its existing, else will return -1 indicating that an id has not being generated
     * <p><b>Note: </b>When a message is sent to its subscriptions the first time it will return null,
     * else if its a resend the particular id will be returned </p>
     *
     * @param clusterID the cluster representation of the id relevant for the message
     * @return if the message is a resend the corresponding message id, null if there's no message id defined
     */
    public Integer getMessageID(long clusterID) {
        MQTTSubscriptionInformation subscriptionInformation = clusterMessageToMessageInformation.get(clusterID);

        if (null == subscriptionInformation) {
            //This means there's not existing id defined
            return null;
        } else {
            //This means the message is a resend
            return subscriptionInformation.getLocalMessageID();
        }
    }

    /**
     * Will be called upon receiving an ack for a message
     *
     * @param localMessageID the id of the message the ack was received
     * @return the cluster specific message if of the message which received the ack
     */
    public long ackReceived(int localMessageID) {
        Long clusterID = localMessageToClusterMessage.remove(localMessageID);
        clusterMessageToMessageInformation.remove(clusterID);
        return clusterID;
    }

    /**
     * Gets message meta information form a provided local id, meta information is required to send rejection for
     * un-acked messages.
     * {@link org.wso2.andes.kernel.Andes#messageRejected(org.wso2.andes.kernel.AndesMessageMetadata)}
     * @param localID the local message id generated before dispatching the message to its subscriptions
     * @return the meta information relevant for the message
     */
    public AndesMessageMetadata getMessageMetaInformation(Integer localID){
        long clusterID = localMessageToClusterMessage.get(localID);

        MQTTSubscriptionInformation mqttSubscriptionInformation = clusterMessageToMessageInformation.get(clusterID);

        if(null != mqttSubscriptionInformation){
            return mqttSubscriptionInformation.getMetadata();
        }else {
            return null;
        }
    }

    /**
     * The channel a particular subscription is bound to
     *
     * @return the uuid of the channel
     */
    public UUID getSubscriptionChannel() {
        return subscriptionChannel;
    }

    public boolean isCleanSession() {
        return isCleanSession;
    }

    /**
     * The channel the subscription would bound to
     *
     * @param subscriptionChannel the id of the subscription
     */
    public void setSubscriptionChannel(UUID subscriptionChannel) {
        this.subscriptionChannel = subscriptionChannel;
    }

    /**
     * The storage representation of the message
     *
     * @return the storage name where the message would be represented
     */
    public String getStorageIdentifier() {
        return storageIdentifier;
    }

    /**
     * The storage representation of the subscription
     *
     * @param storageIdentifier the identification of the storage representation
     */
    public void setStorageIdentifier(String storageIdentifier) {
        this.storageIdentifier = storageIdentifier;
    }

    /**
     * Will allow retrieval of the unique identifier of the subscriber
     *
     * @return the identifier of the subscriber
     */
    public String getSubscriberChannelID() {
        return subscriberChannelID;
    }

    /**
     * Set the id generated for the subscriber locally
     *
     * @param subscriberChannelID the unique subscription identifier
     */
    public void setSubscriberChannelID(String subscriberChannelID) {
        this.subscriberChannelID = subscriberChannelID;
    }

    /**
     * Indicates whether the subscription is durable or not, false if not
     *
     * @param isCleanSession whether the subscription is durable
     */
    public void setCleanSession(boolean isCleanSession) {
        this.isCleanSession = isCleanSession;
    }

    /**
     * Will set the level of QOS the subscriber is bound to
     *
     * @param QOSLevel the QOS level, this can either be 1,2 or 3
     */
    public void setQOSLevel(QOSLevel QOSLevel) {
        this.QOSLevel = QOSLevel;
    }

    /**
     * Will return the level of QOS the subscriber is bound to
     * @return QOS level
     */
    public QOSLevel getQOSLevel() {
        return this.QOSLevel;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

}
