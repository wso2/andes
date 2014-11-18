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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * All the subscriptions relation to a topic will be maintained though the following class, attributes such as QOS
 * levels will be maintained here
 */
public class MQTTSubscriber {
    //TODO QOS level information should be accessed for use cases which will be implimented in future
    //The level of QOS the subscriber is bound to
    private int QOS_Level;
    //Specifies whether the subscription is durable or not
    private boolean isCleanSession;
    //Specifies the channel id of the subscriber
    private String subscriberChannelID;
    //Specifies the storage identifier of the subscription
    private String storageIdentifier;
    //Specifies the subscription channel
    private UUID subscriptionChannel;
    //The map maintains the relation between the cluster ids to the local ids, MQTT ids will be the type int
    //Each subscription object will maintain the ids of the messages that were sent out for delivery
    //Upon recieving of an ack the message element will be removed
    //Cluster message id to local messages the key will be the local id of the map and the value will be cluster id
    //TODO check if a concurren hashmap is neccessary here
    private Map<Integer, Long> clusterMessageToLocalMessage = new ConcurrentHashMap<Integer, Long>();
    //Will hold the message id which was last processed
    //Will generate a unique id
    //TODO check to see if making this volatile is neccassary
    private volatile int lastGeneratedMessageID = 0;


    /**
     * Will add the details of the message that will be delivered among the subscriptions
     *
     * @param clusterMessageID the unique cluster identifier of the message
     * @return a unique identifier for the local message sent through the subscription
     */
    public int markSend(long clusterMessageID) {
        lastGeneratedMessageID += 1;
        if (lastGeneratedMessageID >= Short.MAX_VALUE) {
            //Then we need to reduce
            //TODO add a debug log here
            //TODO assuming that the acks are receved on the oreder which was sent, this is wrong need to re think
            lastGeneratedMessageID = 0;

        }
        //Here the local id will be the key since we do not accept the same local id to duplicate
        clusterMessageToLocalMessage.put(lastGeneratedMessageID, clusterMessageID);
        return lastGeneratedMessageID;
    }

    public long ackReceived(int localMessageID) {
        //TODO throw exception if the id cannot be correlated
        long custerID = clusterMessageToLocalMessage.remove(localMessageID);
        return custerID;
    }

    /**
     * The channel a purticuler subscription is bound to
     *
     * @return the uuid of the channel
     */
    public UUID getSubscriptionChannel() {
        return subscriptionChannel;
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
     * The storage representation of the subscritpion
     *
     * @param storageIdentifier the identifcation of the storage representation
     */
    public void setStorageIdentifier(String storageIdentifier) {
        this.storageIdentifier = storageIdentifier;
    }

    /**
     * Will allow retrival of the unique identifyer of the subscriber
     *
     * @return the identifier of the subscriber
     */
    public String getSubscriberChannelID() {
        return subscriberChannelID;
    }

    /**
     * Set the id generated for the subscriber locally
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
     * Will set the lvel of QOS the subscriber is bound to
     *
     * @param QOS_Level the QOS level, this can either be 1,2 or 3
     */
    public void setQOS_Level(int QOS_Level) {
        this.QOS_Level = QOS_Level;
    }

}
