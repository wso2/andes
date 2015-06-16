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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.dna.mqtt.wso2.AndesMQTTBridge.QOSLevel;

/**
 * Each topic will have multiple relationships with its subscribers, the list of subscribers and the topics will be
 * maintained through this class
 */
public class MQTTopic  {

    /**
     * Will log the messages generated through the class
     */
    private static Log log = LogFactory.getLog(MQTTopic.class);

    /**
     * Holds the name of the topic that is of relevance
     */
    private String topic;

    /**
     * Will maintain the client id generated for the topic
     * private String clusterSpecificClientID;
     * Will map between the relationship of topics and subscribers key will be the channel id of the subscriber
     * Value will be the properties associated with the subscriber
     */
    private Map<String, MQTTSubscription> subscribers = new HashMap<String, MQTTSubscription>();

    /**
     * Will construct the topic object which will hold references to all the subscribers
     * Will require the topic name, since there cannot be a topic without the name
     *
     * @param topicName the name of the topic registered
     */
    public MQTTopic(String topicName) {
        this.topic = topicName;
    }

    /**
     * @return name of the topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Will create a new subscriber for the topic
     *
     * @param mqttClientChannelID the channel identity of the subscriber bound to the topic
     * @param qos                 the level of qos which can be of value 0,1 or 2
     * @param isCleanSession      the durability of the session
     * @param clusterSpecificID   the id generated for the subscriber which is unique across the cluster
     * @param subscriptionChannel  will hold the unique cluster wide subscription identifier
     * @throws MQTTException if the subscriber with the same channel id exist
     */
    public void addSubscriber(String mqttClientChannelID, QOSLevel qos, boolean isCleanSession,
                              String clusterSpecificID, UUID subscriptionChannel)
    throws MQTTException {
        MQTTSubscription subscriber = subscribers.get(mqttClientChannelID);
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
            //Will set the topic name
            subscriber.setTopicName(topic);
            //Will register the subscriber
            subscribers.put(mqttClientChannelID, subscriber);
            if (log.isDebugEnabled()) {
                log.debug("Subscriber with channel id :" + mqttClientChannelID + " with qos :" + qos +
                        " having clean session :" + isCleanSession);
            }

        } else {
            //If the subscriber with the same channel id exists
            final String message = "Subscriber with channel id " + mqttClientChannelID + " exists already";
            throw new MQTTException(message);
        }
    }

    /**
     * Will add the subscriber object directly to the topic, this method was designed to be called during callback
     *
     * @param mqttchannelID the id of the channel of the subscriber
     * @param subscriber    the subscription
     */
    public void addSubscriber(String mqttchannelID, MQTTSubscription subscriber) {
        //Will not do a check here, since this operation will mostly be called during the call back
        subscribers.put(mqttchannelID, subscriber);
    }

    /**
     * Removes the subscription entry that is bound with the topic
     *
     * @param mqttClientChannelID the channel id of the subscriber bound to the topic
     * @return the subscriber object which will be removed
     * @throws MQTTException will indicate if an error occurs during subscription entry removal
     */
    public MQTTSubscription removeSubscriber(String mqttClientChannelID) throws MQTTException {
        //Will get the subscription
        MQTTSubscription subscriber = subscribers.remove(mqttClientChannelID);
        //If there was no subscriber to be removed
        if (null == subscriber) {
            final String message = "Subscriber with id " + mqttClientChannelID + " cannot be found";
            throw new MQTTException(message);
        }

        return subscriber;
    }

    /**
     * Returns the subscription relevant for a particular channel
     *
     * @param mqttClientChannelID the channel id of the subscription
     * @return the subscription
     */
    public MQTTSubscription getSubscription(String mqttClientChannelID) {
        return subscribers.get(mqttClientChannelID);
    }

    /**
     * Will provide the cluster wide id generated for the topic subscription
     *
     * @param mqttClientChannelID the local subscription channel id
     * @return the cluster specific id generated for subscription
     */
    public String getSubscriptionID(String mqttClientChannelID) {
        MQTTSubscription subscriber = subscribers.get(mqttClientChannelID);
        return subscriber != null ? subscriber.getSubscriberChannelID() : null;
    }


}
