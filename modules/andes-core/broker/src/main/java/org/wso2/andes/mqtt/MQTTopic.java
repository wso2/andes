/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Each topic will have multiple relationships with its subscribers, the list of subscribers and the topics will be
 * maintained through this class
 */
public class MQTTopic {

    //Will log the messages generated through the class
    private static Log log = LogFactory.getLog(MQTTopic.class);
    //Holds the name of the topic that is of relevance
    private String topic;
    //Will maintain the client id generated for the topic
    //private String clusterSpecificClientID;
    //Will map between the relationship of topics and subscribers key will be the channel id of the subscriber
    //Value will be the properties associated with the subscriber
    //TODO check whether concurrency should be considered here
    private Map<String, MQTTSubscriber> subscribers = new ConcurrentHashMap<String, MQTTSubscriber>();

    /**
     * Will construct the topic object which will hold references to all the subscribers
     * Will require the topic name, since there cannot be a topic without the name
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
     * @param mqttClientChannelID the channel identitiy of the subscriber bound to the topic
     * @param qos                 the level of qos which can be of value 0,1 or 2
     * @param isCleanSession      the durability of the session
     * @param clusterSpecificID   the id generated for the subscriber which is unique across the cluster
     * @throws MQTTException if the subscriber with the same channel id exist
     */
    public void addSubscriber(String mqttClientChannelID, int qos, boolean isCleanSession, String clusterSpecificID)
            throws MQTTException {
        MQTTSubscriber subscriber = subscribers.get(mqttClientChannelID);
        //Will create a new subscriber if the susbscriber do not exist
        if (subscriber == null) {
            subscriber = new MQTTSubscriber();
            //Will set the level of QOS of the subscriber
            subscriber.setQOS_Level(qos);
            //Will specify the durablitiy of the session
            subscriber.setCleanSession(isCleanSession);
            //Will set the subscriber channel id
            subscriber.setSubscriberChannelID(clusterSpecificID);
            //Will register the subscriber
            subscribers.put(mqttClientChannelID, subscriber);
            if (log.isDebugEnabled()) {
                log.debug("Subscriber with channel id :" + mqttClientChannelID + " with qos :" + qos +
                        " havin clean session :" + isCleanSession);
            }

        } else {
            //If the subscriber with the same channel id exists
            final String message = "Subscriber with channel id " + mqttClientChannelID + " exists already";
            throw new MQTTException(message);
        }
    }

    /**
     * Removes the subscription entry that is bound with the topic
     * @param mqttClientChannelID the channel id of the subscriber bound to the topic
     * @return the id of the subscriber that was removed
     * @throws MQTTException will indicate if an error occurs during subscription entry removal
     */
    public String removeSubscriber(String mqttClientChannelID) throws MQTTException {
        //Will get the subscription
        MQTTSubscriber subscriber = subscribers.remove(mqttClientChannelID);
        //If there was no subscriber to be removed
        if (subscriber == null) {
            final String message = "Subscriber with id " + mqttClientChannelID + " cannot be found";
            throw new MQTTException(message);
        }

        return subscriber.getSubscriberChannelID();
    }

    /**
     * Will provide the cluster wide id generated for the topic subscription
     * @param mqttClientChannelID the local subscription channel id
     * @return the cluster specific id generated for subscription
     */
    public String getSubscriptionID(String mqttClientChannelID) {
        MQTTSubscriber subscriber = subscribers.get(mqttClientChannelID);
        return subscriber != null ? subscriber.getSubscriberChannelID() : null;
    }


}
