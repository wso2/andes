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
import org.dna.mqtt.wso2.AndesMQTTBridge;
import org.wso2.andes.kernel.AndesException;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Will manage and hold topic informaton,
 * this class will be declared as singleton since the state of the topics will be
 * preserved here, all the operations relational to a topic will go through this.
 */
public class MQTTopicManager {

    //Will log the messages generated through the class
    private static Log log = LogFactory.getLog(MQTTopicManager.class);
    //The instance which will be referred
    private static MQTTopicManager instance = new MQTTopicManager();
    //The topic name will be defined as the key and the value will hold the subscription information
    //TODO check whether its required the hashmap to be concurrent
    private Map<String, MQTTopic> topics = new ConcurrentHashMap<String, MQTTopic>();
    //Will keep the reference with the bridge
    private static AndesMQTTBridge mqttAndesConnectingBridge = null;
    //Will correlate between topic and subscribers
    //The map will be used when subscriber disconnection is called where the corresponding topic needs to be identified
    //The key of the map will be the client id and the value will be the topic
    private Map<String, String> clientTopicCorrelate = new ConcurrentHashMap<String, String>();


    /**
     * The class will be declared as singleton since the state will be cenrtaized
     */
    private MQTTopicManager() {
    }

    /**
     * All cenralized instance will be taken for
     *
     * @return the instance which will hold all relevent topic information
     */
    public static MQTTopicManager getInstance() {
        return instance;
    }

    /**
     * Will initialize the bridge which will connect with the MQTT protocol handler
     *
     * @param mqttAndesConnection connectin
     * @throws MQTTException will invalidate if attempted to initialize more than once
     */
    public void initProtocolEngine(AndesMQTTBridge mqttAndesConnection) throws MQTTException {
        if (null == mqttAndesConnectingBridge) {
            mqttAndesConnectingBridge = mqttAndesConnection;
            log.info("MQTT andes connecting bridge initialized successfully");
        } else {
            final String error = "Attempting to initialize the bridge more than once, there cannot be more than " +
                    "one bridge instance";
            log.error(error);
            throw new MQTTException(error);
        }
    }

    /**
     * Will add the message to the cluster notifying all its subscribers
     *
     * @param topic              the name of the topic the message was published
     * @param qosLevel           the level of qos the message was sent this can be either 0,1 or 2
     * @param message            the message content
     * @param retain             whether the message should retain
     * @param mqttLocalMessageID the channel in which the message was published
     * @throws MQTTException at a time where the message content doen't get registered
     */
    public void addTopicMessage(String topic, int qosLevel, ByteBuffer message, boolean retain,
                                int mqttLocalMessageID) throws MQTTException {

        //Will generate a unique id for the message which will be unique across the cluster
        long clusterSpecificMessageID = MQTTUtils.generateMessageID();
        if (log.isDebugEnabled()) {
            log.debug("Incoming message recived with id : " + mqttLocalMessageID + ", QOS level : " + qosLevel
                    + ", for topic :" + topic + ", with retain :" + retain);
            log.debug("Generated message cluster specific message id " + clusterSpecificMessageID +
                    " for mqtt local message id " + mqttLocalMessageID);
        }

        //Will add the topic message to the cluster for distribution
        MQTTChannel.getInstance().addMessageContent(message, clusterSpecificMessageID, topic, qosLevel,
                mqttLocalMessageID, retain);

    }

    /**
     * Will include the topic subscription to the list of topics maintained.
     *
     * @param topicName           the name of the topic the subscription is being registered for
     * @param mqttClientChannelID the channel identitiy of the subscriber mainatained by the protocol reference
     * @param qos                 the level of QOS the message subscription was created the value can be wither 0,1 or 2
     * @param isCleanSession      indicates whether the subscription should be durable or not
     * @throws MQTTException if the subscriber addition was not successful
     */
    public void addTopicSubscription(String topicName, String mqttClientChannelID, int qos,
                                     boolean isCleanSession) throws MQTTException {
        //TODO what if the subscribers disconnect while topic is conceptually removed
        //Will extract out the topic information if the topic is created already
        MQTTopic topic = topics.get(topicName);
        //If the topic has not being created before
        if (topic == null) {
            //First the topic should be registered in the cluster
            //Once the cluster registration is successful the topic will be created
            topic = new MQTTopic(topicName);
            //Will set the topic specific subscription id generated
            topics.put(topicName, topic);

        } else {
            if (log.isDebugEnabled()) {
                log.debug("The topic " + topic + "has local subsbscriptions already");
            }
        }
        //TODO address the possibility of two nodes subscribing to the same topic in the same manner
        //First the topic should be registered in the cluster
        String subscriptionID = registerTopicSubscriptionInCluster(topicName, mqttClientChannelID, isCleanSession);
        //Will add the subscription to the topic
        //The status will be false if the subscriber with the same channel id exists
        try {
            topic.addSubscriber(mqttClientChannelID, qos, isCleanSession, subscriptionID);
        } catch (MQTTException ex) {
            //In case if an error occurs we need to rollback the subscription created cluster wide
            MQTTChannel.getInstance().removeSubscriber(this, topicName, subscriptionID);
            final String message = "Error while adding the subscriber to the cluser";
            log.error(message);
            throw ex;
        }
        //Finally will register the the topic subscription for the topic
        clientTopicCorrelate.put(mqttClientChannelID, topicName);
    }

    /**
     * Will be called during the event where the subscriber disconnection is triggered
     *
     * @param mqttClientChannelID the id of the channel which the subscriber is bound to
     * @throws MQTTException occurs if the subscriber was not disconnected properly
     */
    public void removeTopicSubscription(String mqttClientChannelID) throws MQTTException {
        //First the topic name will be taken from the subscriber channel
        //TODO what if the node crashes at this point the state will be lost before disconnection
        String topic = clientTopicCorrelate.get(mqttClientChannelID);
        //If the topic has correlators
        if (null != topic) {
            //Will get the corresponding topic
            MQTTopic mqttTopic = topics.get(topic);

            if (null != mqttTopic) {
                //Will remove the subscription entitiy
                MQTTSubscriber subscriber = mqttTopic.removeSubscriber(mqttClientChannelID);
                String subscriberChannelID = subscriber.getSubscriberChannelID();
                //The corresponding subscription created cluster wide will be topic name and the local channel id
                //Will remove the subscriber clusterwide
                try {
                    MQTTChannel.getInstance().removeSubscriber(this, topic, subscriberChannelID);
                    //Will indicate the disconnection of the topic
                    if (log.isDebugEnabled()) {
                        final String message = "Subscription with cluster id " + subscriberChannelID + " disconnected " +
                                "from topic " + topic;
                        log.debug(message);
                    }
                } catch (MQTTException ex) {
                    //Should re state the connection of the subscriber back to the map
                    mqttTopic.addSubscriber(mqttClientChannelID, subscriber);
                    final String error = "Error occured while removing the subscription " + mqttClientChannelID;
                    log.error(error);
                    throw ex;
                }

            } else {
                final String message = "Error unidentified topic found for client id " + mqttClientChannelID;
                throw new MQTTException(message);
            }

        } else {
            final String message = "Error occured while disconnecting the subscriber, " +
                    "topic doesn't exist for client id " + mqttClientChannelID;
            throw new MQTTException(message);
        }
    }

    /**
     * Will notify to the subscribers who are bound to the topic
     *
     * @param storageName  the topic the message was published that will be stored
     * @param message      the message content
     * @param messageID    the idnetifier of the message
     * @param publishedQOS the level of qos the message was published
     * @param shouldRetain whether the message should retain after it was published
     * @throws Exception during a failure to deliver the message to the subscribers
     */
    public void subscriptionNotificationReceived(String storageName, ByteBuffer message, long messageID, int publishedQOS,
                                                 boolean shouldRetain, String channelID) throws Exception {
        //Will cast the cluster wide message to an int since the mqtt protocol engine maintain the id of the message as
        // int
        //TODO need to get rid of casting change the MQTT library to support long instead of int
        int mqttLocalMessageID = (int) messageID;
        //Should get the topic name from the channel id
        String topic = clientTopicCorrelate.get(channelID);
        AndesMQTTBridge.getBridgeInstance().notifySubscriptions(topic, publishedQOS, message, shouldRetain,
                mqttLocalMessageID, channelID);
        //We will indicate the ack to the kernal at this stage
        //TODO for QOS 0 we need to ack to the message here
        messageAck(topic, messageID, storageName);
    }

    /**
     * Will interact with the kernal and will create a cluster wide indication of the topic
     *
     * @param topicName      the name of the topic which should be registered in the cluster
     * @param mqttChannel    the subscriber id which is local to the node
     * @param isCleanSession should the subscription be identified as durable
     * @return topic subscription id which will represent the topic in the cluster
     */
    private String registerTopicSubscriptionInCluster(String topicName, String mqttChannel, boolean isCleanSession)
            throws MQTTException {
        //Will generate a unique id for the client
        //Per topic only one subscription will be created across the cluster
        String topicSpecificClientID = MQTTUtils.generateTopicSpecficClientID();
        if (log.isDebugEnabled()) {
            log.debug("Cluster wide topic connection was created with id " + topicSpecificClientID + " for topic " +
                    topicName + " with clean session " + isCleanSession);
        }

        //Will register the topic cluster wide
        MQTTChannel.getInstance().addSubscriber(this, topicName, topicSpecificClientID, mqttChannel, isCleanSession);

        return topicSpecificClientID;
    }

    /**
     * When acknowledments arrive for the delivered messages this method will be called
     *
     * @param topic       the name of the topic the ack was recived
     * @param messageID   the id of the message the ack was recived
     * @param storageName the name of the representation of the topic in the store
     * @throws MQTTException at an event where the ack was not properly processed
     */
    private void messageAck(String topic, long messageID, String storageName) throws MQTTException {
        try {
            MQTTChannel.getInstance().messageAck(messageID, topic, storageName);
        } catch (AndesException ex) {
            final String message = "Error occurred while cleaning up the acked message";
            log.error(message);
            throw new MQTTException(message, ex);
        }
    }

}
