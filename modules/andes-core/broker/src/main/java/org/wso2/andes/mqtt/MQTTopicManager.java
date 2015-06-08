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
import org.wso2.andes.kernel.SubscriptionAlreadyExistsException;
import org.wso2.andes.mqtt.connectors.DistributedStoreConnector;
import org.wso2.andes.mqtt.connectors.MQTTConnector;
import org.wso2.andes.mqtt.utils.MQTTUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.dna.mqtt.wso2.AndesMQTTBridge.*;

/**
 * Will manage and hold topic information,
 * this class will be declared as singleton since the state of the topics will be
 * preserved here, all the operations relational to a topic will go through this.
 */
public class MQTTopicManager {

    /*
     * Will log the messages generated through the class
     */
    private static Log log = LogFactory.getLog(MQTTopicManager.class);
    /**
     * The instance which will be referred
     */
    private static MQTTopicManager instance = new MQTTopicManager();
    /**
     * The topic name will be defined as the key and the value will hold the subscription information
     * We could go with the hash map since we don't need immediate reflection of values being added during the runtime
     * i.e subscription getting bound when a message is given out for distribution, for topics we only need to deliver
     * messages to subscribers who were bound before the message was published to the broker
     */
    private Map<String, MQTTopic> topics = new HashMap<String, MQTTopic>();
    /**
     * Will keep the reference with the bridge
     */
    private static AndesMQTTBridge mqttAndesConnectingBridge = null;
    /**
     * Will correlate between topic and subscribers
     * The map will be used when subscriber disconnection is called where the corresponding topic needs to be identified
     * the underlying protocol engine only provides the disconnected client id.
     * The key of the map will be the client id and the value will be the topic
     * We use the Hash-map here since there cannot have an occurrence of subscriber binding and disconnecting in parallel
     */
    private Map<String, String> subscriberTopicCorrelate = new HashMap<String, String>();
    /**
     * The channel reference which will be used to interact with the Andes Kernal
     */
    private MQTTConnector connector = new DistributedStoreConnector();

    /**
     * The class will be declared as singleton since the state will be centralized
     */
    private MQTTopicManager() {
    }

    /**
     * All centralized instance will be taken for
     *
     * @return the instance which will hold all relevant topic information
     */
    public static MQTTopicManager getInstance() {
        return instance;
    }

    /**
     * Will initialize the bridge which will connect with the MQTT protocol handler
     *
     * @param mqttAndesConnection connection
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
     * Adds a topic message to the cluster
     *
     * @param messageContext Which will hold message information
     * @throws MQTTException
     * @see org.wso2.andes.mqtt.MQTTMessageContext
     */
    public void addTopicMessage(MQTTMessageContext messageContext) throws MQTTException {

        if (log.isDebugEnabled()) {
            log.debug("Incoming message received with id : " + messageContext.getMqttLocalMessageID() + ", QOS level : "
                    + messageContext.getQosLevel() + ", for topic :" + messageContext.getTopic() + ", with retain :" +
                    messageContext.isRetain());
        }
        //Will add the topic message to the cluster for distribution
        try {
            connector.addMessage(messageContext);
        } catch (MQTTException e) {
            //Will need to rollback the state
            final String error = "Error occurred while publishing the message";
            log.error(error, e);
            throw e;
        }

    }

    /**
     * Will include the topic subscription to the list of topics maintained.
     *
     * @param topicName           the name of the topic the subscription is being registered for
     * @param mqttClientChannelID the channel identity of the subscriber maintained by the protocol reference
     * @param qos                 the level of QOS the message subscription was created the value can be wither 0,1 or 2
     * @param isCleanSession      indicates whether the subscription should be durable or not
     * @throws MQTTException if the subscriber addition was not successful
     * @see org.dna.mqtt.wso2.AndesMQTTBridge.QOSLevel
     */
    public void addTopicSubscription(String topicName, String mqttClientChannelID, QOSLevel qos,
                                     boolean isCleanSession) throws MQTTException {
        //Will extract out the topic information if the topic is created already
        MQTTopic topic = topics.get(topicName);
        String subscriptionID = null;
        //Will generate a unique identifier for the subscription
        final UUID subscriptionChannelID = MQTTUtils.generateSubscriptionChannelID(mqttClientChannelID, topicName,
                qos.getQosValue(), isCleanSession);
        //If the topic has not being created before
        if (null == topic) {
            //First the topic should be registered in the cluster
            //Once the cluster registration is successful the topic will be created
            topic = new MQTTopic(topicName);
            //Will set the topic specific subscription id generated
            topics.put(topicName, topic);

        } else {
            if (log.isDebugEnabled()) {
                log.debug("The topic " + topic + "has local subscriptions already");
            }
        }
        //Will add the subscription to the topic
        //The status will be false if the subscriber with the same channel id exists
        try {
            // Register the the topic subscription for the topic
            subscriberTopicCorrelate.put(mqttClientChannelID, topicName);

            //First the topic should be registered in the cluster
            subscriptionID = registerTopicSubscriptionInCluster(topicName, mqttClientChannelID, isCleanSession,
                    qos, subscriptionChannelID);
            topic.addSubscriber(mqttClientChannelID, qos, isCleanSession, subscriptionID, subscriptionChannelID);
            //Finally will register the the topic subscription for the topic
            subscriberTopicCorrelate.put(mqttClientChannelID, topicName);
        } catch (SubscriptionAlreadyExistsException ignore) {
            //We do not throw this any further, the process should not stop due to this
            final String message = "Error while adding the subscriber to the cluster";
            log.error(message, ignore);
        } catch (MQTTException ex) {
            //In case if an error occurs we need to rollback the subscription created cluster wide
            connector.removeSubscriber(this, topicName, subscriptionID, subscriptionChannelID,
                    isCleanSession, mqttClientChannelID);
            final String message = "Error while adding the subscriber to the cluster";
            log.error(message, ex);
            throw ex;
        }
    }

    /**
     * Will be called during the event where the subscriber disconnection or un-subscription is triggered
     *
     * @param mqttClientChannelID the id of the channel which the subscriber is bound to
     * @param action              describes whether its a disconnection or an un-subscription
     * @throws MQTTException occurs if the subscriber was not disconnected properly
     */
    public void removeOrDisconnectTopicSubscription(String mqttClientChannelID, SubscriptionEvent action)
            throws MQTTException {
        //First the topic name will be taken from the subscriber channel
        String topic = subscriberTopicCorrelate.get(mqttClientChannelID);
        //If the topic has correlations
        if (null != topic) {
            //Will get the corresponding topic
            MQTTopic mqttTopic = topics.get(topic);

            if (null != mqttTopic) {
                //Will remove the subscription entity
                MQTTSubscriber subscriber = mqttTopic.removeSubscriber(mqttClientChannelID);
                String subscriberChannelID = subscriber.getSubscriberChannelID();
                UUID subscriberChannel = subscriber.getSubscriptionChannel();
                boolean isCleanSession = subscriber.isCleanSession();
                //The corresponding subscription created cluster wide will be topic name and the local channel id
                //Will remove the subscriber cluster wide
                try {
                    //Will indicate the disconnection of the topic
                    if (action == SubscriptionEvent.DISCONNECT && !(subscriber.getQOSLevel() == QOSLevel.AT_MOST_ONCE
                            && !subscriber.isCleanSession())) {
                        connector.disconnectSubscriber(this, topic, subscriberChannelID, subscriberChannel,
                                isCleanSession, mqttClientChannelID);
                    } else {
                        //If un-subscribed we need to remove the subscription off
                        connector.removeSubscriber(this, topic, subscriberChannelID, subscriberChannel,
                                isCleanSession, mqttClientChannelID);
                    }
                    if (log.isDebugEnabled()) {
                        final String message = "Subscription with cluster id " + subscriberChannelID + " disconnected " +
                                "from topic " + topic;
                        log.debug(message);
                    }

                    // Remove the topic mapping
                    subscriberTopicCorrelate.remove(mqttClientChannelID);
                } catch (MQTTException ex) {
                    //Should re state the connection of the subscriber back to the map
                    mqttTopic.addSubscriber(mqttClientChannelID, subscriber);
                    final String error = "Error occurred while removing the subscription " + mqttClientChannelID;
                    log.error(error, ex);
                    throw ex;
                }

            } else {
                final String message = "Error unidentified topic found for client id " + mqttClientChannelID;
                throw new MQTTException(message);
            }

        } else {
            //If the connection is publisher based
            UUID publisherID = connector.removePublisher(mqttClientChannelID);
            if (null == publisherID) {
                log.warn("A subscriber or a publisher with Connection with id " + mqttClientChannelID + " cannot be " +
                        "found to disconnect.");
            }
        }
    }

    /**
     * Will notify to the subscribers who are bound to the topic
     *
     * @param storageName   the topic the message was published that will be stored
     * @param message       the message content
     * @param messageID     the identifier of the message
     * @param publishedQOS  the level of qos the message was published
     * @param shouldRetain  whether the message should retain after it was published
     * @param subscriberQOS the level of QOS of the subscription
     * @throws MQTTException during a failure to deliver the message to the subscribers
     */
    public void distributeMessageToSubscriber(String storageName, ByteBuffer message, long messageID, int publishedQOS,
                                              boolean shouldRetain, String channelID, int subscriberQOS)
            throws MQTTException {
        //Will generate a unique id, cannot force MQTT to have a long as the message id since the protocol looks for
        //unsigned short
        int mqttLocalMessageID = 1;
        //Should get the topic name from the channel id
        String topic = subscriberTopicCorrelate.get(channelID);
        //We need to keep track of the message if the QOS level is > 0
        if (subscriberQOS > QOSLevel.AT_MOST_ONCE.getQosValue()) {
            //We need to add the message information to maintain state, in-order to identify the messages
            // once the acks receive
            MQTTopic mqttopic = topics.get(topic);
            MQTTSubscriber mqttSubscriber = mqttopic.getSubscription(channelID);
            //There could be a situation where the message was published, but before it arrived to the subscription
            //The subscriber has disconnected at a situation as such we have to indicate the disconnection
            if (null != mqttSubscriber) {
                //Will mark the message as sent to subscribers
                mqttLocalMessageID = mqttSubscriber.markSend(messageID);
                //Will add the information that will be necessary to process once the acks arrive
                mqttSubscriber.setStorageIdentifier(storageName);
                //Subscriber state will not be handled for the case of QoS 0, hence if the subscription has disconnected it
                // will be handled from the protocol engine
                getBridgeInstance().distributeMessageToSubscriptions(topic, publishedQOS, message,
                        shouldRetain, mqttLocalMessageID, channelID);
            } else {
                throw new MQTTException("The subscriber with id " + channelID +
                        " has disconnected hence message will not be published to " + messageID);
            }
        } else {
            getBridgeInstance().distributeMessageToSubscriptions(topic, publishedQOS, message,
                    shouldRetain, mqttLocalMessageID, channelID);
        }
    }

    /**
     * Will trigger during the time where an ack was received for a message
     *
     * @param mqttChannelID the identifier of the channel
     * @param messageID     the message id on which the ack was received
     */
    public void onMessageAck(String mqttChannelID, int messageID) throws MQTTException {
        //Will retrieve the topic
        String topicName = subscriberTopicCorrelate.get(mqttChannelID);
        //Will retrieve the topic object out of the list
        MQTTopic mqttTopic = topics.get(topicName);
        //Will get the subscription object out of the topic
        MQTTSubscriber mqttSubscriber = mqttTopic.getSubscription(mqttChannelID);
        //Will indicate that the ack was received
        long clusterID = mqttSubscriber.ackReceived(messageID);
        //First we need to get the subscription information
        messageAck(topicName, clusterID, mqttSubscriber.getStorageIdentifier(), mqttSubscriber.getSubscriptionChannel());
    }

    /**
     * This will be called when simulating the ack for the server for QOS 0 messages
     *
     * @param topic        the name of the topic the ack will be simulated for
     * @param messageID    the id of the message the ack will be simulated for
     * @param storageName  the storage name representation
     * @param subChannelID the id of the subscription channel
     * @throws MQTTException occurs if the ack failed to be processed by the kernal
     */
    public void implicitAck(String topic, long messageID, String storageName, UUID subChannelID) throws MQTTException {
        messageAck(topic, messageID, storageName, subChannelID);
    }

    /**
     * Will interact with the kernal and will create a cluster wide indication of the topic
     *
     * @param topicName             the name of the topic which should be registered in the cluster
     * @param mqttClientID          the subscriber id which is local to the node
     * @param isCleanSession        should the subscription be identified as durable
     * @param qos                   the subscriber level qos
     * @param subscriptionChannelID the unique identifier of the subscription channel
     * @return topic subscription id which will represent the topic in the cluster
     */
    private String registerTopicSubscriptionInCluster(String topicName, String mqttClientID, boolean isCleanSession,
                                                      QOSLevel qos, UUID subscriptionChannelID)
            throws MQTTException, SubscriptionAlreadyExistsException {
        //Will generate a unique id for the client
        //Per topic only one subscription will be created across the cluster
        String topicSpecificClientID = MQTTUtils.generateTopicSpecficClientID(mqttClientID);
        if (log.isDebugEnabled()) {
            log.debug("Cluster wide topic connection was created with id " + topicSpecificClientID + " for topic " +
                    topicName + " with clean session " + isCleanSession);
        }

        //Will register the topic cluster wide
        connector.addSubscriber(this, topicName, topicSpecificClientID, mqttClientID, isCleanSession,
                qos, subscriptionChannelID);

        return topicSpecificClientID;
    }

    /**
     * When acknowledgments arrive for the delivered messages this method will be called
     *
     * @param topic       the name of the topic the ack was received
     * @param messageID   the id of the message the ack was received
     * @param storageName the name of the representation of the topic in the store
     * @throws MQTTException at an event where the ack was not properly processed
     */
    private void messageAck(String topic, long messageID, String storageName, UUID subChannelID) throws MQTTException {
        try {
            connector.messageAck(messageID, topic, storageName, subChannelID);
        } catch (AndesException ex) {
            final String message = "Error occurred while cleaning up the acked message";
            log.error(message, ex);
            throw new MQTTException(message, ex);
        }
    }

}
