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
package org.dna.mqtt.wso2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dna.mqtt.moquette.messaging.spi.impl.ProtocolProcessor;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.wso2.andes.mqtt.MQTTChannel;
import org.wso2.andes.mqtt.MQTTException;
import org.wso2.andes.mqtt.MQTTUtils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The class will be resposible to mediate between the MQTT library and the Andes kernal.
 * When writing methods all the connecting logic between the MQTT protocol engine and kernal
 * should go through this class
 * This way a clear abstraction could be maintained between MQTT protocol class and the logic
 * Each function in the class should represent a state, ex :- register subscriber, publish message, unsubscribe
 */

public final class AndesMQTTBridge {

    private static Log log = LogFactory.getLog(AndesMQTTBridge.class);
    //The connection between the MQTT library
    private static ProtocolProcessor mqttProtocolHandlingReference = null;
    //The Andes bridge instance
    private static AndesMQTTBridge instance = new AndesMQTTBridge();
    //Will manage the relation between each topic with andes connection
    //Map will only contain local information and does not require it to be ditributed
    //The relation will be a one-one mapping where the key would be the name of the topic
    //Value would be a unique identification number for that topic
    private Map<String, String> topics = new ConcurrentHashMap<String, String>();
    //Will manage the releationship between each subscriber to topic
    //This map doen not required to be disributed since it will hold data to manage between local information
    // Each client subscirbed will have its own identifiyer, the following will maintain clientID <-> topic relation
    private Map<String, String> clientTopicRelation = new ConcurrentHashMap<String, String>();

    /**
     * The class will be delcared as singleton since only one instance of this should be created on the JVM
     * We cannot define multiple bridge instances since all the state between the topics will be maintained here
     */
    private AndesMQTTBridge() {
    }

    /**
     * Will handle processing the protocol specifc details on MQTT     *
     *
     * @param mqttProtocolProcessor the reference to the protocol processing object
     */
    public static void initMQTTProtocolProcessor(ProtocolProcessor mqttProtocolProcessor) throws Exception {
        if (instance == null) {
            mqttProtocolHandlingReference = mqttProtocolProcessor;
        } else {
            final String error = "Attempting to initialize the protocol engine, an instance has already being " +
                    "initialized";
            log.error(error);
            throw new Exception(error);
        }
    }

    /**
     * Will return the object which contains the MQTT protocol instance
     *
     * @return The bridge instance that will allow connectivity between the kernal and mqtt protocol
     */
    public static AndesMQTTBridge getBridgeInstance() throws Exception {
        if (mqttProtocolHandlingReference != null) {
            return instance;
        } else {
            final String message = "MQTT protocol reference has not being initialized, cannot establish connectivity";
            log.error(message);
            throw (new Exception(message));
        }
    }

    /**
     * Will remove the subscribers once disconnection call is being triggered
     *
     * @param mqttClientChannelID the id of the client(subscriber) who requires disconnection
     */
    public void onSubscriberDisconnection(String mqttClientChannelID) {

        if (clientTopicRelation.containsKey(mqttClientChannelID)) {
            //Will get the relavent topic the subscriber has registered to
            String subscribedTopic = clientTopicRelation.get(mqttClientChannelID);
            clientTopicRelation.remove(mqttClientChannelID);
            if (log.isDebugEnabled()) {
                final String message = "The client with channel ID " + mqttClientChannelID + " disconnected " +
                        "from topic " + subscribedTopic;
                log.debug(message);
            }
            //Per topic there will only be a single conneciton
            //Need to ensure that all the connections are removed to totally disconnect from topic
            //After removal if there's no indication of the topic existance we could remove
            //If there's indication that a purticular topic still has client relations we cannot disconnect
            if (!clientTopicRelation.containsValue(subscribedTopic)) {
                //No relation to the topic means no connections
                if (topics.containsKey(subscribedTopic)) {
                    //Will get the actual subscriber connection
                    String bridgingClientID = topics.get(subscribedTopic);
                    try {
                        MQTTChannel.getInstance().removeSubscriber(this, subscribedTopic, bridgingClientID);
                    } catch (MQTTException e) {
                        log.error("Error occured while removing the subscriber from topic "+
                                subscribedTopic+" "+e.getMessage());
                        //will be logging the exception here and will not be throwing it further since there should be
                        //an abstraction maintained
                    }
                    //Finally remove the entry from the list
                    topics.remove(subscribedTopic);
                    final String message = "All subscribers from topic :" + subscribedTopic + " have being " +
                            "disconnected";
                    log.info(message);
                } else {
                    final String message = "The topic : " + subscribedTopic + " has client relations but the topic " +
                            "is not registered with the kernal";
                    log.warn(message);
                }

            } else {
                if (log.isDebugEnabled()) {
                    final String message = "Topic : " + subscribedTopic + " had more connections, hence will not " +
                            "disconnect cluster wide";
                    log.debug(message);
                }
            }

        } else {
            final String message = "An unknown subscriber with id :" + mqttClientChannelID + " was set for " +
                    "disconnection, " +
                    "probaly " +
                    "due to invalid cache";
            log.warn(message);
        }
    }

    /**
     * Will provide the information from the MQTT library to andes for cluster wide representation
     * This method will be called when a message is published
     *
     * @param topic              the name of the topic the message is published to
     * @param qosLevel           the level of qos expected through the subscribers
     * @param message            the content of the message
     * @param retain             should this message be persisted
     * @param mqttLocalMessageID the message uniq identifier
     */
    public static void onMessagePublished(String topic, int qosLevel, ByteBuffer message, boolean retain,
                                          int mqttLocalMessageID) {
        //Will generate a unique id for the message which will be unique across the cluster
        long clusterSpecificMessageID = MQTTUtils.generateMessageID();
        if (log.isDebugEnabled()) {
            log.debug("Incoming message recived with id : " + mqttLocalMessageID + ", QOS level : " + qosLevel
                    + ", for topic :" + topic + ", with retain :" + retain);
            log.debug("Generated message cluster specific message id " + clusterSpecificMessageID +
                    " for mqtt local message id " + mqttLocalMessageID);
        }
        //Will add the message content to the andes kernal
        try {
            MQTTChannel.getInstance().addMessageContent(message, clusterSpecificMessageID, topic, qosLevel,
                    mqttLocalMessageID, retain);
        } catch (MQTTException e) {
            //Will not be throwing the exception any further since need to
            //maintain an abstaction between the mqqt protocol
            final String exceptionMessae = "Error occred while adding message content ";
            log.error(exceptionMessae +e.getMessage());
        }
    }

    /**
     * This will be triggered each time a subscirber subscribes to a topic, when connecting with Andes
     * only one subscription will be indicated per node
     * just to ensure that cluster wide the subscriptions are visible.
     * The message delivery to the subscibers will be managed through the respective channel
     *
     * @param topic               the name of the topic the subscribed to
     * @param mqttClientChannelID the client identification maintained by the MQTT protocol lib
     */
    public void onTopicSubscription(String topic, String mqttClientChannelID) {
        //TODO need to addres this mechanism when dealing with topic hierarchies
        if (!topics.containsKey(topic)) {
            //Will generate a unique id for the client
            //Per topic there will only be one visible client connection with the Andes Kernal
            String topicSpecificClientID = MQTTUtils.generateTopicSpecficClientID();
            if (log.isDebugEnabled()) {
                log.debug("Cluster wide topic connection was created with id " + topicSpecificClientID + " for topic " +
                        topic);
            }
            try {
                MQTTChannel.getInstance().addSubscriber(this, topic, topicSpecificClientID);
            } catch (MQTTException e) {
                final String message = "Error while registering the subscription to the topic ";
                log.error(message +topic+e.getMessage());
            }
            topics.put(topic, topicSpecificClientID);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("The topic " + topic + " already is visible across the cluster, " +
                        "hence will not create a seperate connection");
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("The client with id " + mqttClientChannelID + " is registered for topic " + topic);
        }
        //Will maintain the relationship between the subscribers to topic
        clientTopicRelation.put(mqttClientChannelID, topic);
    }

    /**
     * When a message is sent the notification to the subscriber channels managed by the MQTT library will be notified
     *
     * @param topic     the topic of the message that the subscribers should be notified of
     * @param qos       the level of QOS the message was subscribed to
     * @param message   the content of the message
     * @param retain    should this message be persisted
     * @param messageID the identity of the message
     */
    public void notifySubscriptions(String topic, int qos, ByteBuffer message, boolean retain, long messageID) {
        final int andesMessageID = (int) messageID;

        if (mqttProtocolHandlingReference != null) {
            //Need to set do a re possition of bytes for writing to the buffer
            //Since the buffer needs to be initialized for reading before sending out
            //TODO check for a avaiable method to be used instead of using magic numbers
            message.position(0);
            AbstractMessage.QOSType qosType = MQTTUtils.getMQTTQOSTypeFromInteger(qos);
            mqttProtocolHandlingReference.publish2Subscribers(topic, qosType, message, retain, andesMessageID);
            if (log.isDebugEnabled()) {
                log.debug("The message with id " + messageID + " for topic " + topic +
                        " was notified to its subscribers");
            }

        } else {
            final String error = "The reference to the MQTT protocol has not being initialized, " +
                    "an attmpt was made to deliver message ";
            log.error(error + messageID);
        }
    }
}
