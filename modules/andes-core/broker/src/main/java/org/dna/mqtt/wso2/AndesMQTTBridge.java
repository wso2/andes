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
package org.dna.mqtt.wso2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dna.mqtt.moquette.messaging.spi.impl.ProtocolProcessor;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.wso2.andes.kernel.distruptor.inbound.PubAckHandler;
import org.wso2.andes.mqtt.MQTTException;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.mqtt.MQTTopicManager;

import java.nio.ByteBuffer;


/**
 * The class will be responsible to mediate between the MQTT library and the Andes kernal.
 * When writing methods all the connecting logic between the MQTT protocol engine and kernal
 * should go through this class
 * This way a clear abstraction could be maintained between MQTT protocol class and the logic
 * Each function in the class should represent a state, ex :- register subscriber, publish message, un-subscribe
 */

public final class AndesMQTTBridge {

    //Will log the messages generated through the class
    private static Log log = LogFactory.getLog(AndesMQTTBridge.class);
    //The connection between the MQTT library
    private static ProtocolProcessor mqttProtocolHandlingEngine = null;
    //The Andes bridge instance
    private static AndesMQTTBridge instance = new AndesMQTTBridge();

    //Will define the different states the connection could have
    public enum SubscriptionEvent{
        DISCONNECT,UNSUBSCRIBE
    }

    /**
     * The class will be declared as singleton since only one instance of this should be created on the JVM
     * We cannot define multiple bridge instances since all the state between the topics will be maintained here
     */
    private AndesMQTTBridge() {
    }

    /**
     * Will handle processing the protocol specific details on MQTT
     *
     * @param mqttProtocolProcessor the reference to the protocol processing object
     */
    public static void initMQTTProtocolProcessor(ProtocolProcessor mqttProtocolProcessor) throws MQTTException {
        mqttProtocolHandlingEngine = mqttProtocolProcessor;
        //Also we initialize the topic manager instance
        MQTTopicManager.getInstance().initProtocolEngine(instance);
    }

    /**
     * Will return the object which contains the MQTT protocol instance
     *
     * @return The bridge instance that will allow connectivity between the kernal and mqtt protocol
     */
    public static AndesMQTTBridge getBridgeInstance() throws MQTTException {
        if (null != mqttProtocolHandlingEngine) {
            return instance;
        } else {
            //Will capture the exception here and will not throw it any further
            final String message = "MQTT protocol reference has not being initialized, cannot establish connectivity";
            log.error(message);
            throw (new MQTTException(message));
        }
    }

    /**
     * Will remove the subscribers once disconnection call is being triggered
     *
     * @param mqttClientChannelID the id of the client(subscriber) who requires disconnection
     */
    public void onSubscriberDisconnection(String mqttClientChannelID,SubscriptionEvent event) {
        try {
            MQTTopicManager.getInstance().removeOrDisconnectTopicSubscription(mqttClientChannelID,event);
        } catch (MQTTException e) {
            //Will capture the exception here and will not throw it any further
            final String message = "Error while disconnecting the subscription with the id " + mqttClientChannelID;
            log.error(message, e);
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
     * @param mqttLocalMessageID the message unique identifier
     * @param publisherID        the id of the publisher provided by mqtt protocol
     * @param pubAckHandler      publisher acknowledgements are handled by this handler
     */
    public static void onMessagePublished(String topic, int qosLevel, ByteBuffer message, boolean retain,
                                          int mqttLocalMessageID, String publisherID, PubAckHandler pubAckHandler) {
        try {
            MQTTopicManager.getInstance().addTopicMessage(
                    topic, qosLevel, message, retain, mqttLocalMessageID, publisherID, pubAckHandler);
        } catch (MQTTException e) {
            //Will capture the message here and will not throw it further to mqtt protocol
            final String error = "Error occurred while adding the message content for message id : "
                    + mqttLocalMessageID;
            log.error(error, e);
        }
    }

    /**
     * This will be triggered each time a subscriber subscribes to a topic, when connecting with Andes
     * only one subscription will be indicated per node
     * just to ensure that cluster wide the subscriptions are visible.
     * The message delivery to the subscribers will be managed through the respective channel
     *
     * @param topic               the name of the topic the subscribed to
     * @param mqttClientChannelID the client identification maintained by the MQTT protocol lib
     * @param qos                 the type of qos the subscription is connected to this can be either MOST_ONE,LEAST_ONE, EXACTLY_ONE
     * @param isCleanSession      whether the subscription is durable
     */
    public void onTopicSubscription(String topic, String mqttClientChannelID, AbstractMessage.QOSType qos,
                                    boolean isCleanSession) {
        try {
            MQTTopicManager.getInstance().addTopicSubscription(topic,
                    mqttClientChannelID, MQTTUtils.convertMQTTProtocolTypeToInteger(qos), isCleanSession);
        } catch (MQTTException e) {
            //Will not throw the exception further since the bridge will handle the exceptions in both the realm
            final String message = "Error occurred while subscription is initiated for topic : " + topic +
                    " and session id :" + mqttClientChannelID;
            log.error(message, e);
        }
    }

    /**
     * Will trigger at an event where a message was published and an ack being received for the published message
     *
     * @param mqttClientChannelID the id of the channel where the message was published
     * @param messageID           the id of the message
     */
    public void onAckReceived(String mqttClientChannelID, int messageID) throws MQTTException {
        if (log.isDebugEnabled()) {
            log.debug("Message ack received for message with id " + messageID + " and subscription " +
                    mqttClientChannelID);
        }
        try {
            MQTTopicManager.getInstance().onMessageAck(mqttClientChannelID, messageID);
        } catch (MQTTException e) {
            final String message = "Error occurred while the subscription ack was received for channel "
                    + mqttClientChannelID + " and for message " + messageID;
            log.error(message, e);
            throw e;
        }
    }

    /**
     * When a message is sent the notification to the subscriber channels managed by the MQTT library will be notified
     *
     * @param topic     the topic of the message that the subscribers should be notified of
     * @param qos       the level of QOS the message was subscribed to
     * @param message   the content of the message
     * @param retain    should this message be persisted
     * @param channelID the unique id of the subscription created by the protocol
     * @param messageID the identity of the message
     */
    public void distributeMessageToSubscriptions(String topic, int qos, ByteBuffer message, boolean retain,
                                                 int messageID, String channelID) {

        if (null != mqttProtocolHandlingEngine) {
            //Need to set do a re position of bytes for writing to the buffer
            //Since the buffer needs to be initialized for reading before sending out
            final int bytesPosition = 0;
            message.position(bytesPosition);
            AbstractMessage.QOSType qosType = MQTTUtils.getMQTTQOSTypeFromInteger(qos);
            // mqttProtocolHandlingEngine.publish2Subscribers(topic, qosType, message, retain, andesMessageID);
            mqttProtocolHandlingEngine.publishToSubscriber(topic, qosType, message, retain, messageID, channelID);
            if (log.isDebugEnabled()) {
                log.debug("The message with id " + messageID + " for topic " + topic +
                        " was notified to its subscribers");
            }

        } else {
            //Will capture the exception here and will not throw it any further
            final String error = "The reference to the MQTT protocol has not being initialized, " +
                    "an attempt was made to deliver message ";
            log.error(error + messageID);
        }
    }
}
