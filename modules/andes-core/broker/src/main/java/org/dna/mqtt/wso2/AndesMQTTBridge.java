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
import org.wso2.andes.mqtt.MQTTException;
import org.wso2.andes.mqtt.MQTTUtils;
import org.wso2.andes.mqtt.MQTTopicManager;

import java.nio.ByteBuffer;


/**
 * The class will be resposible to mediate between the MQTT library and the Andes kernal.
 * When writing methods all the connecting logic between the MQTT protocol engine and kernal
 * should go through this class
 * This way a clear abstraction could be maintained between MQTT protocol class and the logic
 * Each function in the class should represent a state, ex :- register subscriber, publish message, unsubscribe
 */

public final class AndesMQTTBridge {

    //Will log the messages generated through the class
    private static Log log = LogFactory.getLog(AndesMQTTBridge.class);
    //The connection between the MQTT library
    private static ProtocolProcessor mqttProtocolHandlingEngine = null;
    //The Andes bridge instance
    private static AndesMQTTBridge instance = new AndesMQTTBridge();


    /**
     * The class will be delcared as singleton since only one instance of this should be created on the JVM
     * We cannot define multiple bridge instances since all the state between the topics will be maintained here
     */
    private AndesMQTTBridge() {
    }

    /**
     * Will handle processing the protocol specifc details on MQTT
     *
     * @param mqttProtocolProcessor the reference to the protocol processing object
     */
    public static void initMQTTProtocolProcessor(ProtocolProcessor mqttProtocolProcessor) throws Exception {
        mqttProtocolHandlingEngine = mqttProtocolProcessor;
        //Also we initialize the topic manager instance
        MQTTopicManager.getInstance().initProtocolEngine(instance);
    }

    /**
     * Will return the object which contains the MQTT protocol instance
     *
     * @return The bridge instance that will allow connectivity between the kernal and mqtt protocol
     */
    public static AndesMQTTBridge getBridgeInstance() throws Exception {
        if (mqttProtocolHandlingEngine != null) {
            return instance;
        } else {
            //Will capture the exception here and will not throw it any further
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
        try {
            MQTTopicManager.getInstance().removeTopicSubscription(mqttClientChannelID);
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
     * @param mqttLocalMessageID the message uniq identifier
     */
    public static void onMessagePublished(String topic, int qosLevel, ByteBuffer message, boolean retain,
                                          int mqttLocalMessageID) {
        try {
            MQTTopicManager.getInstance().addTopicMessage(topic, qosLevel, message, retain, mqttLocalMessageID);
        } catch (MQTTException e) {
            //Will capture the message here and will not throw it further to mqtt protocol
            final String error = "Error occured while adding the message content for message id : "
                    + mqttLocalMessageID;
            log.error(error, e);
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
    public void onTopicSubscription(String topic, String mqttClientChannelID, AbstractMessage.QOSType qos,
                                    boolean isCleanSession) {
        try {
            MQTTopicManager.getInstance().addTopicSubscription(topic,
                    mqttClientChannelID, MQTTUtils.convertMQTTProtocolTypeToInteger(qos), isCleanSession);
        } catch (MQTTException e) {
            //Will not thow the exception further since the bridge will handle the exceptions in both the relams
            final String message = "Error occured while subscription is initiated for topic : " + topic +
                    " and session id :" + mqttClientChannelID;
            log.error(message, e);
        }
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
    public void notifySubscriptions(String topic, int qos, ByteBuffer message, boolean retain, long messageID,String channelID) {
        final int andesMessageID = (int) messageID;

        if (mqttProtocolHandlingEngine != null) {
            //Need to set do a re possition of bytes for writing to the buffer
            //Since the buffer needs to be initialized for reading before sending out
            final int bytesPossition = 0;
            message.position(bytesPossition);
            AbstractMessage.QOSType qosType = MQTTUtils.getMQTTQOSTypeFromInteger(qos);
           // mqttProtocolHandlingEngine.publish2Subscribers(topic, qosType, message, retain, andesMessageID);
            mqttProtocolHandlingEngine.publishToSubscriber(topic,qosType,message,retain,andesMessageID,channelID);
            if (log.isDebugEnabled()) {
                log.debug("The message with id " + messageID + " for topic " + topic +
                        " was notified to its subscribers");
            }

        } else {
            //Will capture the exception here and will not throw it any further
            final String error = "The reference to the MQTT protocol has not being initialized, " +
                    "an attmpt was made to deliver message ";
            log.error(error + messageID);
        }
    }
}
