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
package org.wso2.andes.mqtt.utils;

import io.netty.channel.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.SubscriptionsStore;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.wso2.QOSLevel;
import org.wso2.andes.kernel.AndesContent;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.disruptor.inbound.PubAckHandler;
import org.wso2.andes.mqtt.MQTTMessageContext;
import org.wso2.andes.mqtt.MQTTPublisherChannel;
import org.wso2.andes.server.store.MessageMetaDataType;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * This class will contain operations such as conversion of message which are taken from the protocol to be compliant
 * with the objects expected by Andes, message id generation, construction of meta information,
 */
public class MQTTUtils {

    private static Log log = LogFactory.getLog(MQTTUtils.class);
    public static final String MESSAGE_ID = "MessageID";
    public static final String MQTT_EXCHANGE_NAME = "mqtt.topic";
    private static final String TOPIC = "Topic";
    public static final String ARRIVAL_TIME = "ArrivalTime";
    private static final String DESTINATION = "Destination";
    private static final String PERSISTENCE = "Persistent";
    private static final String MESSAGE_CONTENT_LENGTH = "MessageContentLength";
    public static final String QOSLEVEL = "QOSLevel";
    public static final String IS_COMPRESSED = "IsCompressed";
    //This will be required to be at the initial byte stream the meta data will have since when the message is processed
    //back from andes since the message relevancy is checked ex :- whether its amqp, mqtt etc
    public static final String MQTT_META_INFO = "\u0002MQTT Protocol v3.1";

    public static final String SINGLE_LEVEL_WILDCARD = "+";
    public static final String MULTI_LEVEL_WILDCARD = "#";

    public static final String DEFAULT_ANDES_CHANNEL_IDENTIFIER = "MQTT-Unknown";
    /**
     * MQTT Publisher ID
     */
    public static final String CLIENT_ID = "clientID";

    /**
     * The published messages will be taken in as a byte stream, the message will be transformed into AndesMessagePart as
     * its required by the Andes kernal for processing
     *
     * @param message  the message contents
     * @param messagID the message identifier
     * @return AndesMessagePart which wraps the message into a Andes kernal compliant object
     */
    public static AndesMessagePart convertToAndesMessage(byte[] message, long messagID) {
        AndesMessagePart messageBody = new AndesMessagePart();
        messageBody.setOffSet(0); //Here we set the offset to 0, but it will be a problem when large messages are sent
        messageBody.setData(message);
        messageBody.setMessageID(messagID);
        return messageBody;
    }

    /**
     * The data about the message (meta information) will be constructed at this phase Andes requires the meta data as a
     * byte stream, The method basically collects all the relevant information necessary to construct the bytes stream
     * and will convert the message into a bytes object
     *
     * @param metaData      the information about the published message
     * @param messageID     the identity of the message
     * @param arrivalTime   the arrival time of the message
     * @param topic         the value to indicate if this is a topic or not
     * @param qos           the level of qos the message was published at
     * @param destination   the definition where the message should be sent to
     * @param persistence   should this message be persisted
     * @param contentLength the length of the message content
     * @param isCompressed  the value to indicate, if the message is compressed or not
     * @return the collective information as a bytes object
     */
    public static byte[] encodeMetaInfo(String metaData, long messageID, long arrivalTime, boolean topic, int qos,
                                        String destination, boolean persistence, int contentLength, boolean isCompressed) {
        byte[] metaInformation;
        String information = metaData + "?" + MESSAGE_ID + "=" + messageID + "," + ARRIVAL_TIME + "=" + arrivalTime
                + "," + TOPIC + "=" + topic + "," + DESTINATION + "=" + destination + "," + PERSISTENCE
                + "=" + persistence + "," + MESSAGE_CONTENT_LENGTH + "=" + contentLength + "," + QOSLEVEL + "=" + qos
                + "," + IS_COMPRESSED + "=" + isCompressed;
        metaInformation = information.getBytes();
        return metaInformation;
    }

    /**
     * Extract the message information and will generate the object which will be compliant with the Andes kernal
     *
     * @param messageID            message identification
     * @param topic                name of topic
     * @param qosLevel             the level of qos the message was published
     * @param messageContentLength the content length of the message
     * @param retain               should this message retain
     * @param publisher            the uuid which will uniquely identify the publisher
     * @param isCompressed         the value to indicate, if the message is compressed or not
     * @return the meta information compliant with the kernal
     */
    public static AndesMessageMetadata convertToAndesHeader(long messageID, String topic, int qosLevel,
                                                            int messageContentLength, boolean retain,
                                                            MQTTPublisherChannel publisher, boolean isCompressed) {
        long receivedTime = System.currentTimeMillis();

        AndesMessageMetadata messageHeader = new AndesMessageMetadata();
        messageHeader.setMessageID(messageID);
        messageHeader.setTopic(true);
        messageHeader.setMessageRouterName(MQTT_EXCHANGE_NAME);
        messageHeader.setDestination(topic);
        messageHeader.setPersistent(true);
        messageHeader.setRetain(retain);
        messageHeader.setMessageContentLength(messageContentLength);
        messageHeader.setStorageQueueName(topic);
        messageHeader.setMetaDataType(MessageMetaDataType.META_DATA_MQTT);
        messageHeader.setQosLevel(qosLevel);
        messageHeader.setCompressed(isCompressed);
        // message arrival time set to mb node's system time.
        messageHeader.setArrivalTime(receivedTime);
        if (log.isDebugEnabled()) {
            log.debug("Message with id " + messageID + " having the topic " + topic + " with QOS" + qosLevel
                    + " and retain flag set to " + retain + " was created");
        }

        byte[] andesMetaData = encodeMetaInfo(MQTT_META_INFO, messageHeader.getMessageID(), receivedTime,
                messageHeader.isTopic(), qosLevel, messageHeader.getDestination(), messageHeader.isPersistent(),
                messageContentLength, isCompressed);

        messageHeader.setMetadata(andesMetaData);
        return messageHeader;
    }

    /**
     * Will extract out the message content from the meta data object provided, this will be called when a published
     * message is distributed among the subscribers
     *
     * @param content Content object which has access to the message content
     * @return the byte stream of the message
     */
    public static ByteBuffer getContentFromMetaInformation(AndesContent content)
            throws AndesException {
        ByteBuffer message = ByteBuffer.allocate(content.getContentLength());

        try {
            //offset value will always be set to 0 since mqtt doesn't support chunking the messages, always the message
            //will be in the first chunk but in AMQP there will be chunks
            final int mqttOffset = 0;
            content.putContent(mqttOffset, message);
        } catch (AndesException e) {
            final String errorMessage = "Error in getting content for message";
            log.error(errorMessage, e);
            throw new AndesException(errorMessage, e);
        }
        return message;
    }

    /**
     * Will convert between the types of the QOS to adhere to the conversion of both andes and mqtt protocol
     *
     * @param qos the quality of service level the message should be published/subscribed
     * @return the level which is compliment by the mqtt library
     */
    public static AbstractMessage.QOSType getQOSType(int qos) {
        return AbstractMessage.QOSType.valueOf(qos);
    }


    /**
     * Check if a subscribed queue bound destination routing key matches with a given message routing key using MQTT
     * wildcards.
     *
     * @param queueBoundRoutingKey The subscribed destination with/without wildcards
     * @param messageRoutingKey    The message destination routing key without wildcards
     * @return Is queue bound routing key match the message routing key
     */
    public static boolean isTargetQueueBoundByMatchingToRoutingKey(String queueBoundRoutingKey,
                                                                   String messageRoutingKey) {
        return SubscriptionsStore.matchTopics(messageRoutingKey, queueBoundRoutingKey);
    }

    /**
     * Checks whether a given subscription is a wildcard subscription.
     *
     * @param subscribedDestination The destination string subscriber subscribed to
     * @return is this a wild card subscription
     */
    public static boolean isWildCardSubscription(String subscribedDestination) {
        boolean isWildCard = false;

        if (subscribedDestination.contains(SINGLE_LEVEL_WILDCARD) || subscribedDestination.contains(MULTI_LEVEL_WILDCARD)) {
            isWildCard = true;
        }

        return isWildCard;
    }

    /**
     * Generate a unique UUID for a given client who has subscribed to a given topic with given qos and given clean
     * session.
     *
     * @param clientId     The MQTT client Id
     * @param topic        The topic subscribed to
     * @param qos          The Quality of Service level subscribed to
     * @param cleanSession Clean session value of the client
     * @return A unique UUID for the given arguments
     */
    public static UUID generateSubscriptionChannelID(String clientId, String topic, int qos, boolean cleanSession) {
        return UUID.nameUUIDFromBytes((clientId + topic + qos + cleanSession).getBytes());
    }

    /**
     * Creates the message context based on the values obtained from the protocol engine
     *
     * @param topic              the name of the topic the message was sent
     * @param qosLevel           the level of QoS
     * @see org.dna.mqtt.wso2.QOSLevel
     * @param message            the message in bytes
     * @param retain             should this message be persisted
     * @param mqttLocalMessageID the local id of the mqtt message
     * @param publisherID        the id of the publisher
     * @param pubAckHandler      the acknowledgment handler
     * @param socket             the channel in which the communication occurred
     * @return the message context
     */
    public static MQTTMessageContext createMessageContext(String topic, QOSLevel qosLevel,
                                                          ByteBuffer message, boolean retain,
                                                          int mqttLocalMessageID, String publisherID,
                                                          PubAckHandler pubAckHandler, Channel socket) {
        MQTTMessageContext messageContext = new MQTTMessageContext();
        messageContext.setTopic(topic);
        messageContext.setQosLevel(qosLevel);
        messageContext.setMessage(message);
        messageContext.setRetain(retain);
        messageContext.setMqttLocalMessageID(mqttLocalMessageID);
        messageContext.setPublisherID(publisherID);
        messageContext.setPubAckHandler(pubAckHandler);
        messageContext.setChannel(socket);

        return messageContext;
    }

    /**
     * Extract tenant name from a given topic.
     *
     * @param topic The topic to retrieve tenant name
     * @return Tenant name extracted from topic
     */
    public static String getTenantFromTopic(String topic) {
        String tenant = "";
//                MultitenantConstants.SUPER_TENANT_DOMAIN_NAME;
//
//        if (null != topic && topic.contains(AndesConstants.TENANT_SEPARATOR)) {
//            tenant = topic.split(AndesConstants.TENANT_SEPARATOR)[0];
//        }

        return tenant;
    }

    /**
     * Given the clean session value and qos level, decide whether it if falling into durable path.
     *
     * @param cleanSession The clean session value
     * @param qos The quality of service level
     *
     * @return true if this falls into durable path
     */
    public static boolean isDurable(boolean cleanSession, int qos) {
        boolean durable = false;

        if (!cleanSession && qos > 0) {
            durable = true;
        }

        return durable;
    }

}
