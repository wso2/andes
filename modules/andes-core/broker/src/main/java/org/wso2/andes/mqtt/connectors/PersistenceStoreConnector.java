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
package org.wso2.andes.mqtt.connectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dna.mqtt.wso2.QOSLevel;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.disruptor.inbound.InboundBindingEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundQueueEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundSubscriptionEvent;
import org.wso2.andes.kernel.disruptor.inbound.QueueInfo;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.kernel.subscription.SubscriberConnection;
import org.wso2.andes.mqtt.MQTTException;
import org.wso2.andes.mqtt.MQTTLocalSubscription;
import org.wso2.andes.mqtt.MQTTMessage;
import org.wso2.andes.mqtt.MQTTMessageContext;
import org.wso2.andes.mqtt.MQTTPublisherChannel;
import org.wso2.andes.mqtt.MQTTopicManager;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.server.ClusterResourceHolder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


/**
 * This class mainly focuses on negotiating the connections and exchanging data with the message store
 * The class will interface with the Andes kernel and will ensure that the information that's received from the bridge
 * is conforming to the data structure expected by the kernel, The basic operations done through this class will be
 * converting between the meta data and message content, indicate subscriptions and disconnections
 */
public class PersistenceStoreConnector implements MQTTConnector {

    private static Log log = LogFactory.getLog(PersistenceStoreConnector.class);

    /**
     * Will maintain the relation between the publisher client identifiers vs the id generated cluster wide
     * Key of the map would be the mqtt specific client id and the value would be the cluster uuid
     */
    //TODO state the usage of a hash-map instead of using concurrent hashmap
    private Map<String, MQTTPublisherChannel> publisherTopicCorrelate = new HashMap<>();

    /**
     * Will maintain retain message identification (message id + channel id) until ack received
     * by the subscriber.
     * Retain message acks will not handle in andes level.
     */
    private Set<String> retainMessageIdSet = new HashSet<>();

    /**
     * {@inheritDoc}
     */
    public void messageAck(long messageID, UUID channelID)
            throws AndesException {
        AndesAckData andesAckData = new AndesAckData(channelID, messageID);

        // Remove retain message ack upon receive from retain message metadata map
        if(retainMessageIdSet.contains(messageID + channelID.toString())) {
            retainMessageIdSet.remove(messageID + channelID.toString());
        } else {
            Andes.getInstance().ackReceived(andesAckData);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void messageNack(long messageId, UUID channelID) throws AndesException{
        Andes.getInstance().messageRejected(messageId, channelID, true, false);
    }

    /**
     * {@inheritDoc}
     */
    public void addMessage(MQTTMessageContext messageContext) throws MQTTException {
        if (messageContext.getMessage().hasArray()) {

            MQTTPublisherChannel publisher = publisherTopicCorrelate.get(messageContext.getPublisherID());
            if (null == publisher) {
                //We need to create a new publisher
                publisher = new MQTTPublisherChannel(messageContext.getChannel());
                publisherTopicCorrelate.put(messageContext.getPublisherID(), publisher);
                //Finally will register the publisher channel for flow controlling

                String andesChannelId = MQTTUtils.DEFAULT_ANDES_CHANNEL_IDENTIFIER;
                if (null != messageContext.getChannel()) {
                    andesChannelId = messageContext.getChannel().remoteAddress().toString().substring(1);
                }
                
                AndesChannel publisherChannel = null;
                try {
                    publisherChannel = Andes.getInstance().createChannel(andesChannelId, publisher);
                } catch (AndesException ex) {
                    throw new MQTTException("unable to create a new channel " , ex);
                }
 
                //Set channel details
                //Substring to remove leading slash character from remote address
                publisherChannel.setDestination(messageContext.getTopic());
                publisher.setChannel(publisherChannel);
            }

            //Will get the bytes of the message
            byte[] messageData = messageContext.getMessage().array();
            long messageID = 0L; // unique message Id will be generated By Andes.
            //Will start converting the message body
            AndesMessagePart messagePart = MQTTUtils.convertToAndesMessage(messageData, messageID);
            //Will Create the Andes Header
            AndesMessageMetadata messageHeader = MQTTUtils.convertToAndesHeader(messageID, messageContext.getTopic(),
                    messageContext.getQosLevel().getValue(), messageData.length, messageContext.isRetain(),
                    publisher, messageContext.isCompressed());

            // Add properties to be used for publisher acks
            messageHeader.addProperty(MQTTUtils.CLIENT_ID, messageContext.getPublisherID());
            messageHeader.addProperty(MQTTUtils.MESSAGE_ID, messageContext.getMqttLocalMessageID());
            messageHeader.addProperty(MQTTUtils.QOSLEVEL, messageContext.getQosLevel().getValue());

            // Publish to Andes core
            AndesMessage andesMessage = new MQTTMessage(messageHeader);
            andesMessage.addMessagePart(messagePart);
            Andes.getInstance().messageReceived(andesMessage, publisher.getChannel(), messageContext.getPubAckHandler());
            if (log.isDebugEnabled()) {
                log.debug(" Message added with message id " + messageContext.getMqttLocalMessageID());
            }

        } else {
            throw new MQTTException("Message content is not backed by an array, or the array is read-only.");
        }
    }

    /**
     * {@inheritDoc}
     */
    public void addSubscriber(MQTTopicManager channel, String topic, String clientID, String username,
                              boolean isCleanSession, QOSLevel qos, UUID subscriptionChannelID)
            throws MQTTException, SubscriptionAlreadyExistsException {

        //create a MQTTLocalSubscription wrapping underlying channel
        MQTTLocalSubscription mqttTopicSubscriber = createSubscription(topic, channel, clientID, qos.getValue(),
                subscriptionChannelID, true, isCleanSession);

        try {

            //We need to create a queue in-order to preserve messages relevant for the durable subscription
            String storageQueueName =
                            AndesUtils.getStorageQueueForDestination(topic,
                            MQTTUtils.MQTT_EXCHANGE_NAME,
                            topic,
                            mqttTopicSubscriber.isDurable());

            InboundQueueEvent createQueueEvent = new InboundQueueEvent(storageQueueName,
                    true, false, username, false);
            Andes.getInstance().createQueue(createQueueEvent);

            QueueInfo queueInfo = new QueueInfo(storageQueueName, mqttTopicSubscriber.isDurable(), false, username, false);
            InboundBindingEvent mqttBinding =
                    new InboundBindingEvent(queueInfo, MQTTUtils.MQTT_EXCHANGE_NAME, topic);
            Andes.getInstance().addBinding(mqttBinding);

            //Once the connection is created we register subscription
            AndesSubscription localSubscription = createLocalSubscription(mqttTopicSubscriber, topic, clientID);

            //create open subscription event
            String subscribedNode = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
            SubscriberConnection connection = new SubscriberConnection("127.0.0.1",
                    subscribedNode, mqttTopicSubscriber
                    .getChannelID(), mqttTopicSubscriber);

            InboundSubscriptionEvent openSubscriptionEvent = new InboundSubscriptionEvent(ProtocolType.MQTT,
                    "", localSubscription.getStorageQueue().getName(),
                    localSubscription.getStorageQueue()
                    .getMessageRouterBindingKey(),
                    connection);

            //notify subscription create event
            Andes.getInstance().openLocalSubscription(openSubscriptionEvent);

            if (log.isDebugEnabled()) {
                log.debug("Subscription registered to the " + topic + " with channel id " + clientID);
            }

        } catch (SubscriptionAlreadyExistsException e) {
            final String message = "Error occurred while creating the topic subscription in the kernel";
            log.error(message, e);
            throw e;
        } catch (AndesException e) {
            String message = "Error occurred while opening subscription ";
            log.error(message, e);
            throw new MQTTException(message, e);
        }


    }

    /**
     * {@inheritDoc}
     */
    public void sendRetainedMessagesToSubscriber(String topic,String subscriptionID, QOSLevel qos,
                                                 UUID subscriptionChannelID)
            throws MQTTException {

        // Send retained message if available to the subscriber.
        // Retain message should send before register topic subscription in cluster. This will ensure
        // retain message is received prior to any other topic message to the subscriber.
        try {
            List<DeliverableAndesMetadata> metadataList = Andes.getInstance().getRetainedMetadataByTopic(topic);

            // Looped through metadata list as there can be multiple messages (due to wildcards) per single subscriber.
            for (DeliverableAndesMetadata metadata : metadataList) {
                AndesContent content = Andes.getInstance().getRetainedMessageContent(metadata);
                // get the message byte buffer from content
                ByteBuffer message = MQTTUtils.getContentFromMetaInformation(content);

                // Need to set do a re position of bytes for writing to the buffer
                // Since the buffer needs to be initialized for reading before sending out
                final int bytesPosition = 0;

                message.position(bytesPosition);
                metadata.setRetain(true);

                MQTTopicManager.getInstance().distributeMessageToSubscriber(topic,message,metadata.getMessageID(),
                                                                            metadata.getQosLevel(), metadata.isRetain(),
                                                                            subscriptionID, qos.getValue(), metadata);

                // keep retain message identification in a set to handle acks.
                // After sending a retain message, this will stored until ack received from subscriber.
                retainMessageIdSet.add(metadata.getMessageID() + subscriptionChannelID.toString());
            }
        } catch (AndesException e) {
            final String message = "Error occurred while fetching MQTT retained metadata/content for topic " + topic;
            log.error(message, e);
        } catch (MQTTException e) {
            String message = "Error occurred while sending retained messages to new subscription.";
            log.error(message, e);
            throw e;
        }
    }


    /**
     * {@inheritDoc}
     */
    public void removeSubscriber(MQTTopicManager channel, String subscribedTopic, String username, String
            subscriptionChannelID, UUID subscriberChannel, boolean isCleanSession, String mqttClientID, QOSLevel
            qosLevel) throws MQTTException {
        try {


            disconnectSubscriber(channel,subscribedTopic,username,subscriptionChannelID, subscriberChannel,
                    isCleanSession, mqttClientID, qosLevel);

            boolean durable = MQTTUtils.isDurable(isCleanSession, qosLevel.getValue());
            if (durable) {

                //This will be similar to a durable subscription of AMQP
                //There could be two types of events one is the disconnection due to the lost of the connection
                //The other is un-subscription, if is the case of un-subscription the subscription should be removed
                //Andes will automatically remove all the subscriptions bound to a queue when the queue is deleted
                String storageQueueName =
                        AndesUtils.getStorageQueueForDestination(subscribedTopic, MQTTUtils.MQTT_EXCHANGE_NAME,
                                subscribedTopic, true);
                InboundQueueEvent queueChange = new InboundQueueEvent(storageQueueName, true, false, username, false);
                Andes.getInstance().deleteQueue(queueChange);
            }

            if (log.isDebugEnabled()) {
                log.debug("Disconnected subscriber from topic " + subscribedTopic);
            }

        } catch (AndesException e) {
            final String message = "Error occurred while removing the subscriber ";
            log.error(message, e);
            throw new MQTTException(message, e);
        }
    }

    /**
     * @{inheritDoc}
     */
    public void disconnectSubscriber(MQTTopicManager channel, String subscribedTopic, String username,
                                     String subscriptionChannelID, UUID subscriberChannel,
                                     boolean isCleanSession, String mqttClientID, QOSLevel qosLevel)
            throws MQTTException {
        try {
            MQTTLocalSubscription mqttTopicSubscriber = createSubscription(subscribedTopic, channel,
                    subscriptionChannelID, qosLevel.getValue(), subscriberChannel, isCleanSession, isCleanSession);

            //create a close subscription event
            AndesSubscription localSubscription = createLocalSubscription(mqttTopicSubscriber, subscribedTopic,
                    mqttClientID);

            String subscribedNode = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();

            SubscriberConnection connection = new SubscriberConnection("0.0.0.0", subscribedNode, mqttTopicSubscriber
                    .getChannelID(), mqttTopicSubscriber);

            InboundSubscriptionEvent subscriptionCloseEvent = new InboundSubscriptionEvent(ProtocolType.MQTT,
                    "", localSubscription.getStorageQueue().getName(), localSubscription.getStorageQueue()
                    .getMessageRouterBindingKey(), connection);

            Andes.getInstance().closeLocalSubscription(subscriptionCloseEvent);

            //remove binding from MQTT message router so that no longer messages are persisted to that queue
            if (localSubscription.getStorageQueue().getBoundSubscriptions().isEmpty()
                    && !mqttTopicSubscriber.isDurable()) {

                QueueInfo queueInfo = new QueueInfo(localSubscription.getStorageQueue().getName(),
                        mqttTopicSubscriber.isDurable(), false, username, false);

                InboundBindingEvent mqttBinding =
                        new InboundBindingEvent(queueInfo, MQTTUtils.MQTT_EXCHANGE_NAME, subscribedTopic);
                Andes.getInstance().removeBinding(mqttBinding);
            }

            if (log.isDebugEnabled()) {
                log.debug("Disconnected subscriber from topic " + subscribedTopic);
            }

        } catch (AndesException e) {
            final String message = "Error occurred while removing the subscriber ";
            log.error(message, e);
            throw new MQTTException(message, e);
        }
    }

    /**
     * @{inheritDoc}
     */
    public UUID removePublisher(String mqttClientChannelId) {
        MQTTPublisherChannel publisher = publisherTopicCorrelate.remove(mqttClientChannelId);
        UUID clusterID = null;
        if (null != publisher) {
            clusterID = publisher.getClusterID();
        }
        return clusterID;
    }

    /**
     * Will create subscriptions out of the provided list of information, this will be used when creating durable,
     * non durable subscriptions. As well as creating the subscription object for removal
     *
     * @param channel               the chanel the data communication should be done at
     * @param mqttClientID          the id of the client which is provided by the protocol
     * @param qos                   the level in which the messages would be exchanged this will be either 0,1 or 2
     * @param subscriptionChannelID the id of the channel that would be unique across the cluster
     * @param isActive              is the subscription active it will be inactive during removal
     * @param cleanSession          has the subscriber subscribed with clean session
     *
     * @return the andes specific object that will be registered in the cluster
     * @throws MQTTException
     */
    private MQTTLocalSubscription createSubscription(String wildcardDestination, MQTTopicManager channel,
                                                     String mqttClientID, int qos,
                                                     UUID subscriptionChannelID, boolean isActive, boolean cleanSession)
            throws MQTTException {

        boolean durable = MQTTUtils.isDurable(cleanSession, qos);

        MQTTLocalSubscription outBoundTopicSubscription = new MQTTLocalSubscription
                (wildcardDestination, subscriptionChannelID, isActive, durable);

        outBoundTopicSubscription.setMqqtServerChannel(channel);
        outBoundTopicSubscription.setMqttSubscriptionID(mqttClientID);
        outBoundTopicSubscription.setSubscriberQOS(qos);

        return outBoundTopicSubscription;

    }

    /**
     * Generate a local subscription object using MQTT subscription information
     * @param mqttLocalSubscription instance of underlying mqtt local subscriber
     * @param topic subscribed topic name
     * @param clientID valid only when isCleanSession = false. A unique id should be given
     * @return Local subscription object representing a subscription in Andes kernel
     */
    private AndesSubscription createLocalSubscription(MQTTLocalSubscription mqttLocalSubscription, String topic,
                                                      String clientID) {

        boolean isDurable = mqttLocalSubscription.isDurable();
        String subscribedNode = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        String targetQueueBoundExchange;
        targetQueueBoundExchange = MQTTUtils.MQTT_EXCHANGE_NAME;

        String storageQueueName =
                AndesUtils.getStorageQueueForDestination(topic, targetQueueBoundExchange,topic, isDurable);

        StorageQueue queueToBind = AndesContext.getInstance().
                getStorageQueueRegistry().getStorageQueue(storageQueueName);

        SubscriberConnection connection = new SubscriberConnection("127.0.0.1",
                subscribedNode, mqttLocalSubscription
                .getChannelID(), mqttLocalSubscription);

        String subscriptionID = mqttLocalSubscription.getMqttSubscriptionID();

        return new AndesSubscription(subscriptionID,
                queueToBind,
                ProtocolType.MQTT,
                connection);
    }

}
