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

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dna.mqtt.wso2.QOSLevel;
import org.wso2.andes.kernel.AndesContent;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.ConcurrentTrackingList;
import org.wso2.andes.kernel.MessageData;
import org.wso2.andes.kernel.OnflightMessageTracker;
import org.wso2.andes.kernel.disruptor.inbound.InboundSubscriptionEvent;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.server.ClusterResourceHolder;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Cluster wide subscriptions relevant per topic will be maintained through this class
 * Per topic there will be only one subscription just on indicate that the subscription rely on the specific node
 * Each time a message is published to a specific node the Andes kernal will call this subscription object
 * The subscriber will contain a reference to the relevant bridge connection where the bridge will notify the protocol
 * engine to inform the relevant subscriptions which are channel bound
 */
public class MQTTLocalSubscription extends InboundSubscriptionEvent {
    //Will log the flows in relevant for this class
    private static Log log = LogFactory.getLog(MQTTLocalSubscription.class);
    //The reference to the bridge object
    private MQTTopicManager mqqtServerChannel;
    //Will store the MQTT channel id
    private String mqttSubscriptionID;
    //Will set unique uuid as the channel of the subscription this will be used to track the delivery of messages
    private UUID channelID;

    //The QOS level the subscription is bound to
    private int subscriberQOS;

    /**
     * Count sent but not acknowledged message count for channel of the subscriber
     */
    private AtomicInteger unAckedMsgCount = new AtomicInteger(0);


    /**
     * Map to track messages being sent <message id, MsgData reference>
     */
    private final ConcurrentHashMap<Long, MessageData> messageSendingTracker
            = new ConcurrentHashMap<Long, MessageData>();

    /**
     * Track messages sent as retained messages
     */
    private ConcurrentTrackingList<Long> retainedMessageList = new ConcurrentTrackingList<Long>();

    /**
     * Will allow retrieval of the qos the subscription is bound to
     *
     * @return the level of qos the subscription is bound to
     */
    public int getSubscriberQOS() {
        return subscriberQOS;
    }

    /**
     * Will specify the level of the qos the subscription is bound to
     *
     * @param subscriberQOS the qos could be either 0,1 or 2
     */
    public void setSubscriberQOS(int subscriberQOS) {
        this.subscriberQOS = subscriberQOS;
    }


    /**
     * Sets a channel identifier which is unique for each subscription, this will be used to tack delivery of message
     *
     * @param channelID the unique identifier of a channel specific to a subscription
     */
    public void setChannelID(UUID channelID) {
        this.channelID = channelID;
    }

    /**
     * Retrival of the subscription id
     *
     * @return the id of the subscriber
     */
    public String getMqttSubscriptionID() {
        return mqttSubscriptionID;
    }

    /**
     * Sets an id to the subscriber which will be unique
     *
     * @param mqttSubscriptionID the unique id of the subscriber
     */
    public void setMqttSubscriptionID(String mqttSubscriptionID) {
        this.mqttSubscriptionID = mqttSubscriptionID;
    }

    /**
     * The relevant subscription will be registered
     *
     * @param mqttTopicSubscription the name of the topic the subscription will be bound to
     */
    public MQTTLocalSubscription(String mqttTopicSubscription) {
        super(mqttTopicSubscription);
        setSubscriptionType(SubscriptionType.MQTT);
        //setTargetBoundExchange();
        setIsTopic(true);
        setNodeInfo();
        setIsActive(true);
    }

    /**
     * Will set the server channel that will maintain the connectivity between the mqtt protocol realm
     *
     * @param mqqtServerChannel the bridge connection that will be maintained between the protocol and andes
     */
    public void setMqqtServerChannel(MQTTopicManager mqqtServerChannel) {
        this.mqqtServerChannel = mqqtServerChannel;
    }

    /**
     * Will include the destination topic name the message should be given to
     *
     * @param dest the destination the message should be delivered to
     */
    public void setTopic(String dest) {
        this.destination = dest;
    }

    /**
     * Will set the unique identification for the subscription reference
     *
     * @param id the identity of the subscriber
     */
    public void setSubscriptionID(String id) {
        this.subscriptionID = id;
    }

    /**
     * Will set the target bound exchange
     */
    public void setTargetBoundExchange(String exchange) {
        this.targetQueueBoundExchange = exchange;
    }

    /**
     * In MQTT all the messages will be exchanged through topics
     * So will override the is bound to always have the value true
     */
    public void setIsTopic(boolean isTopic) {
        this.isBoundToTopic = isTopic;
    }

    /**
     * Should inform the particular node the subscriber is bound to, This method would set the node id to the current
     */
    public void setNodeInfo() {
        this.subscribedNode = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
    }

    /**
     * Will indicate whether the given request is to active the subscription or deactivate it
     *
     * @param isActive the connection status
     */
    public void setIsActive(boolean isActive) {
        this.hasExternalSubscriptions = isActive;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendMessageToSubscriber(AndesMessageMetadata messageMetadata, AndesContent content)
            throws AndesException {

        if(messageMetadata.isRetain()) {
            recordRetainedMessage(messageMetadata.getMessageID());
        }

        //Should get the message from the list
        ByteBuffer message = MQTTUtils.getContentFromMetaInformation(content);
        //Will publish the message to the respective queue
        if (null != mqqtServerChannel) {
            try {
                addMessageToSendingTracker(messageMetadata.getMessageID());
                unAckedMsgCount.incrementAndGet();
                mqqtServerChannel.distributeMessageToSubscriber(this.getStorageQueueName(),
                        this.getSubscribedDestination(), message, messageMetadata.getMessageID(),
                        messageMetadata.getQosLevel(), messageMetadata.isPersistent(), getMqttSubscriptionID(),
                        getSubscriberQOS(), messageMetadata);
                //We will indicate the ack to the kernel at this stage
                //For MQTT QOS 0 we do not get ack from subscriber, hence will be implicitly creating an ack
                if (QOSLevel.AT_MOST_ONCE.getValue() == getSubscriberQOS() ||
                        QOSLevel.AT_MOST_ONCE.getValue() == messageMetadata.getQosLevel()) {
                    mqqtServerChannel.implicitAck(getSubscribedDestination(), messageMetadata.getMessageID(),
                            this.getStorageQueueName(), getChannelID());
                }
            } catch (MQTTException e) {
                //TODO: we need to remove from sending tracker if we could not send
                unAckedMsgCount.decrementAndGet();
                final String error = "Error occurred while delivering message to the subscriber for message :" +
                        messageMetadata.getMessageID();
                log.error(error, e);
                throw new AndesException(error, e);
            }
        }
    }

    /**
     * Record the given message ID as a retained message in the trcker.
     *
     * @param messageID
     *    Message ID of the retained message
     */
    public void recordRetainedMessage(long messageID) {
        retainedMessageList.add(messageID);
    }

    /**
     * Stamp a message as sent. This method also evaluate if the
     * message is being redelivered
     *
     * @param messageID id of the message
     * @return if message is redelivered
     */
    private boolean addMessageToSendingTracker(long messageID) {

        if (retainedMessageList.contains(messageID)) {
            return false;
        }

        if (log.isDebugEnabled()) {
            log.debug("Adding message to sending tracker channel id = " + getChannelID() + " message id = "
                    + messageID);
        }
        MessageData messageData = messageSendingTracker.get(messageID);

        if (null == messageData) {
            messageData = OnflightMessageTracker.getInstance().getTrackingData(messageID);
            messageSendingTracker.put(messageID, messageData);
        }
        // increase delivery count
        int numOfCurrentDeliveries = messageData.incrementDeliveryCount(getChannelID());


        if (log.isDebugEnabled()) {
            log.debug("Number of current deliveries for message id= " + messageID + " to Channel " + getChannelID()
                    + " is " + numOfCurrentDeliveries);
        }

        //check if this is a redelivered message
        return  messageData.isRedelivered(getChannelID());
    }


    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public UUID getChannelID() {
        return channelID != null ? channelID : null;
    }

    @Override
    public boolean hasRoomToAcceptMessages() {
        return true;
    }

    @Override
    public void ackReceived(long messageID) {
        messageSendingTracker.remove(messageID);
        unAckedMsgCount.decrementAndGet();
        // Remove if received acknowledgment message id contains in retained message list.
        retainedMessageList.remove(messageID);
    }

    @Override
    public void msgRejectReceived(long messageID) {
        messageSendingTracker.remove(messageID);
        unAckedMsgCount.decrementAndGet();
    }

    @Override
    public void close() {
        messageSendingTracker.clear();
        unAckedMsgCount.set(0);
    }
    
    public boolean equals(Object o) {
        if (o instanceof MQTTLocalSubscription) {
            MQTTLocalSubscription c = (MQTTLocalSubscription) o;
            if (this.subscriptionID.equals(c.subscriptionID) &&
                    this.getSubscribedNode().equals(c.getSubscribedNode()) &&
                    this.targetQueue.equals(c.targetQueue) &&
                    this.targetQueueBoundExchange.equals(c.targetQueueBoundExchange)) {
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return new HashCodeBuilder(17, 31).
                append(subscriptionID).
                append(getSubscribedNode()).
                append(targetQueue).
                append(targetQueueBoundExchange).
                toHashCode();
    }
}
