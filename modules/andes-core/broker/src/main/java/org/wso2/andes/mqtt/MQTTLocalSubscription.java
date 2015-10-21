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
import org.dna.mqtt.wso2.QOSLevel;
import org.wso2.andes.kernel.*;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.subscription.OutboundSubscription;
import java.nio.ByteBuffer;
import java.util.UUID;


/**
 * Cluster wide subscriptions relevant per topic will be maintained through this class
 * Per topic there will be only one subscription just on indicate that the subscription rely on the specific node
 * Each time a message is published to a specific node the Andes kernal will call this subscription object
 * The subscriber will contain a reference to the relevant bridge connection where the bridge will notify the protocol
 * engine to inform the relevant subscriptions which are channel bound
 */
public class MQTTLocalSubscription implements OutboundSubscription {

    /**
     * Will log the flows in relevant for this class
     */
    private static Log log = LogFactory.getLog(MQTTLocalSubscription.class);

    /**
     * The reference to the bridge object
     */
    private MQTTopicManager mqqtServerChannel;

    /**
     * Will store the MQTT channel id
     */
    private String mqttSubscriptionID;

    /**
     * Will set unique uuid as the channel of the subscription this will be used to track the delivery of messages
     */
    private UUID channelID;

    /**
     * The QOS level the subscription is bound to
     */
    private int subscriberQOS;

    /**
     * The destination subscriber subscribed to
     */
    private String wildcardDestination;

    /**
     * keep if the underlying subscription is active
     */
    private boolean isActive;

    /**
     * Should this subscriber act as a durable one
     */
    private boolean isDurable;

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
     * Retrieval of the subscription id
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
     *  The relevant subscription will be registered
     *
     * @param wildCardDestination The original destination subscriber subscribed to
     * @param channelID ID of the underlying subscription channel
     * @param isActive true if subscription is active (TCP connection is live)
     * @param isDurable Should this subscriber fall into durable path
     */
    public MQTTLocalSubscription(String wildCardDestination, UUID channelID, boolean isActive, boolean isDurable) {

        this.channelID = channelID;
        this.isActive = isActive;
        this.wildcardDestination = wildCardDestination;
        this.isDurable = isDurable;
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
     * {@inheritDoc}
     */
    @Override
    public boolean sendMessageToSubscriber(ProtocolMessage protocolMessage, AndesContent content)
            throws AndesException {

        boolean sendSuccess;

        DeliverableAndesMetadata messageMetadata = protocolMessage.getMessage();

        if(messageMetadata.isRetain()) {
            recordRetainedMessage(messageMetadata.getMessageID());
        }

        //Should get the message from the list
        ByteBuffer message = MQTTUtils.getContentFromMetaInformation(content);
        //Will publish the message to the respective queue
        if (null != mqqtServerChannel) {
            try {

                //TODO:review - instead of getSubscribedDestination() used message destination
                mqqtServerChannel.distributeMessageToSubscriber(wildcardDestination, message,
                        messageMetadata.getMessageID(), messageMetadata.getQosLevel(),
                        messageMetadata.isPersistent(), getMqttSubscriptionID(),
                        getSubscriberQOS(), messageMetadata);

                //We will indicate the ack to the kernel at this stage
                //For MQTT QOS 0 we do not get ack from subscriber, hence will be implicitly creating an ack
                if (QOSLevel.AT_MOST_ONCE.getValue() == getSubscriberQOS() ||
                        QOSLevel.AT_MOST_ONCE.getValue() == messageMetadata.getQosLevel()) {
                    mqqtServerChannel.implicitAck(messageMetadata.getMessageID(), getChannelID());
                }
                sendSuccess = true;
            } catch (MQTTException e) {
                final String error = "Error occurred while delivering message to the subscriber for message :" +
                        messageMetadata.getMessageID();
                log.error(error, e);
                throw new AndesException(error, e);
            }
        } else {
            sendSuccess = false;
        }

        return sendSuccess;
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


    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public UUID getChannelID() {
        return channelID != null ? channelID : null;
    }

    //TODO: decide how to call this
    public void ackReceived(long messageID) {
        // Remove if received acknowledgment message id contains in retained message list.
        retainedMessageList.remove(messageID);
    }

    /**
     * Should this subscription act as a durable subscription
     *
     * @return True if this falls into durable path
     */
    public boolean isDurable() {
        return isDurable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getStorageQueueName(String destination, String subscribedNode) {
        String storageQueueName;
        if (isDurable) {
            storageQueueName = MQTTUtils.getTopicSpecificQueueName(mqttSubscriptionID, destination);
        } else {
            storageQueueName = AndesUtils.getStorageQueueForDestination(destination, subscribedNode, true);
        }

        return storageQueueName;
    }
}