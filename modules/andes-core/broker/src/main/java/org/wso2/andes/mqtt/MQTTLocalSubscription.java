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

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dna.mqtt.wso2.QOSLevel;
import org.wso2.andes.kernel.AndesContent;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.distruptor.inbound.InboundSubscriptionEvent;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.kernel.OnflightMessageTracker;

import java.nio.ByteBuffer;
import java.util.UUID;


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
     * @param mqttTopicSubscriptionAsString the name of the topic the subscription will be bound to
     */
    public MQTTLocalSubscription(String mqttTopicSubscriptionAsString) {
        super(mqttTopicSubscriptionAsString);
        setSubscriptionType(SubscriptionType.MQTT);
        //setTargetBoundExchange();
        setIsTopic(true);
        setNodeInfo();
        setIsActive(true);

        String[] propertyToken = mqttTopicSubscriptionAsString.split(",");
        for (String pt : propertyToken) {
            String[] tokens = pt.split("=");
            if (tokens[0].equals("qos")) {
                this.subscriberQOS = Integer.parseInt(tokens[1]);
            }
        }
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
        //Should get the message from the list
        ByteBuffer message = MQTTUtils.getContentFromMetaInformation(content);
        //Will publish the message to the respective queue
        if (null != mqqtServerChannel) {
            try {
                boolean successful = OnflightMessageTracker.getInstance().incrementNonAckedMessageCount(channelID);
                if (successful) {
                    OnflightMessageTracker.getInstance().addMessageToSendingTracker(getChannelID(),
                            messageMetadata.getMessageID());

                    mqqtServerChannel.distributeMessageToSubscriber(this.getStorageQueueName(),
                            this.getSubscribedDestination(),message,messageMetadata.getMessageID(),
                            messageMetadata.getQosLevel(), messageMetadata.isPersistent(), getMqttSubscriptionID(),
                            getSubscriberQOS(),messageMetadata);
                    //We will indicate the ack to the kernel at this stage
                    //For MQTT QOS 0 we do not get ack from subscriber, hence will be implicitly creating an ack
                    if (QOSLevel.AT_MOST_ONCE.getValue() == getSubscriberQOS() ||
                            QOSLevel.AT_MOST_ONCE.getValue() == messageMetadata.getQosLevel()) {
                        mqqtServerChannel.implicitAck(getSubscribedDestination(), messageMetadata.getMessageID(),
                                this.getStorageQueueName(), getChannelID());
                    }
                } else {
                    //This means the subscription channel has closed and the message acknowledgments would not be tracked
                    //We do not want to attempt to deliver a message to a ghost
                    //Hence we inform the upper layers that the message was not delivered
                    String error = "The subscription do not exist, hence the message will not be dispatched";
                    throw new AndesException(error);
                }
            } catch (MQTTException e) {
                OnflightMessageTracker.getInstance().decrementNonAckedMessageCount(channelID);
                final String error = "Error occurred while delivering message to the subscriber for message :" +
                        messageMetadata.getMessageID();
                log.error(error, e);
                throw new AndesException(error, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isActive() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID getChannelID() {
        return channelID != null ? channelID : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalSubscription createQueueToListentoTopic() {
        //mqqtServerChannel.
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
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

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31).
                append(subscriptionID).
                append(getSubscribedNode()).
                append(targetQueue).
                append(targetQueueBoundExchange).
                toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return super.toString() +
               "/QOS=" + Integer.toString(subscriberQOS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String encodeAsStr() {
        return super.encodeAsStr() + ",qos=" + Integer.toString(subscriberQOS);
    }
}
