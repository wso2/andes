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

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.SubscriptionAlreadyExistsException;
import org.wso2.andes.kernel.distruptor.inbound.PubAckHandler;
import org.wso2.andes.mqtt.MQTTException;
import org.wso2.andes.mqtt.MQTTopicManager;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Connects with the underlying messaging engine for further distribution of the message
 * The messaging engine could be Andes, In-Memory based message store
 */
public interface MQTTConnector {
    /**
     * The acked messages will be informed to the kernel
     *
     * @param messageID   the identifier of the message
     * @param topicName   the name of the topic the message was published
     * @param storageName the storage name representation of the topic
     * @throws org.wso2.andes.kernel.AndesException if the ack was not processed properly
     */
    public void messageAck(long messageID, String topicName, String storageName, UUID subChannelID)
            throws AndesException;

    /**
     * Will add the message content which will be received
     *
     * @param message            the content of the message which was published
     * @param topic              the name of the topic which the message was published
     * @param qosLevel           the level of the qos the message was published
     * @param mqttLocalMessageID the channel id the subscriber is bound to
     * @param retain             whether the message requires to be persisted
     * @param publisherID        the id which will identify the publisher
     * @param pubAckHandler      interface with publisher acknowledgement handling
     * @throws org.wso2.andes.mqtt.MQTTException occurs if there was an error while adding the message content
     */
    public void addMessage(ByteBuffer message, String topic, int qosLevel,
                           int mqttLocalMessageID, boolean retain,
                           String publisherID, PubAckHandler pubAckHandler) throws MQTTException;


    /**
     * Will add and indicate the subscription to the kernel the bridge will be provided as the channel
     * since per topic we will only be creating one channel with andes
     *
     * @param channel               the bridge connection as the channel
     * @param topic                 the name of the topic which has subscriber/s
     * @param clientID              the id which will distinguish the topic channel
     * @param mqttClientID          the subscription id which is local to the subscriber
     * @param isCleanSesion         should the connection be durable
     * @param qos                   the subscriber specific qos this can be either 0,1 or 2
     * @param subscriptionChannelID will hold the unique identifier of the subscription
     * @throws MQTTException
     */
    public void addSubscriber(MQTTopicManager channel, String topic, String clientID, String mqttClientID,
                              boolean isCleanSesion, int qos, UUID subscriptionChannelID) throws MQTTException,
            SubscriptionAlreadyExistsException;


    /**
     * Will trigger when subscriber sends a un subscription message
     *
     * @param channel               the connection reference to the bridge
     * @param subscribedTopic       the topic the subscription disconnection should be made
     * @param subscriptionChannelID the channel id of the disconnected client
     * @param subscriberChannel     the cluster wide unique identification of the subscription
     * @param isCleanSession        durability of the subscription
     * @param mqttClientID          the id of the client who subscribed to the topic
     * @throws MQTTException
     */
    public void removeSubscriber(MQTTopicManager channel, String subscribedTopic, String subscriptionChannelID,
                                 UUID subscriberChannel, boolean isCleanSession, String mqttClientID)
            throws MQTTException;

    /**
     * Will trigger the subscription disconnect event
     *
     * @param channel               the connection reference to the bridge
     * @param subscribedTopic       the topic the subscription disconnection should be made
     * @param subscriptionChannelID the channel id of the disconnected client
     * @param subscriberChannel     the cluster wide unique identification of the subscription
     * @param isCleanSession        durability of the subscription
     * @param mqttClientID          the id of the client who subscribed to the topic
     * @throws MQTTException
     */
    public void disconnectSubscriber(MQTTopicManager channel, String subscribedTopic, String subscriptionChannelID,
                                     UUID subscriberChannel, boolean isCleanSession, String mqttClientID)
            throws MQTTException;

    /**
     * Removes the publisher
     * @param mqttClientChannelID publisher id (local id) of the publisher to be removed
     * @return UUID of the publisher. Unique id for the publisher in the cluster
     */
    public UUID removePublisher(String mqttClientChannelID);
}
