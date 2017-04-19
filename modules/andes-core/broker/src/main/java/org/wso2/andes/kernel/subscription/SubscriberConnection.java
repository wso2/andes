/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.kernel.subscription;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContent;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.ProtocolMessage;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents connection and transport level information
 * of a subscription. It has protocol specific impl inside used to send messages
 * out and receive ACK/REJECT etc.
 */
public class SubscriberConnection {

    private String connectedIP;

    private UUID protocolChannelID;

    private String connectedNode;

    /**
     * Used to mark the subscription as obsolete. Upon being set to true, messages that are scheduled to be
     * delivered will be dropped.
     */
    private AtomicBoolean obsolete;

    /**
     * Used to mark the connection as suspended. Upon being seting to true, messages being scheduled to the
     * subscription will be stopped.
     */
    private boolean suspended;

    /**
     * Outbound subscription reference. We forward outbound events to this object. Get its response
     * and act upon (make kernel side changes)
     */
    private OutboundSubscription outboundSubscription;

    /**
     * This keeps track of sent and acknowledged, rejected messages by the connection
     */
    private OutBoundMessageTracker outBoundMessageTracker;

    private static Log log = LogFactory.getLog(SubscriberConnection.class);

    /**
     * Create a subscriber connection to deliver messages in Andes kernel
     *
     * @param connectedIP          IP address of subscriber host machine
     * @param connectedNode        Node ID of node subscriber connection is created
     * @param protocolChannelID    ID of protocol channel of the connection
     * @param outboundSubscription Protocol specific subscriber
     */
    public SubscriberConnection(String connectedIP, String connectedNode, UUID protocolChannelID,
                                OutboundSubscription outboundSubscription) {
        this.connectedIP = connectedIP;
        this.connectedNode = connectedNode;
        this.protocolChannelID = protocolChannelID;
        //create a tracker with maximum number of messages to keep in memory
        int maxNumberOfDeliveredButNotAckedMessages = AndesConfigurationManager
                .readValue(AndesConfiguration.PERFORMANCE_TUNING_ACK_HANDLING_MAX_UNACKED_MESSAGES);
        this.outBoundMessageTracker = new OutBoundMessageTracker(maxNumberOfDeliveredButNotAckedMessages);
        this.outboundSubscription = outboundSubscription;
        obsolete = new AtomicBoolean(false);
        suspended = false;
    }

    /**
     * Create a subscriber connection to deliver messages in Andes kernel using the properties of an existing
     * subscriber connection.
     *
     * @param connectedIP            IP address of subscriber host machine
     * @param connectedNode          Node ID of node subscriber connection is created
     * @param protocolChannelID      ID of protocol channel of the connection
     * @param outboundSubscription   Protocol specific subscriber
     * @param outBoundMessageTracker the message tracker to be used
     */
    private SubscriberConnection(String connectedIP, String connectedNode, UUID protocolChannelID,
            OutboundSubscription outboundSubscription, OutBoundMessageTracker outBoundMessageTracker) {
        this.connectedIP = connectedIP;
        this.connectedNode = connectedNode;
        this.protocolChannelID = protocolChannelID;
        this.outBoundMessageTracker = outBoundMessageTracker;
        this.outboundSubscription = outboundSubscription;
        obsolete = new AtomicBoolean(false);
        suspended = false;
    }

    /**
     * Create a fresh clone of the connection. Upon creation, this will reset all the suspend/obsolete states as a
     * fresh subscriber connection.
     *
     * @return a clone of the subscriber connection with reset suspend/obsolete states
     */
    SubscriberConnection createFreshClone() {
        return new SubscriberConnection(connectedIP, connectedNode, protocolChannelID, outboundSubscription,
                outBoundMessageTracker);
    }

    /**
     * Create a SubscriberConnection object from encoded information
     *
     * @param encodedConnectionInfo encoded information
     */
    public SubscriberConnection(String encodedConnectionInfo) {
        String[] propertyToken = encodedConnectionInfo.split(",");
        for (String pt : propertyToken) {
            String[] tokens = pt.split("=");
            switch (tokens[0]) {
                case "connectedIP":
                    this.connectedIP = tokens[1];
                    break;
                case "connectedNode":
                    this.connectedNode = tokens[1];
                    break;
                case "protocolChannelID":
                    this.protocolChannelID = UUID.fromString(tokens[1]);
                    break;
                default:
                    if (tokens[0].trim().length() > 0) {
                        throw new UnsupportedOperationException("Unexpected token " + tokens[0]);
                    }
                    break;
            }
        }

        this.outboundSubscription = new NullSubscription();
    }

    /**
     * Retrieves the outbound subscription for the connection.
     *
     * @return the outbound subscription
     */
    OutboundSubscription getOutboundSubscription() {
        return outboundSubscription;
    }

    /**
     * Get IP address of host machine subscriber connection is made
     *
     * @return IP as a string
     */
    public String getConnectedIP() {
        return connectedIP;
    }

    /**
     * Get ID of the protocol channel of the subscriber connection. This is
     * unique for a subscriber
     *
     * @return ID of subscription channel
     */
    public UUID getProtocolChannelID() {
        return protocolChannelID;
    }

    /**
     * ID of the node subscription is made
     *
     * @return ID of the node
     */
    public String getConnectedNode() {
        return connectedNode;
    }

    /**
     * Get time of subscription is made to the broker
     *
     * @return time as number of milliseconds elapsed from 1/1/1970
     */
    public long getSubscribeTime() {
        return outboundSubscription.getSubscribeTime();
    }

    /**
     * Get name of the protocol queue
     *
     * @return name of the queue set by protocol
     */
    public String getProtocolQueueName() {
        return outboundSubscription.getProtocolQueueName();
    }

    /**
     * Forcefully disconnects protocol subscriber connection from server. This is initiated by a server admin using the
     * management console.
     *
     * @throws AndesException on a protocol level issue disconnecting
     */
    public void forcefullyDisconnect() throws AndesException {
        log.info("forcefully disconnecting subscription connection: channelID=" + getProtocolChannelID() + " client " +
                "ip= " + connectedIP);
        outboundSubscription.forcefullyDisconnect();
    }

    /**
     * Send message to the underlying protocol connection
     *
     * @param messageMetadata metadata of the message
     * @param content         content of the message
     * @throws AndesException on an issue writing message to the protocol
     */
    public synchronized void writeMessageToConnection(ProtocolMessage messageMetadata, AndesContent content)
            throws AndesException {

        //messages are dropped if the connection is marked as obsolete.
        if (!obsolete.get()) {
            //It is needed to add the message reference to the tracker and increase un-ack message count BEFORE
            // actual message send because if it is not done ack can come BEFORE executing those lines in parallel world
            if (log.isDebugEnabled()) {
                log.debug("Adding message to sending tracker channel id = " + protocolChannelID
                          + " message id = " + messageMetadata.getMessageID());
            }
            outboundSubscription.sendMessageToSubscriber(messageMetadata, content);
        }
    }

    /**
     * Marks the connection as obsolete. This will stop messages being delivered to the subscription.
     */
    public synchronized void setObsolete() {
        obsolete.set(true);
    }

    /**
     * Called upon a connection error while writing a message to the subscriber to send
     *
     * @param messageID ID of the message to send
     */
    public void onWriteToConnectionError(long messageID) {
        outBoundMessageTracker.removeSentMessageFromTracker(messageID);
    }

    /**
     * Check if subscriber has room to accept messages. This indicates
     * consumer side flow control. If there is a lot of sent but
     * unacknowledged messages it is not ready to accept messages.
     *
     * @return true if subscriber is ready to accept the messages to send to
     * client side.
     */
    public boolean hasRoomToAcceptMessages() {
        return outBoundMessageTracker.hasRoomToAcceptMessages();
    }

    /**
     * Returns if the subscription is marked as suspended.
     *
     * @return true if the subscription is suspended.
     */
    public boolean isSuspended() {
        return suspended;
    }

    /**
     * Suspend/resume subscription.
     *
     * @param suspended if set to true, subscription is marked as suspended
     */
    public void setSuspended(boolean suspended) {
        this.suspended = suspended;
    }

    /**
     * Check if message is accepted by 'selector' set to the connection.
     *
     * @param messageMetadata message to be checked
     * @return true if message is selected, false otherwise
     * @throws AndesException on an error
     */
    public boolean isMessageAcceptedByConnectionSelector(AndesMessageMetadata messageMetadata) throws AndesException {
        return outboundSubscription.isMessageAcceptedBySelector(messageMetadata);
    }

    /**
     * Get all sent but not acknowledged messages for the connection
     *
     * @return list of messages
     */
    public List<DeliverableAndesMetadata> getUnAckedMessages() {
        return outBoundMessageTracker.getUnackedMessages();
    }

    /**
     * Get a specific un-acknowledged message by messageID
     *
     * @param messageID ID of the message
     * @return DeliverableAndesMetadata instance
     */
    public DeliverableAndesMetadata getUnAckedMessage(long messageID) {
        return outBoundMessageTracker.getMessageByMessageID(messageID);
    }

    /**
     * Get all sent but not acknowledged messages to connection
     *
     * @return list of messages
     */
    public List<DeliverableAndesMetadata> getSentButUnAckedMessages() {
        return outBoundMessageTracker.getUnackedMessages();
    }

    /**
     * Clear tracked sent but un-acknowledged messages. Return the messages of the same view
     * at the moment it was cleared. While this operation is performed, no new
     * message will be added to the list.
     *
     * @return list of messages tracked when cleaned up
     */
    public synchronized List<DeliverableAndesMetadata> clearAndReturnUnackedMessages() {
        return outBoundMessageTracker.clearAndReturnUnackedMessages();
    }

    /**
     * Is the underlying protocol connection active and can accept
     * messages
     *
     * @return true if connection is active
     */
    public boolean isSubscriberConnectionLive() {
        return outboundSubscription.isOutboundConnectionLive();
    }

    /**
     * Perform on acknowledgement receive for a message
     *
     * @param messageID id of the message acknowledged
     * @return DeliverableAndesMetadata reference of message acknowledged
     * @throws AndesException
     */
    public DeliverableAndesMetadata onMessageAck(long messageID) throws AndesException {
        DeliverableAndesMetadata ackedMessage =
                outBoundMessageTracker.removeSentMessageFromTracker(messageID);
        if (log.isDebugEnabled()) {
            log.debug("Ack. Removed message reference. Message Id = "
                    + messageID + " channelID= " + protocolChannelID);
        }
        return ackedMessage;
    }

    /**
     * Perform on reject receive for a message. This will remove message from messages tracked by the connection.
     *
     * @param messageID id of the message acknowledged
     * @return  DeliverableAndesMetadata reference of message rejected
     */
    public DeliverableAndesMetadata onMessageReject(long messageID) {
        DeliverableAndesMetadata rejectedMessage = outBoundMessageTracker.removeSentMessageFromTracker(messageID);
        rejectedMessage.markAsNackedByClient(protocolChannelID);
        if (log.isDebugEnabled()) {
            log.debug("Message id= " + messageID + " is rejected by connection " + this);
        }
        return rejectedMessage;
    }


    public String encodeAsString() {
        return "connectedIP=" + connectedIP
                + ",connectedNode=" + connectedNode
                + ",protocolChannelID=" + protocolChannelID.toString();
    }

    public String toString() {
        return encodeAsString();
    }

    public void addMessageToSendingTracker(ProtocolMessage messageMetadata) {
        outBoundMessageTracker.addMessageToSendingTracker(messageMetadata);
    }

}
