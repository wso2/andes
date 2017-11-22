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

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.attribute.SimpleNullableAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.MessageFlusher;
import org.wso2.andes.kernel.MessageStatus;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.metrics.MetricsConstants;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.tools.utils.MessageTracer;
import org.wso2.carbon.metrics.manager.Counter;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.Meter;
import org.wso2.carbon.metrics.manager.MetricManager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * This class represents subscription to the broker inside Andes Kernel. It has no protocol specific attributes
 * or functions. There are only attributes needed to represent it inside kernel. AndesSubscription is responsible
 * for handling both inbound (protocol > kernel) and outbound (kernel > protocol) subscription
 * events. For handling outbound events it keeps a OutboundSubscription object per connection and forward
 * requests
 */
public class AndesSubscription {

    /**
     * ID of the subscription. This id is unique cluster-wide
     */
    private String subscriptionId;

    /**
     * Queue to which subscriber is bound
     */
    private StorageQueue storageQueue;

    /**
     * Protocol of the subscription connection
     */
    private ProtocolType protocolType;

    /**
     * Is subscription active
     */
    protected boolean isActive;

    /**
     * Connection attached to the subscriber
     */
    protected final SubscriberConnection subscriberConnection;

    private static Log log = LogFactory.getLog(AndesSubscription.class);

    /**
     * Create a AndesSubscription.
     *
     * @param subscriptionId       ID of subscription. This is unique cluster-wide for a subscription
     * @param storageQueue         queue to bind the subscription. Any message come to this queue will be received by
     *                             subscriber
     * @param protocol             protocol of the subscription (AMQP/MQTT)
     * @param subscriberConnection connection handler with protocol specific implementations
     */
    public AndesSubscription(String subscriptionId, StorageQueue storageQueue,
                             ProtocolType protocol, SubscriberConnection subscriberConnection) {

        this.subscriptionId = subscriptionId;
        this.storageQueue = storageQueue;
        this.protocolType = protocol;
        this.subscriberConnection = subscriberConnection;
        this.isActive = true;

    }

    /**
     * Clones the subscription object with a clone of it's underlying connection with reset suspend/resume states
     *
     * @return a clone of the subscription that is not suspended or obsolete
     */
    private AndesSubscription cloneAndResetSubscription() {
        return new AndesSubscription(subscriptionId, storageQueue, protocolType,
                subscriberConnection.createFreshClone());
    }

    /**
     * Create a AndesSubscription representation
     *
     * @param encodedSubscription encoded subscription information as a String
     * @throws SubscriptionException on an issue when creating instance
     */
    public AndesSubscription(String encodedSubscription) throws SubscriptionException {

        SubscriberConnection subscriberConnection = null;

        String[] propertyToken = encodedSubscription.split(",");

        for (String pt : propertyToken) {
            String[] tokens = pt.split("=", 2);
            switch (tokens[0]) {
                case "subscriptionId":
                    this.subscriptionId = tokens[1];
                    break;
                case "storageQueue":
                    String storageQueueName = tokens[1];
                    this.storageQueue = AndesContext.getInstance().
                            getStorageQueueRegistry().getStorageQueue(storageQueueName);
                    if (null == storageQueue) {
                        throw new SubscriptionException("StorageQueue: " + storageQueueName
                                + " is not registered while creating "
                                + "subscription id=" + subscriptionId);
                    }
                    break;
                case "protocolType":
                    this.protocolType = ProtocolType.valueOf(tokens[1]);
                    break;
                case "isActive":
                    this.isActive = Boolean.parseBoolean(tokens[1]);
                    break;
                case "subscriberConnection":
                    byte[] decodedBytes = Base64.decodeBase64(tokens[1].getBytes());
                    String decodedConnectionInfo = new String(decodedBytes);
                    subscriberConnection = new SubscriberConnection(decodedConnectionInfo);
                    break;
                default:
                    if (tokens[0].trim().length() > 0) {
                        throw new UnsupportedOperationException("Unexpected token " + tokens[0]);
                    }
                    break;
            }
        }

        this.subscriberConnection = subscriberConnection;
    }

    /**
     * Get ID of the subscription
     *
     * @return ID of the subscriber
     */
    public String getSubscriptionId() {
        return subscriptionId;
    }

    /**
     * Get if subscriber is durable
     *
     * @return true if subscriber is durable
     */
    public boolean isDurable() {
        return storageQueue.isDurable();
    }

    /**
     * Get if subscriber is active. This means
     * internal connection is live
     *
     * @return true if subscriber is active
     */
    public boolean isActive() {
        return isActive;
    }

    /**
     * Get storage queue subscriber is bound to
     *
     * @return queue bounded
     */
    public StorageQueue getStorageQueue() {
        return storageQueue;
    }

    /**
     * Get protocol queue name
     *
     * @return queue name
     */
    public String getProtocolQueue() {
        return subscriberConnection.getProtocolQueueName();
    }

    /**
     * Get protocol of the subscriber
     *
     * @return protocol of the connection
     */
    public ProtocolType getProtocolType() {
        return protocolType;
    }


    //TODO: fix giving this out is not good

    /**
     * Get subscriber connection information underlying
     *
     * @return SubscriberConnection
     */
    public SubscriberConnection getSubscriberConnection() {
        return subscriberConnection;
    }


    /**
     * Perform on acknowledgement receive for a message
     *
     * @param messageID id of the message acknowledged
     * @throws AndesException on an issue when handling ack
     */
    public void onMessageAck(long messageID) throws AndesException {
        subscriberConnection.onMessageAck(messageID);
    }

    /**
     * Perform on reject receive for a message
     *
     * @param messageID id of the message acknowledged
     * @param reQueue true if message should be re-queued to subscriber
     * @throws AndesException on a message re-schedule issue
     * @return DeliverableAndesMetadata reference of rejected message
     */
    public DeliverableAndesMetadata onMessageReject(long messageID, boolean reQueue)
            throws AndesException {
        DeliverableAndesMetadata rejectedMessage = subscriberConnection.onMessageReject(messageID);
        //Adding metrics meter for ack rate
        Meter ackMeter = MetricManager.meter(MetricsConstants.REJECT_RECEIVE_RATE, Level.INFO);
        ackMeter.mark();

        //Adding metrics counter for reject messages
        Counter counter = MetricManager.counter(MetricsConstants.REJECT_MESSAGES, Level.INFO);
        counter.inc();
        if(reQueue) {
            if(log.isDebugEnabled()) {
                log.debug("Message rejected id= " + rejectedMessage.getMessageID() + " reQueue= true");
            }
            reDeliverMessage(rejectedMessage);
        } else {
            if(log.isDebugEnabled()) {
                log.debug("Message rejected id= " + rejectedMessage.getMessageID() + " reQueue= false");
            }
            /*
             * This is done when message is rejected with reQueue=false. Thus move message to Dead Letter Channel
             */
            Andes.getInstance().moveMessageToDeadLetterChannel(rejectedMessage, rejectedMessage.getDestination());
        }

        return rejectedMessage;
    }

    /**
     * Recover messages of the subscriber. Buffers all sent but un-acknowledged messages back to the queue it is bound.
     */
    public void recoverMessages() throws AndesException {

        //stop message delivery to the channel
        prepareRecover();

        synchronized (subscriberConnection) {
            List<DeliverableAndesMetadata> unAckedMessages = subscriberConnection.clearAndReturnUnackedMessages();

            if (isDurable()) {     //add un-acknowledged messages back to queue
                Iterator<DeliverableAndesMetadata> unAckedMessageIterator = unAckedMessages.iterator();
                while (unAckedMessageIterator.hasNext()) {
                    DeliverableAndesMetadata unAckedMessage = unAckedMessageIterator.next();
                    if (log.isDebugEnabled()) {
                        log.debug("Recovered message id= " + unAckedMessage.getMessageID() + " channel = "
                                  + subscriberConnection.getProtocolChannelID());
                    }
                    unAckedMessage.markAsRecoveredByClient(subscriberConnection.getProtocolChannelID());
                    unAckedMessage.rollbackDelivery(subscriberConnection.getProtocolChannelID());
                    storageQueue.bufferMessageForDelivery(unAckedMessage);
                    unAckedMessageIterator.remove();
                }
            } else {    //reschedule messages back to subscriber
                for (DeliverableAndesMetadata unAckedMessage : unAckedMessages) {
                    if (log.isDebugEnabled()) {
                        log.debug("Recovered message id= " + unAckedMessage.getMessageID() + " channel = "
                                  + subscriberConnection.getProtocolChannelID());
                    }
                    unAckedMessage.markAsRecoveredByClient(subscriberConnection.getProtocolChannelID());
                    unAckedMessage.rollbackDelivery(subscriberConnection.getProtocolChannelID());
                    reDeliverMessage(unAckedMessage);
                }
            }
        }
    }

    /**
     * Prepare the broker to recover messages. This mainly stops messages being delivered to the subscription from
     * which the recovery request was received.
     */
    private void prepareRecover() {
        //stop message delivery through the connection
        subscriberConnection.setObsolete();

        //replace the existing subscriber from a new subscriber inorder to invalidate messages that are buffered but
        // are not yet delivered.
        AndesSubscription replacingSubscription = cloneAndResetSubscription();
        //suspend the subscription since we do not need messages to be scheduled for it yet.
        replacingSubscription.getSubscriberConnection().setSuspended(true);

        //remove all references from the existing subscirption and point those to the new subscription
        storageQueue.replaceBoundSub(this, replacingSubscription);
        AndesContext.getInstance().getAndesSubscriptionManager().replaceSubscription(this,
                replacingSubscription);
    }

    /**
     * Handle sent but unacked messages of the subscriber. For durable subscriptions, all sent but un-acknowledged
     * messages back to the queue it is bound
     */
    public void rebufferUnackedMessages() throws AndesException {
        List<DeliverableAndesMetadata> unAckedMessages = subscriberConnection.clearAndReturnUnackedMessages();
        if (isDurable()) {     // add un-acknowledged messages back to queue

            for (DeliverableAndesMetadata unAckedMessage : unAckedMessages) {
                if (log.isDebugEnabled()) {
                    log.debug("Recovered message id= " + unAckedMessage.getMessageID() + " channel = "
                            + subscriberConnection.getProtocolChannelID());
                }

                MessageTracer.trace(unAckedMessage.getMessageID(), unAckedMessage.getStorageQueueName(),
                        "rebuffering message due to subscription left");

                UUID protocolChannelID = getSubscriberConnection().getProtocolChannelID();
                unAckedMessage.markAsNackedByClient(subscriberConnection.getProtocolChannelID());
                unAckedMessage.markDeliveredChannelAsClosed(protocolChannelID);
                storageQueue.bufferMessageForDelivery(unAckedMessage);
            }
        } else {    // removed messages acked by all
            List<DeliverableAndesMetadata> messagesToRemove = new ArrayList<>();

            for (DeliverableAndesMetadata unAckedMessage : unAckedMessages) {
                if (log.isDebugEnabled()) {
                    log.debug("Re message id= " + unAckedMessage.getMessageID()
                            + " channel = " + subscriberConnection.getProtocolChannelID());
                }

                UUID protocolChannelID = getSubscriberConnection().getProtocolChannelID();
                unAckedMessage.markDeliveredChannelAsClosed(protocolChannelID);
                unAckedMessage.evaluateMessageAcknowledgement();

                //for topic messages see if we can delete the message
                if ((!unAckedMessage.isOKToDispose()) && (unAckedMessage.isTopic())) {
                    if (unAckedMessage.getLatestState().equals(MessageStatus.ACKED_BY_ALL)) {
                        messagesToRemove.add(unAckedMessage);
                    }
                }
            }

            MessagingEngine.getInstance().deleteMessages(messagesToRemove);
        }
    }


    /**
     * Schedule to redeliver a message to this subscriber
     *
     * @param messageMetadata message to redeliver
     * @throws AndesException
     */
    private void reDeliverMessage(DeliverableAndesMetadata messageMetadata) throws AndesException {
        if (!messageMetadata.isOKToDispose()) {
            MessageFlusher.getInstance().scheduleMessageForSubscription(this, messageMetadata);
            MessageTracer.trace(messageMetadata, MessageTracer.MESSAGE_REQUEUED_SUBSCRIBER);
        } else {
            log.warn("Cannot reschedule message id= "+ messageMetadata.getMessageID()
                    + " as it is disposable. Status history " + messageMetadata.getStatusHistoryAsString());
        }
    }


    /**
     * Close subscriber connection. This will terminate the network connection
     * from that subscriber to broker and modify states of messages shared by
     * subscribers if subscriber is local.
     *
     * @param channelID ID of the channel to close
     * @param nodeID    ID of the node
     * @throws AndesException
     */
    public void closeConnection(UUID channelID, String nodeID) throws AndesException {
        /*
          Re-evaluate ACKED_BY_ALL and delete messages if applicable
         */
        String localNodeID = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        if (nodeID.equals(localNodeID)) {
            //close the actual connection to the broker
            subscriberConnection.forcefullyDisconnect();
            try {
                List<DeliverableAndesMetadata> messagesToRemove = new ArrayList<>();

                for (DeliverableAndesMetadata andesMetadata : getSubscriberConnection().getUnAckedMessages()) {
                    andesMetadata.markDeliveredChannelAsClosed(channelID);
                    //re-evaluate ACK if a topic subscriber has closed
                    if (!isDurable()) {
                        andesMetadata.evaluateMessageAcknowledgement();
                        //for topic messages see if we can delete the message
                        if ((!andesMetadata.isOKToDispose()) && (andesMetadata.isTopic())) {
                            if (andesMetadata.getLatestState().equals(MessageStatus.ACKED_BY_ALL)) {
                                messagesToRemove.add(andesMetadata);
                            }
                        }
                    }
                }

                MessagingEngine.getInstance().deleteMessages(messagesToRemove);
            } catch (AndesException e) {
                throw new SubscriptionException("Could not delete ACKED_BY_ALL messages"
                        + " on non durable subscription close");
            }
        }
    }

    /**
     * Disconnect all the connections with this subscription from server side
     *
     * @throws AndesException
     */
    public void forcefullyDisconnectConnections() throws AndesException {
        subscriberConnection.forcefullyDisconnect();
    }

    /**
     * Check subscription is belong to local node
     *
     * @return true/false based on the connection
     */
    public boolean isLocal() {
        String myNodeID = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        String connectedNode = ((null == getSubscriberConnection()) ? "" : getSubscriberConnection()
                .getConnectedNode());
        return connectedNode.equals(myNodeID);
    }

    /**
     * Encode the subscription to a string representation
     *
     * @return String of encoded subscription
     */
    public String toString() {
        return "subscriptionId=" + subscriptionId
                + ",storageQueue=" + storageQueue.getName()
                + ",protocolType=" + protocolType.toString()
                + ",isActive=" + isActive
                + ",connection= [ " + getSubscriberConnection().toString()
                + " ]";
    }


    /**
     * Attributes needed for subscription selection using cqengine
     */


    public static final Attribute<AndesSubscription, String> SUB_ID = new SimpleAttribute<AndesSubscription, String>
            ("subID") {
        @Override
        public String getValue(AndesSubscription sub, QueryOptions queryOptions) {
            return sub.subscriptionId;
        }
    };

    public static final Attribute<AndesSubscription, ProtocolType> PROTOCOL = new SimpleAttribute<AndesSubscription,
            ProtocolType>("protocol") {
        @Override
        public ProtocolType getValue(AndesSubscription sub, QueryOptions queryOptions) {
            return sub.protocolType;
        }
    };

    public static final Attribute<AndesSubscription, String> NODE_ID = new SimpleAttribute<AndesSubscription, String>
            ("nodeID") {
        @Override
        public String getValue(AndesSubscription sub, QueryOptions queryOptions) {
            return sub.subscriberConnection.getConnectedNode();
        }
    };

    public static final Attribute<AndesSubscription, UUID> CHANNEL_ID = new SimpleAttribute<AndesSubscription, UUID>
            ("channelID") {
        @Override
        public UUID getValue(AndesSubscription sub, QueryOptions queryOptions) {
            return sub.subscriberConnection.getProtocolChannelID();
        }
    };

    public static final Attribute<AndesSubscription, String> ROUTER_NAME =
            new SimpleNullableAttribute<AndesSubscription, String>("routerName") {
        @Override
        public String getValue(AndesSubscription sub, QueryOptions queryOptions) {
            String routerName = null;
            if (sub.storageQueue.getMessageRouter() != null) {
                routerName = sub.storageQueue.getMessageRouter().getName();
            }
            return routerName;
        }
    };

    public static final Attribute<AndesSubscription, String> ROUTING_KEY =
            new SimpleNullableAttribute<AndesSubscription, String>("routingKey") {
        @Override
        public String getValue(AndesSubscription sub, QueryOptions queryOptions) {
            String routingKey = null;
            if (sub.getStorageQueue().getMessageRouter() != null) {
                routingKey = sub.getStorageQueue().getMessageRouterBindingKey().toLowerCase();
            }
            return routingKey;
        }
    };

    public static final Attribute<AndesSubscription, String> STORAGE_QUEUE_NAME = new
            SimpleAttribute<AndesSubscription, String>("storageQueue") {
                @Override
                public String getValue(AndesSubscription sub, QueryOptions queryOptions) {
                    return sub.storageQueue.getName();
                }
            };

    public static final Attribute<AndesSubscription, Boolean> DURABILITY = new
            SimpleAttribute<AndesSubscription, Boolean>("isDurable") {
                @Override
                public Boolean getValue(AndesSubscription sub, QueryOptions queryOptions) {
                    return sub.isDurable();
                }
            };

    public static final Attribute<AndesSubscription, Boolean> STATE = new SimpleAttribute<AndesSubscription, Boolean>
            ("isOutboundConnectionLive") {
        @Override
        public Boolean getValue(AndesSubscription sub, QueryOptions queryOptions) {
            return sub.isActive;
        }
    };


    public boolean equals(Object o) {
        if (o instanceof AndesSubscription) {
            AndesSubscription c = (AndesSubscription) o;
            return this.subscriptionId.equals(c.subscriptionId);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return new HashCodeBuilder(17, 31).
                append(subscriptionId).
                toHashCode();
    }

    /**
     * Encode subscription information to a string
     *
     * @return encoded String
     */
    public String encodeAsStr() {
        byte[] encodedConnectionAsBytes =
                Base64.encodeBase64((subscriberConnection.encodeAsString()).getBytes());
        String encodedConnectionInfo = new String(encodedConnectionAsBytes);
        return "subscriptionId=" + subscriptionId
                + ",storageQueue=" + storageQueue.getName()
                + ",protocolType=" + protocolType.toString()
                + ",isActive=" + Boolean.toString(isActive)
                + ",subscriberConnection=" + encodedConnectionInfo;
    }
}
