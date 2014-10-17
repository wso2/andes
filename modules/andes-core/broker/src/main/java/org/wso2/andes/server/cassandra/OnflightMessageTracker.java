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

package org.wso2.andes.server.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQStoreException;
import org.wso2.andes.kernel.*;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.slot.SlotDeliveryWorker;
import org.wso2.andes.server.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.server.stats.PerformanceCounter;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is responsible of keeping track of message delivery and their status throughout
 * delivery, rejection, retry etc.
 */
public class OnflightMessageTracker {

    private static Log log = LogFactory.getLog(OnflightMessageTracker.class);

    private int acktimeout = 10000;
    private int maximumRedeliveryTimes = 1;

    /**
     * In memory map keeping sent messages. If this map does not have an entry for a delivery
     * scheduled message it is a new message. Otherwise it is a redelivery
     */
    private LinkedHashMap<Long, MsgData> msgId2MsgData = new LinkedHashMap<Long, MsgData>();

    private Map<String, Long> deliveryTag2MsgID = new ConcurrentHashMap<String, Long>();
    private ConcurrentHashMap<UUID, ConcurrentSkipListSet<Long>> channelToMsgIDMap = new
            ConcurrentHashMap<UUID, ConcurrentSkipListSet<Long>>();
    private ConcurrentHashMap<Long, AndesMessageMetadata> messageIdToAndesMessagesMap = new
            ConcurrentHashMap<Long, AndesMessageMetadata>();
    private ConcurrentHashMap<Slot, AtomicInteger> pendingMessagesBySlot = new
            ConcurrentHashMap<Slot, AtomicInteger>();


    /**
     * In memory set keeping track of sent messageIds. Used to prevent duplicate message count
     * decrements
     */
    private Map<Long, Long> deliveredButNotAckedMessages = new ConcurrentHashMap<Long, Long>();

    private static final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor();
    private AtomicLong sendButNotAckedMessageCount = new AtomicLong();
    private ConcurrentHashMap<String, ArrayList<AndesMessageMetadata>>
            queueTosentButNotAckedMessageMap = new ConcurrentHashMap<String,
            ArrayList<AndesMessageMetadata>>();


    /**
     * Class to keep tracking data of a message
     */
    public class MsgData {

        final long msgID;
        boolean ackreceived = false;
        final String queue;
        final long timestamp;
        final String deliveryID;
        final AMQChannel channel;
        int numOfDeliveries;
        boolean ackWaitTimedOut;

        public MsgData(long msgID, boolean ackreceived, String queue, long timestamp,
                       String deliveryID, AMQChannel channel, int numOfDeliveries,
                       boolean ackWaitTimedOut) {
            this.msgID = msgID;
            this.ackreceived = ackreceived;
            this.queue = queue;
            this.timestamp = timestamp;
            this.deliveryID = deliveryID;
            this.channel = channel;
            this.numOfDeliveries = numOfDeliveries;
            this.ackWaitTimedOut = ackWaitTimedOut;
        }
    }

    private static OnflightMessageTracker instance = new OnflightMessageTracker();

    public static OnflightMessageTracker getInstance() {
        return instance;
    }

    private OnflightMessageTracker() {

        this.acktimeout = ClusterResourceHolder.getInstance().getClusterConfiguration()
                .getMaxAckWaitTime() * 1000;
        this.maximumRedeliveryTimes = ClusterResourceHolder.getInstance().getClusterConfiguration()
                .getNumberOfMaximumDeliveryCount();
        /*
         * For all add and remove, following is executed, and it will remove the oldest entry if
         * needed
         */
        msgId2MsgData = new LinkedHashMap<Long, MsgData>() {
            private static final long serialVersionUID = -8681132571102532817L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, MsgData> eldest) {
                MsgData msgData = eldest.getValue();
                boolean todelete = (System.currentTimeMillis() - msgData.timestamp) > (acktimeout
                        * 10);
                if (todelete) {
                    if (!msgData.ackreceived) {
                        //Reduce messages on flight on this channel
                        msgData.channel.decrementNonAckedMessageCount();
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "No ack received for delivery tag " + msgData.deliveryID + " and " +
                                            "message id " + msgData.msgID);
                        }
                        //TODO notify the QueueDeliveryWorker to resend (it work now as well as
                        // Flusher loops around, but this will be faster)
                    }
                    if (deliveryTag2MsgID.remove(msgData.deliveryID) == null) {
                        log.error(
                                "Cannot find delivery tag " + msgData.deliveryID + " and message " +
                                        "id " + msgData.msgID);
                    }
                }
                return todelete;
            }
        };

        /**
         * This thread will removed acked messages or messages that breached max redelivery count
         * from tracking
         * These messages are already scheduled to be removed from message store.
         */
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                //TODO replace this with Gvava Cache if possible
                synchronized (this) {
                    Iterator<MsgData> iterator = msgId2MsgData.values().iterator();
                    while (iterator.hasNext()) {
                        MsgData mdata = iterator.next();
                        if (mdata.ackreceived || (mdata.numOfDeliveries) > maximumRedeliveryTimes) {
                            iterator.remove();
                            deliveryTag2MsgID.remove(mdata.deliveryID);
                            if ((mdata.numOfDeliveries) > maximumRedeliveryTimes) {
                                log.warn(
                                        "Message " + mdata.msgID + " with " + mdata.deliveryID +
                                                " removed as it has gone though max redeliveries");
                            }
                        }
                    }
                }
            }
        }, 5, 10, TimeUnit.SECONDS);

    }

    /**
     * When messages are rejected and failed this method will be called. Rejected/failed message
     * will be put again to the buffer in QueueDeliveryWorker
     * @param deliveryTag
     * @param channelId
     * @throws AMQException
     */
    public void handleFailure(long deliveryTag, UUID channelId) throws AMQException {
        String deliveryID = new StringBuffer(channelId.toString()).append("/").append(deliveryTag)
                .toString();
        Long messageId = deliveryTag2MsgID.get(deliveryID);

        if (log.isDebugEnabled()) {
            log.debug("Handling failed message and reQueue for deliveryTag : " + deliveryID + " messageId : " +
                    messageId);
        }
        if (messageId != null) {
            try {
                synchronized (this) {
                    MsgData msgData = msgId2MsgData.get(messageId);
                    msgData.ackWaitTimedOut = true;
                    if (msgData != null) {
                        msgData.channel.decrementNonAckedMessageCount();
                    }
                }
                //Re-queue the message to send again
                reQueueMessage(messageId);

            } catch (AndesException e) {
                log.warn("Message " + messageId + "re-queueing failed");
                throw new AMQException(AMQConstant.INTERNAL_ERROR,
                        "Message " + messageId + "re-queueing failed", e);
            }
        }
    }

    /**
     * Re-queue the message to be sent again
     *
     * @param messageId
     */
    public void reQueueMessage(long messageId) throws AndesException {
        AndesMessageMetadata metadata = messageIdToAndesMessagesMap.get(messageId);
        QueueDeliveryWorker.QueueDeliveryInfo queueDeliveryInfo = QueueDeliveryWorker.getInstance().
                getQueueDeliveryInfo(metadata.getDestination());
        if (log.isDebugEnabled()) {
            log.debug("Re-Queueing message " + messageId + " to readButUndeliveredMessages map");
        }
        queueDeliveryInfo.readButUndeliveredMessages.add(metadata);
        messageIdToAndesMessagesMap.remove(messageId);
    }

    /**
     * Message is allowed to be sent if and only if it is a new message or an already sent message
     * whose ack wait time out has happened
     *
     * @param messageId
     * @return true if message is not sent earlier, else return false
     */
    public synchronized boolean testMessage(long messageId) {
        return msgId2MsgData.get(messageId) == null;
    }

    /**
     * Register message as delivered through transport. If the message has not sent up to maximum
     * number of re-delivery tries, resend it.
     *
     * @param andesMetaDataEntry
     * @param deliveryTag
     * @param channel
     * @return
     * @throws AMQException
     */
    public boolean testAndAddMessage(AndesMessageMetadata andesMetaDataEntry,
                                     long deliveryTag, AMQChannel channel)
            throws AMQException {

        //TODO - hasitha - are these AMQP specific checks?

        long messageId = andesMetaDataEntry.getMessageID();
        String queue = andesMetaDataEntry.getDestination();
        String nodeSpecificQueueName = queue;
        String deliveryID = new StringBuffer(channel.getId().toString()).append("/")
                .append(deliveryTag)
                .toString();

        long currentTime = System.currentTimeMillis();
        int numOfDeliveriesOfCurrentMsg;
        synchronized (this) {
            MsgData mdata = msgId2MsgData.get(messageId);
            numOfDeliveriesOfCurrentMsg = 0;

            if (deliveryTag2MsgID.containsKey(deliveryID)) {
                throw new RuntimeException(
                        "Delivery Tag " + deliveryID + " reused, this should not happen");
            }
            if (mdata == null) {

                //This is a new message
                deliveredButNotAckedMessages.put(messageId, messageId);
                if (log.isTraceEnabled()) {
                    log.trace(
                            "TRACING>> OFMT-testAndAdd-scheduling new message to deliver with MessageID-"
                                    + messageId);
                }
            }
            //This is an already sent but ack wait time expired message
            else {
                numOfDeliveriesOfCurrentMsg = mdata.numOfDeliveries;
                // Entry should have "ReDelivery" header
                andesMetaDataEntry.setRedelivered();
                /*
                Message has sent once, we will clean lists and consider it a new message,but with delivery times tracked
                 */
                deliveryTag2MsgID.remove(mdata.deliveryID);
                msgId2MsgData.remove(messageId);
                if (log.isTraceEnabled()) {
                    log.trace(
                            "TRACING>> OFMT- testAndAdd-scheduling ack expired message to deliver with " +
                                    "MessageID-" + messageId);
                }
            }
            numOfDeliveriesOfCurrentMsg++;
            deliveryTag2MsgID.put(deliveryID, messageId);

            if(log.isDebugEnabled()) {
                log.debug("Map message to delivery Id for messageID : " + messageId + " deliveryID : " + deliveryID);
            }

            msgId2MsgData.put(messageId,
                    new MsgData(messageId, false, nodeSpecificQueueName, currentTime,
                            deliveryID, channel, numOfDeliveriesOfCurrentMsg, false));
        }
        sendButNotAckedMessageCount.incrementAndGet();

        ConcurrentSkipListSet<Long> messagesDeliveredThroughThisChannel = channelToMsgIDMap.get(channel.getId
                ());
        if (messagesDeliveredThroughThisChannel == null) {
            messagesDeliveredThroughThisChannel = new ConcurrentSkipListSet<Long>();
            messagesDeliveredThroughThisChannel.add(messageId);
            channelToMsgIDMap.put(channel.getId(), messagesDeliveredThroughThisChannel);
        } else {
            messagesDeliveredThroughThisChannel.add(messageId);
        }
        messageIdToAndesMessagesMap.put(messageId, andesMetaDataEntry);
        /*
          Any custom checks or procedures that should be executed before message delivery should
          happen here. Any message
          rejected at this stage will be dropped from the node queue permanently.
         */

        //Check if number of redelivery tries has breached.
        if (numOfDeliveriesOfCurrentMsg > ClusterResourceHolder.getInstance()
                .getClusterConfiguration()
                .getNumberOfMaximumDeliveryCount()) {
            log.warn(
                    "Number of Maximum Redelivery Tries Has Breached. Dropping The Message: " +
                            messageId + "From Queue " + queue);
            return false;
            //Check if queue entry has expired. Any expired message will not be delivered
        } else if (andesMetaDataEntry.isExpired()) {
            log.warn("Message is expired. Dropping The Message: " + messageId);
            return false;
        }
        return true;
    }

    /**
     * This method will update message tracking maps when an ack received.
     *
     * @param channelID Id of the connection which ack was received
     * @param messageId Id of the message for which ack was received
     * @throws AMQStoreException In case
     * @throws AndesException
     */
    public void ackReceived(UUID channelID, long messageId)
            throws AndesException {
        AndesMessageMetadata metadata = null;
        MsgData msgData;
        synchronized (this) {
            msgData = msgId2MsgData.get(messageId);
            if (msgData != null) {
                msgData.ackreceived = true;
                
                // Then update the tracker
                if (log.isTraceEnabled()) {
                    log.trace("TRACING>> OFMT-Ack received for MessageID-" + msgData.msgID);
                }
                sendButNotAckedMessageCount.decrementAndGet();

                //when subscriber is closed these tracks are removed immediately. When ack
                //is handled via disruptor this key might be already removed in such a situation
                //in that case channel id can be null as well
                if (channelID != null && channelToMsgIDMap.get(channelID) != null) {
                    channelToMsgIDMap.get(channelID).remove(messageId);
                }
                metadata = messageIdToAndesMessagesMap.remove(messageId);

                // Decrement pending message count and check whether the slot is empty. If so read the slot again
                // from message store.
                if (metadata != null) {
                    decrementMessageCountInSlotAndCheckToResend(metadata.getSlot(), msgData.queue);
                }

            } else {
                throw new AndesException("No message data found for messageId " + messageId);
            }
        }
    }

    /**
     * Clear channelToMsgIDMap and messageIdToAndesMessagesMap due to channel close.
     *
     * @param channel
     */
    public void releaseAckTrackingSinceChannelClosed(AMQChannel channel) {
        ConcurrentSkipListSet<Long> sentButNotAckedMessages = channelToMsgIDMap.get(channel.getId());

        if (sentButNotAckedMessages != null && sentButNotAckedMessages.size() > 0) {
            Iterator iterator = sentButNotAckedMessages.iterator();
            if (iterator != null) {
                while (iterator.hasNext()) {
                    long messageId = (Long) iterator.next();
                    synchronized (this) {
                        if (msgId2MsgData.get(messageId) != null) {
                            sendButNotAckedMessageCount.decrementAndGet();
                            AndesMessageMetadata queueEntry = messageIdToAndesMessagesMap
                                    .remove(messageId);

                            if(queueEntry != null) {
                                //Re-queue message to the buffer
                                QueueDeliveryWorker.getInstance().reQueueUndeliveredMessagesDueToInactiveSubscriptions(
                                        queueEntry);
                            }
                            log.debug("TRACING>> OFMT- re-queued message-" + messageId + "- since delivered but not " +
                                    "acked");
                        }
                    }
                }
            }
        }
        channelToMsgIDMap.remove(channel.getId());
    }

    public void updateDeliveredButNotAckedMessages(long messageID) {
        deliveredButNotAckedMessages.remove(messageID);
    }

    /**
     * Remove all tracking information from a message moved to Dead Letter Channel.
     *
     * @param messageId message ID
     */
    public synchronized void removeTrackingInformationForDeadMessages(long messageId) {

        //If it is an already sent but not acked message we will not decrement message count again
        MsgData messageData = msgId2MsgData.get(messageId);
        if (messageData != null) {
            //We do following to stop trying to delete message again when acked someday
            deliveredButNotAckedMessages.remove(messageId);
        }
    }

    public ArrayList<AndesMessageMetadata> getSentButNotAckedMessagesOfQueue(String queueName) {
        return queueTosentButNotAckedMessageMap.remove(queueName);
    }

    /**
     * Decrement message count in slot and if it is zerocheck the slot again to resend
     *
     * @param slot
     * @param queueName
     * @throws AndesException
     */
    public void decrementMessageCountInSlotAndCheckToResend(Slot slot, String queueName) throws AndesException {
        AtomicInteger pendingMessageCount = pendingMessagesBySlot.get(slot);
        int messageCount = pendingMessageCount.decrementAndGet();
        if (messageCount == 0) {
                /*
                All the Acks for the slot has bee received. Check the slot again for unsend
                messages and if there are any send them and delete the slot.
                 */
            SlotDeliveryWorker slotWorker = SlotDeliveryWorkerManager.getInstance()
                    .getSlotWorker(queueName);
            slotWorker.checkForSlotCompletionAndResend(slot);
        }

    }

    /**
     * Increment the message count in a slot
     *
     * @param slot
     */
    public void incrementMessageCountInSlot(Slot slot) {
        AtomicInteger pendingMessageCount = pendingMessagesBySlot.get(slot);
        if (pendingMessageCount == null) {
            pendingMessagesBySlot.putIfAbsent(slot, new AtomicInteger());
            pendingMessageCount = pendingMessagesBySlot.get(slot);
        }
        pendingMessageCount.incrementAndGet();
    }
}
