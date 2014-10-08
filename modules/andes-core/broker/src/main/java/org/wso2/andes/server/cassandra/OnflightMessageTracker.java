/*
 *
 *   Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 */

package org.wso2.andes.server.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.kernel.*;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.slot.SlotDeliveryWorker;
import org.wso2.andes.server.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.server.stats.PerformanceCounter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class will track message delivery by broker
 * on the fly. Message delivery times, message status,
 * is tracked here
 */
public class OnflightMessageTracker {

    private static Log log = LogFactory.getLog(OnflightMessageTracker.class);

    private static OnflightMessageTracker instance = new OnflightMessageTracker();

    public static OnflightMessageTracker getInstance() {
        return instance;
    }


    /**
     * Maximum number of times a message is tried to deliver
     */
    private int maximumRedeliveryTimes = 1;

    /**
     * When a message is read from a slot tracker keeps a record
     * that the message is buffered. Those tracked are kept in memory
     * until this number of new slots are read.
     */
    private int numberOfSlotsToKeepTracing = 5;

    /**
     * Message tracking data will be kept maximum until
     * this timeout is reached
     */
    private long staleMsgDataRemovalTimeout;

    /**
     * In memory map keeping sent message statistics by message id
     */
    private LinkedHashMap<Long, MsgData> msgId2MsgData = new LinkedHashMap<Long, MsgData>();

    /**
     * Map to track messages being buffered to be sent <slot reference, messageID, MsgData
     * reference>
     */
    private ConcurrentHashMap<Slot, ConcurrentHashMap<Long, MsgData>> messageBufferingTracker
            = new ConcurrentHashMap<Slot, ConcurrentHashMap<Long, MsgData>>();

    /**
     * Map to track messages being sent <channel id, message id, MsgData reference>
     */
    private ConcurrentHashMap<UUID, ConcurrentHashMap<Long, MsgData>> messageSendingTracker
            = new ConcurrentHashMap<UUID, ConcurrentHashMap<Long, MsgData>>();

    private ConcurrentHashMap<Long, AndesMessageMetadata> messageIdToAndesMessagesMap = new
            ConcurrentHashMap<Long, AndesMessageMetadata>();

    /**
     * Map to keep track of message counts pending to read
     */
    private ConcurrentHashMap<Slot, AtomicInteger> pendingMessagesBySlot = new
            ConcurrentHashMap<Slot, AtomicInteger>();


    /**
     * Keep processed slots.
     */
    private LinkedHashMap<String, Slot> locallyProcessedSlots;

    /**
     * count sent but not acked message count for all destinations
     */
    private AtomicLong sendButNotAckedMessageCount = new AtomicLong();

    /**
     * Message status to keep track in which state message is
     */
    public enum MessageStatus {
        Read,
        Buffered,
        Sent,
        Acked,
        RejectedAndBuffered,
        Resent,
        Expired,
        DLCMessage;


        /**
         * Is OK to remove tracking message
         * @param ms status of the message
         * @return eligibility to remove
         */
        public static boolean isOKToRemove(MessageStatus ms){
            return ( ms == Expired || ms == Acked || ms == DLCMessage);
        }

    }

    /**
     * Class to keep tracking data of a message
     */
    private class MsgData {

        final long msgID;
        boolean ackreceived = false;
        final String queue;
        long timestamp;
        final String deliveryID;
        final AMQChannel channel;
        int numOfDeliveries;
        boolean ackWaitTimedOut;
        MessageStatus messageStatus;
        Slot slot;

        private MsgData(long msgID, Slot slot, boolean ackReceived, String queue, long timestamp,
                        String deliveryID, AMQChannel channel, int numOfDeliveries,
                        MessageStatus messageStatus) {
            this.msgID = msgID;
            this.slot = slot;
            this.ackreceived = ackReceived;
            this.queue = queue;
            this.timestamp = timestamp;
            this.deliveryID = deliveryID;
            this.channel = channel;
            this.numOfDeliveries = numOfDeliveries;
            this.messageStatus = messageStatus;
        }
    }


    private OnflightMessageTracker() {

        this.maximumRedeliveryTimes = ClusterResourceHolder.getInstance().getClusterConfiguration()
                                                           .getNumberOfMaximumDeliveryCount();
        this.staleMsgDataRemovalTimeout = 60*60*1000;
        /*
         * for all add and remove, following is executed, and it will remove the oldest entry if
         * needed
         */
        msgId2MsgData = new LinkedHashMap<Long, MsgData>() {
            private static final long serialVersionUID = -8681132571102532817L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, MsgData> eldest) {
                MsgData msgData = eldest.getValue();
                boolean toDelete = (System.currentTimeMillis() - msgData.timestamp) > staleMsgDataRemovalTimeout;
                if (toDelete) {
                    if (!msgData.ackreceived) {
                        //reduce messages on flight on this channel
                        msgData.channel.decrementNonAckedMessageCount();
                        log.debug("No ack received for delivery tag " + msgData.deliveryID + " and " +
                                  "message id " + msgData.msgID + " Message status = " + msgData.messageStatus);
                    }
                    //permanently remove tracking of message
                    stampMessageAsDLCAndRemoveFromTacking(msgData.channel.getId(), msgData.slot,
                                                          msgData.msgID);
                }
                return toDelete;
            }
        };

        /**
         * Keep processed slots. This removes oldest slot as a new one comes in.
         * When a slot is removed we remove all the tracing of those messages that they are
         * buffered from memory.
         */
        locallyProcessedSlots = new LinkedHashMap<String, Slot>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Slot> eldest) {
                Slot eldestSlot = eldest.getValue();
                boolean isPipelineFilled = locallyProcessedSlots.size() > numberOfSlotsToKeepTracing;
                if (isPipelineFilled) {
                    //this will not remove all the trackings.
                    // It will be considered again if memory is growing (last is removed when new one comes)
                    releaseAllMessagesOfSlotFromTracking(eldestSlot);
                }
                return isPipelineFilled;
            }
        };

    }

    /**
     * record that this node assign a slot to process. This will automatically
     * remove all tracking of eldest slot in locallyProcessedSlots map
     * @param slot slot assigned
     */
    public void recordSlotAssignment(Slot slot) {
        locallyProcessedSlots.put(slot.getId(),slot);
    }

    /**
     * Message has failed to process by client. Re-buffer the message
     * @param messageId  id of the message rejected
     * @param channelId id of the channel rejected
     * @throws AMQException
     */
    public void handleFailure(long messageId, UUID channelId) throws AMQException {
        try {
            stampMessageAsRejected(channelId, messageId);
            //re-queue the message to send again
            reQueueMessage(messageId);
        } catch (AndesException e) {
            log.warn("Message " + messageId + "re-queueing failed");
            throw new AMQException(AMQConstant.INTERNAL_ERROR,
                                   "Message " + messageId + "re-queueing failed", e);
        }
    }

    /**
     * Re-queue the message to be sent again
     * @param messageId id of the message to re queue
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
     * @return boolean if the message should be sent
     */
    public boolean testAndAddMessage(AndesMessageMetadata andesMetaDataEntry, AMQChannel channel)
            throws AMQException {

        long messageId = andesMetaDataEntry.getMessageID();

        //Add redelivery header if being redelivered
        boolean isRedelivered = addMessageToSendingTracker(channel.getId(), messageId);
        if (isRedelivered) {
            andesMetaDataEntry.setRedelivered();
        }

        return evaluateDeliveryRules(andesMetaDataEntry);

    }

    /**
     * Any custom checks or procedures that should be executed before message delivery should
     * happen here. Any message rejected at this stage will be sent to DLC
     * @param andesMetaDataEntry message metadata entry to evaluate for delivery
     * @return eligibility deliver
     */
    private boolean evaluateDeliveryRules(AndesMessageMetadata andesMetaDataEntry) {
        boolean isOKToDeliver = true;
        long messageId = andesMetaDataEntry.getMessageID();
        String destination = andesMetaDataEntry.getDestination();

        //check if number of redelivery tries has breached.
        int numOfDeliveriesOfCurrentMsg = getDeliveryCountOfMessage(messageId);
        if (numOfDeliveriesOfCurrentMsg > maximumRedeliveryTimes) {
            log.warn("Number of Maximum Redelivery Tries Has Breached. Dropping The Message: " +
                     messageId + "From Queue " + destination);
            isOKToDeliver =  false;
            //check if queue entry has expired. Any expired message will not be delivered
        } else if (andesMetaDataEntry.isExpired()) {
            stampMessageAsExpired(andesMetaDataEntry.getMessageID());
            log.warn("Message is expired. Dropping The Message: " + messageId);
            isOKToDeliver =  false;
        }

        return isOKToDeliver;
    }

    /**
     * decrement message count in slot and if it is zero check the slot again to resend
     *
     * @param slot Slot to check
     * @param queueName name of queue slot bares
     * @throws AndesException
     */
    public void decrementMessageCountInSlotAndCheckToResend(Slot slot, String queueName)
            throws AndesException {
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
     * increment the message count in a slot
     *
     * @param slot slot whose message counter should increment
     */
    public void incrementMessageCountInSlot(Slot slot) {
        AtomicInteger pendingMessageCount = pendingMessagesBySlot.get(slot);
        if (pendingMessageCount == null) {
            pendingMessagesBySlot.putIfAbsent(slot, new AtomicInteger());
            pendingMessageCount = pendingMessagesBySlot.get(slot);
        }
        pendingMessageCount.incrementAndGet();
    }

    //handle ack

    /**
     * Track acknowledgement for message
     * @param channel channel of the ack
     * @param messageID id of the message ack is for
     */
    public void handleAckReceived(UUID channel, long messageID) {
        log.debug("Ack Received id= " + messageID);
        MsgData trackingData = getTrackingData(messageID);
        trackingData.ackreceived = true;
        setMessageStatus(MessageStatus.Acked, trackingData);
        sendButNotAckedMessageCount.decrementAndGet();
        //record how much time took between delivery and ack receive
        long timeTook = (System.currentTimeMillis() - trackingData.timestamp);
        PerformanceCounter.recordAckReceived(trackingData.queue, (int) timeTook);
        //release delivery tracing
        releaseMessageDeliveryFromTracking(channel, messageID);
    }

    /**
     * Track reject of the message
     * @param channel channel of the message reject
     * @param messageID id of the message reject represent
     */
    public void stampMessageAsRejected(UUID channel, long messageID) {
        log.debug("stamping message as rejected id = " + messageID);
        MsgData trackingData = getTrackingData(messageID);
        trackingData.timestamp = System.currentTimeMillis();
        sendButNotAckedMessageCount.decrementAndGet();
        trackingData.messageStatus = MessageStatus.RejectedAndBuffered;
        //release delivery tracing
        releaseMessageDeliveryFromTracking(channel, messageID);
    }

    /**
     * Track that this message is buffered. Return thre if eligible to buffer
     * @param slot slot message being read in
     * @param andesMessageMetadata metadata to buffer
     * @return  eligibility to buffer
     */
    public boolean addMessageToBufferingTracker(Slot slot, AndesMessageMetadata andesMessageMetadata) {
        long messageID = andesMessageMetadata.getMessageID();
        boolean isOKToBuffer = true;
        log.debug("Buffering message id = " + messageID + " slot = " + slot.toString());
        ConcurrentHashMap<Long, MsgData> messagesOfSlot = messageBufferingTracker.get(slot);
        if (messagesOfSlot == null) {
            messagesOfSlot = new ConcurrentHashMap<Long, MsgData>();
            messageBufferingTracker.put(slot, messagesOfSlot);
        }
        MsgData trackingData = messagesOfSlot.get(messageID);
        if (trackingData == null) {
            trackingData = new MsgData(messageID, slot, false,
                                       andesMessageMetadata.getDestination(),
                                       System.currentTimeMillis(), null, null, 0,
                                       MessageStatus.Read);
            msgId2MsgData.put(messageID, trackingData);
            messagesOfSlot.put(messageID, msgId2MsgData.get(messageID));
            isOKToBuffer =  true;
        } else {
            log.debug("Buffering rejected message id = " + messageID);
            isOKToBuffer =  false;
        }
        return isOKToBuffer;
    }

    /**
     * Release tracking of all messages belonging to a slot. i.e called when slot is removed.
     * This will remove all buffering tracking of messages and tracking objects.
     * But tracking objects will remain until delivery cycle completed
     * @param slot slot to release
     */
    public void releaseAllMessagesOfSlotFromTracking(Slot slot) {
        //remove all actual msgData objects
        log.debug("Releasing tracking of messages for slot " + slot.toString());
        ConcurrentHashMap<Long, MsgData> messagesOfSlot = messageBufferingTracker.remove(slot);
        for (Long messageIdOfSlot : messagesOfSlot.keySet()) {
            if(checkIfReadyToRemoveFromTracking(messageIdOfSlot)) {
                msgId2MsgData.remove(messageIdOfSlot);
            }
        }
    }

    /**
     * Remove tracking object from memory for a message if this returns true
     * @param messageID  id of the message to evaluate
     * @return eligibility to delete tracking object
     */
    private boolean checkIfReadyToRemoveFromTracking(long messageID) {
        MsgData messageTrackingData = getTrackingData(messageID);
        return  MessageStatus.isOKToRemove(messageTrackingData.messageStatus);
    }

    /**
     * Release tracking of all messages sent from a channel
     * @param channelID  id of the channel
     */
    public void releaseAllMessagesOfChannelFromTracking(UUID channelID) {
        log.debug("Releasing tracking of messages sent by channel id = " + channelID);
        messageSendingTracker.remove(channelID);
    }

    /**
     * Release tracking that this message is delivered.
     * @param channelID id of the channel
     * @param messageID id of the message to remove
     */
    public void releaseMessageDeliveryFromTracking(UUID channelID, long messageID) {
        log.debug("Releasing tracking of message sent id= " + messageID);
        messageSendingTracker.get(channelID).remove(messageID);
    }

    /**
     * Release tracking that this message is buffered.
     * This will delete reference to tracking object only
     * @param slot  slot message belongs
     * @param messageId id of the message
     */
    public void releaseMessageBufferingFromTracking(Slot slot, long messageId) {
        log.debug("Releasing message buffering tacking id= " + messageId);
        messageBufferingTracker.get(slot).remove(messageId);
    }

    /**
     * Increment delivery count in tracking object for message when delivered
     * @param messageId id of the message
     * @return number of current deliveries
     */
    public int incrementDeliveryCountOfMessage(long messageId) {
        MsgData trackingData = getTrackingData(messageId);
        trackingData.numOfDeliveries += 1;
        return trackingData.numOfDeliveries;
    }

    /**
     * Get how many times a message is delivered to client
     * @param messageID  id of the message
     * @return num of deliveries
     */
    public int getDeliveryCountOfMessage(long messageID) {
        return getTrackingData(messageID).numOfDeliveries;
    }

    /**
     * Set message status for a message.
     * This can be buffered, sent, rejected etc
     * @param messageStatus status of the message
     * @param msgData  message tracking object
     */
    public void setMessageStatus(MessageStatus messageStatus, MsgData msgData) {
        msgData.messageStatus = messageStatus;
    }

    /**
     * Set message status as expired
     * @param messageID id of the message to set expired
     */
    public void stampMessageAsExpired(long messageID) {
        getTrackingData(messageID).messageStatus = MessageStatus.Expired;
    }

    /**
     * Get the current status of the message in delivery pipeline
     * @param messageID id of the message to get status
     * @return status of the message
     */
    public MessageStatus getMessageStatus(long messageID) {
        return getTrackingData(messageID).messageStatus;
    }

    /**
     * Get message tracking object for a message. This contains
     * all delivery information and message status of the message
     * @param messageID id of the message
     * @return  tracking object for message
     */
    public MsgData getTrackingData(long messageID) {
        return msgId2MsgData.get(messageID);
    }

    //track message sending. return true if a redelivered message

    /**
     * Stamp a message as sent. This method also evaluate if the
     * message is eligible to be sent to subscribers
     * @param channelID id of the connection message is delivering to subscriber
     * @param messageID id of the message
     * @return  eligibility to deliver
     */
    public boolean addMessageToSendingTracker(UUID channelID, long messageID) {
        log.debug(
                "Tracing the sent message channel id = " + channelID + " message id = " +
                messageID);
        ConcurrentHashMap<Long, MsgData> messagesSentByChannel = messageSendingTracker
                .get(channelID);
        if (messagesSentByChannel == null) {
            messagesSentByChannel = new ConcurrentHashMap<Long, MsgData>();
            messageSendingTracker.put(channelID, messagesSentByChannel);
        }
        MsgData trackingData = messagesSentByChannel.get(messageID);
        if (trackingData == null) {
            trackingData = msgId2MsgData.get(messageID);
            messagesSentByChannel.put(messageID, trackingData);
        }
        // increase delivery count
        incrementDeliveryCountOfMessage(messageID);

        sendButNotAckedMessageCount.incrementAndGet();

        trackingData.messageStatus = MessageStatus.Sent;

        //check if this is a redelivered message
        return trackingData.numOfDeliveries > 1;
    }

    /**
     * Permanently remove message from tacker. This will clear the tracking
     * that message is buffered and message is sent and also will remove
     * tracking object from memory
     * @param channelID channel message is delivered to subscriber
     * @param slot  slot message is read from
     * @param messageID id of the message
     */
    public void stampMessageAsDLCAndRemoveFromTacking(UUID channelID, Slot slot, long messageID) {
        log.debug("Removing tracking of message id = " + messageID);
        releaseMessageDeliveryFromTracking(channelID, messageID);
        releaseMessageBufferingFromTracking(slot, messageID);
        //remove actual object from memory
        msgId2MsgData.remove(messageID);
    }

}