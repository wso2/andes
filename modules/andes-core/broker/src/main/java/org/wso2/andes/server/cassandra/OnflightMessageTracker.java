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

    /**
     * This will keep all the message metadata delivered. As soon as ack is received
     * this object is removed
     */
/*    private ConcurrentHashMap<Long, AndesMessageMetadata> messageIdToAndesMessagesMap = new
            ConcurrentHashMap<Long, AndesMessageMetadata>();*/

    /**
     * Map to keep track of message counts pending to read
     */
    private ConcurrentHashMap<Slot, AtomicInteger> pendingMessagesBySlot = new
            ConcurrentHashMap<Slot, AtomicInteger>();

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
        long expirationTime;
        long deliveryID;
        UUID channelId;
        int numOfDeliveries;
        MessageStatus messageStatus;
        Slot slot;

        private MsgData(long msgID, Slot slot, boolean ackReceived, String queue, long timestamp,
                        long expirationTime, long deliveryID, UUID channelId, int numOfDeliveries,
                        MessageStatus messageStatus) {
            this.msgID = msgID;
            this.slot = slot;
            this.ackreceived = ackReceived;
            this.queue = queue;
            this.timestamp = timestamp;
            this.expirationTime = expirationTime;
            this.deliveryID = deliveryID;
            this.channelId = channelId;
            this.numOfDeliveries = numOfDeliveries;
            this.messageStatus = messageStatus;
        }

        private boolean isExpired() {
            if (expirationTime != 0L)
            {
                long now = System.currentTimeMillis();
                return (now > expirationTime);
            } else {
                return false;
            }
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
                        //msgData.channel.decrementNonAckedMessageCount();
                        log.debug("No ack received for delivery tag " + msgData.deliveryID + " and " +
                                  "message id " + msgData.msgID + " Message status = " + msgData.messageStatus);
                    }
                    //permanently remove tracking of message
                    log.warn("Loosing track of message id= " + msgData.msgID);
                    try {
                        stampMessageAsDLCAndRemoveFromTacking(msgData.msgID);
                    } catch (AndesException e) {
                        log.error("Error when loosing track of message id= " + msgData.msgID);
                    }
                }
                return toDelete;
            }
        };

    }

    /**
     * Message has failed to process by client. Re-buffer the message
     * @param metadata metadata of message rejected
     * @throws AndesException
     */
    public void handleFailure(AndesMessageMetadata metadata) throws AndesException {
        long messageId = metadata.getMessageID();
        UUID channelId = metadata.getChannelId();
        if(log.isDebugEnabled()) {
            log.debug("message was rejected by client id= " + messageId + " channel= " + channelId);
        }
        stampMessageAsRejected(channelId, messageId);
        //re-queue the message to send again
        reQueueMessage(metadata);
    }

    /**
     * Re-queue the message to be sent again
     * @param metadata message to re queue
     */
    public void reQueueMessage(AndesMessageMetadata metadata) throws AndesException {
        QueueDeliveryWorker.QueueDeliveryInfo queueDeliveryInfo = QueueDeliveryWorker.getInstance().
                getQueueDeliveryInfo(metadata.getDestination());
        if (log.isDebugEnabled()) {
            log.debug("Re-Queueing message " + metadata.getMessageID() + " to readButUndeliveredMessages map");
        }
        queueDeliveryInfo.readButUndeliveredMessages.add(metadata);
    }

    /**
     * Register that this message is being delivered to client
     *
     * @return boolean if the message is being redelivered
     */
    public boolean checkAndRegisterSent(long messageId, UUID channelID, long deliverTag)
            throws AMQException {

        MsgData trackingData = msgId2MsgData.get(messageId);
        trackingData.channelId = channelID;
        trackingData.deliveryID = deliverTag;

        //Add redelivery header if being redelivered
        boolean isRedelivered = addMessageToSendingTracker(channelID, messageId);

        return isRedelivered;

    }

    /**
     * Any custom checks or procedures that should be executed before message delivery should
     * happen here. Any message rejected at this stage will be sent to DLC
     * @param messageId id of message metadata entry to evaluate for delivery
     * @return eligibility deliver
     */
    public boolean evaluateDeliveryRules(long messageId) {
        boolean isOKToDeliver = true;
        MsgData trackingData = getTrackingData(messageId);

        //check if number of redelivery tries has breached.
        int numOfDeliveriesOfCurrentMsg = getDeliveryCountOfMessage(messageId);
        if (numOfDeliveriesOfCurrentMsg > maximumRedeliveryTimes) {
            log.warn("Number of Maximum Redelivery Tries Has Breached. Routing Message to DLC : id= " +
                     messageId);
            isOKToDeliver =  false;
            //check if queue entry has expired. Any expired message will not be delivered
        } else if (trackingData.isExpired()) {
            stampMessageAsExpired(messageId);
            log.warn("Message is expired. Routing Message to DLC : id= " + messageId);
            isOKToDeliver =  false;
        }
        if(isOKToDeliver) {
            if(numOfDeliveriesOfCurrentMsg == 1) {
                trackingData.messageStatus = MessageStatus.Sent;
            } else if(numOfDeliveriesOfCurrentMsg > 1) {
                trackingData.messageStatus = MessageStatus.Resent;
            }

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
            if(log.isDebugEnabled()) {
                log.debug("Slot has no pending messages. Now re-checking slot for messages");
            }
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

    /**
     * Track acknowledgement for message
     * @param channel channel of the ack
     * @param messageID id of the message ack is for
     */
    public void handleAckReceived(UUID channel, long messageID) throws AndesException {
        if(log.isDebugEnabled()) {
            log.debug("Ack Received message id= " + messageID + " channel id= " + channel);
        }
        MsgData trackingData = getTrackingData(messageID);
        trackingData.ackreceived = true;
        setMessageStatus(MessageStatus.Acked, trackingData);
        sendButNotAckedMessageCount.decrementAndGet();
        //record how much time took between delivery and ack receive
        long timeTook = (System.currentTimeMillis() - trackingData.timestamp);
        PerformanceCounter.recordAckReceived(trackingData.queue, (int) timeTook);
        decrementMessageCountInSlotAndCheckToResend(trackingData.slot, trackingData.queue);
        //release delivery tracing
        releaseMessageDeliveryFromTracking(channel, messageID);
    }

    /**
     * Track reject of the message
     * @param channel channel of the message reject
     * @param messageID id of the message reject represent
     */
    public void stampMessageAsRejected(UUID channel, long messageID) {
        if(log.isDebugEnabled()) {
            log.debug("stamping message as rejected id = " + messageID);
        }
        MsgData trackingData = getTrackingData(messageID);
        trackingData.timestamp = System.currentTimeMillis();
        sendButNotAckedMessageCount.decrementAndGet();
        trackingData.messageStatus = MessageStatus.RejectedAndBuffered;
        //release delivery tracing
        releaseMessageDeliveryFromTracking(channel, messageID);
    }

    /**
     * Track that this message is buffered. Return true if eligible to buffer
     * @param slot slot message being read in
     * @param andesMessageMetadata metadata to buffer
     * @return  eligibility to buffer
     */
    public boolean addMessageToBufferingTracker(Slot slot, AndesMessageMetadata andesMessageMetadata) {
        long messageID = andesMessageMetadata.getMessageID();
        boolean isOKToBuffer;
        if(log.isDebugEnabled()) {
            log.debug("Buffering message id = " + messageID + " slot = " + slot.toString());
        }
        ConcurrentHashMap<Long, MsgData> messagesOfSlot = messageBufferingTracker.get(slot);
        if (messagesOfSlot == null) {
            messagesOfSlot = new ConcurrentHashMap<Long, MsgData>();
            messageBufferingTracker.put(slot, messagesOfSlot);
        }
        MsgData trackingData = messagesOfSlot.get(messageID);
        if (trackingData == null) {
            trackingData = new MsgData(messageID, slot, false,
                                       andesMessageMetadata.getDestination(),
                                       System.currentTimeMillis(), andesMessageMetadata.getExpirationTime(),
                                       0, null, 0,MessageStatus.Buffered);
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
     * Check if a message is already buffered without adding it to the buffer
     * @param slot slot of the message
     * @param messageID id of the message
     * @return if message is already been buffered
     */
    public boolean checkIfMessageIsAlreadyBuffered(Slot slot, long messageID) {
        boolean isAlreadyBuffered = false;
        MsgData trackingData = messageBufferingTracker.get(slot).get(messageID);
        if(trackingData != null) {
           isAlreadyBuffered = true;
        }
        return isAlreadyBuffered;
    }

    /**
     * Release tracking of all messages belonging to a slot. i.e called when slot is removed.
     * This will remove all buffering tracking of messages and tracking objects.
     * But tracking objects will remain until delivery cycle completed
     * @param slot slot to release
     */
    public void releaseAllMessagesOfSlotFromTracking(Slot slot) {
        //remove all actual msgData objects
        if(log.isDebugEnabled()) {
            log.debug("Releasing tracking of messages for slot " + slot.toString());
        }
        ConcurrentHashMap<Long, MsgData> messagesOfSlot = messageBufferingTracker.remove(slot);
        for (Long messageIdOfSlot : messagesOfSlot.keySet()) {
            if(checkIfReadyToRemoveFromTracking(messageIdOfSlot)) {
                if(log.isDebugEnabled()) {
                    log.debug("removing tracking object from memory id= " + messageIdOfSlot);
                }
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
        if(log.isDebugEnabled()) {
            log.debug("Releasing tracking of messages sent by channel id = " + channelID);
        }
        messageSendingTracker.remove(channelID);
    }

    /**
     * Release tracking that this message is delivered.
     * @param channelID id of the channel
     * @param messageID id of the message to remove
     */
    public void releaseMessageDeliveryFromTracking(UUID channelID, long messageID) {
        if(log.isDebugEnabled()) {
            log.debug("Releasing tracking of message sent id= " + messageID);
        }
        messageSendingTracker.get(channelID).remove(messageID);
    }

    /**
     * Release tracking that this message is buffered.
     * This will delete reference to tracking object only
     * @param slot  slot message belongs
     * @param messageId id of the message
     */
    public void releaseMessageBufferingFromTracking(Slot slot, long messageId) {
        if(log.isDebugEnabled()) {
            log.debug("Releasing message buffering tacking id= " + messageId);
        }
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
        if(log.isDebugEnabled()) {
            log.debug("Adding message to sending tracker channel id = " + channelID + " message id = " +
                    messageID);
        }
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
        int numOfCurrentDeliveries = incrementDeliveryCountOfMessage(messageID);

        if(log.isDebugEnabled()) {
            log.debug("Number of current deliveries for message id= " + messageID + " is " + numOfCurrentDeliveries);
        }

        sendButNotAckedMessageCount.incrementAndGet();

        //check if this is a redelivered message
        return trackingData.numOfDeliveries > 1;
    }

    /**
     * Permanently remove message from tacker. This will clear the tracking
     * that message is buffered and message is sent and also will remove
     * tracking object from memory
     * @param messageID id of the message
     */
    public void stampMessageAsDLCAndRemoveFromTacking(long messageID) throws AndesException {
        //remove actual object from memory
        if(log.isDebugEnabled()) {
            log.debug("Removing all tracking of message id = " + messageID);
        }
        MsgData trackingData = msgId2MsgData.remove(messageID);
        UUID channelID = trackingData.channelId;
        String queueName = trackingData.queue;
        Slot slot = trackingData.slot;
        releaseMessageDeliveryFromTracking(channelID, messageID);
        releaseMessageBufferingFromTracking(slot, messageID);

        decrementMessageCountInSlotAndCheckToResend(slot, queueName);
    }

}