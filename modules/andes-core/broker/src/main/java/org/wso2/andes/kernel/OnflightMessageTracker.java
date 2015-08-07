/*
 *
 *   Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.kernel.slot.SlotDeliveryWorker;
import org.wso2.andes.kernel.slot.SlotDeliveryWorkerManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class will track message delivery by broker
 * on the fly. Message delivery times, message status,
 * is tracked here
 */
public class OnflightMessageTracker {

    private static Log log = LogFactory.getLog(OnflightMessageTracker.class);

    private static OnflightMessageTracker instance;

    static {
        try {
            instance = new OnflightMessageTracker();
        } catch (AndesException e) {
            log.error("Error occurred when reading configurations : ", e);
        }
    }


    public static OnflightMessageTracker getInstance() {
        return instance;
    }

    /**
     * In memory map keeping sent message statistics by message id
     */
    private final ConcurrentHashMap<Long, MessageData> msgId2MsgData;

    /**
     * Map to track messages being buffered to be sent <Id of the slot, messageID, MsgData
     * reference>
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<Long, MessageData>> messageBufferingTracker
            = new ConcurrentHashMap<String, ConcurrentHashMap<Long, MessageData>>();

    /**
     * Map to track messages being sent <channel id, message id, MsgData reference>
     */
    private final ConcurrentHashMap<UUID, ConcurrentHashMap<Long, MessageData>> messageSendingTracker
            = new ConcurrentHashMap<UUID, ConcurrentHashMap<Long, MessageData>>();

    /**
     * Map to keep track of message counts pending to read
     */
    private final ConcurrentHashMap<Slot, AtomicInteger> pendingMessagesBySlot = new
            ConcurrentHashMap<Slot, AtomicInteger>();

    /**
     * Class to keep tracking data of a message
     */

    private OnflightMessageTracker() throws AndesException {

        // We don't know the size of the map at startup. hence using an arbitrary value of 16, Need to test
        // Load factor set to default value 0.75
        // Concurrency level set to 6. Currently SlotDeliveryWorker, AckHandler AckSubscription, DeliveryEventHandler,
        // MessageFlusher access this. To be on the safe side set to 6.
        msgId2MsgData = new ConcurrentHashMap<Long, MessageData>(16, 0.75f, 6);

    }

    /**
     * Message has failed to process by client. Re-buffer the message
     *
     * @param metadata metadata of message rejected
     * @throws AndesException
     */
    public void handleFailure(AndesMessageMetadata metadata) throws AndesException {
        long messageId = metadata.getMessageID();
        UUID channelId = metadata.getChannelId();
        if (log.isDebugEnabled()) {
            log.debug("message was rejected by client id= " + messageId + " channel= " + channelId);
        }
        stampMessageAsRejected(channelId, messageId);
    }

    /**
     * To get number of deliveries to a particular channel for a particular message
     *
     * @param messageID MessageID id of the message
     * @param channelID ChannelID id of the subscriber's channel
     * @return Number of message deliveries for this particular channel
     */
    public int getNumOfMsgDeliveriesForChannel(long messageID, UUID channelID) {
        if (null != getTrackingData(messageID)) {
            return getTrackingData(messageID).getNumOfDeliveires4Channel(channelID);
        } else {
            return 0;
        }
    }

    /**
     * To get arrival time of the message
     *
     * @param messageID Id of the message
     * @return Message arrival time
     */
    public long getMsgArrivalTime(long messageID) {
        return getTrackingData(messageID).arrivalTime;
    }

    /**
     * To check whether message is expired or not
     *
     * @param messageID Id of the message
     * @return Msg is expired or not (boolean value)
     */
    public boolean isMsgExpired(long messageID) {
        return getTrackingData(messageID).isExpired();
    }

    /**
     * To get destination of the message
     *
     * @param messageID Id of the message
     * @return Destination of the message
     */
    public String getMsgDestination(long messageID) {
        return getTrackingData(messageID).destination;
    }

    /**
     * Decrement message count in slot and if it is zero prepare for slot deletion
     *
     * @param slot Slot whose message count is decremented
     * @throws AndesException
     */
    public void decrementMessageCountInSlot(Slot slot)
            throws AndesException {
        AtomicInteger pendingMessageCount = pendingMessagesBySlot.get(slot);
        int messageCount = pendingMessageCount.decrementAndGet();
        if (messageCount == 0) {
            /*
            All the Acks for the slot has bee received. Check the slot again for unsend
            messages and if there are any send them and delete the slot.
             */
            SlotDeliveryWorker slotWorker = SlotDeliveryWorkerManager.getInstance()
                                                                     .getSlotWorker(slot.getStorageQueueName());
            if (log.isDebugEnabled()) {
                log.debug("Slot has no pending messages. Now re-checking slot for messages");
            }
            slot.setSlotInActive();
            slotWorker.deleteSlot(slot);
        }
    }

    /**
     * Increment the message count in a slot
     *
     * @param slot slot whose message counter should increment
     */
    public void incrementMessageCountInSlot(Slot slot, int amount) {
        AtomicInteger pendingMessageCount = pendingMessagesBySlot.get(slot);
        if (null == pendingMessageCount) {
            pendingMessageCount = new AtomicInteger();
            pendingMessagesBySlot.putIfAbsent(slot, pendingMessageCount);
        }
        pendingMessageCount.addAndGet(amount);
    }

    /**
     * Track acknowledgement for message. When acknowledgement received this method will remove the
     * tracking information from tracking lists.
     *
     * @param channel   channel of the ack
     * @param messageID id of the message ack is for
     * @return if message is OK to delete (all acks received)
     * @throws AndesException
     */
    public boolean handleAckReceived(UUID channel, long messageID) throws AndesException {

        if (log.isDebugEnabled()) {
            log.debug("Ack Received message id= " + messageID + " channel id= " + channel);
        }

        boolean isOKToDeleteMessage = false;

        //release delivery tracing
        MessageData trackingData = getTrackingData(messageID);

        //decrement delivery count
        if (trackingData != null) {
            trackingData.decrementDeliveryCount(channel);

            trackingData.addMessageStatus(MessageStatus.ACKED);

            //we consider ack is received if all acks came for channels message was sent
            if (trackingData.allAcksReceived() && getNumberOfScheduledDeliveries(messageID) == 0) {
                trackingData.addMessageStatus(MessageStatus.ACKED_BY_ALL);

                isOKToDeleteMessage = true;
                if (log.isDebugEnabled()) {
                    log.debug("OK to remove message from store as all acks are received id= " + messageID);
                }
            }
        } else {
            log.error("Could not find tracking data for message id " + messageID + " and channel " + channel);
        }

        return isOKToDeleteMessage;
    }

    /**
     * Track reject of the message
     *
     * @param channel   channel of the message reject
     * @param messageID id of the message reject represent
     */
    public void stampMessageAsRejected(UUID channel, long messageID) {
        if (log.isDebugEnabled()) {
            log.debug("stamping message as rejected id = " + messageID);
        }
        MessageData trackingData = getTrackingData(messageID);
        if (trackingData != null) {
            trackingData.timestamp = System.currentTimeMillis();
            trackingData.addMessageStatus(MessageStatus.REJECTED_AND_BUFFERED);
        }
    }

    /**
     * Track that this message is buffered. Return true if eligible to buffer
     *
     * @param slot                 slot message being read in
     * @param andesMessageMetadata metadata to buffer
     * @return eligibility to buffer
     */
    public boolean addMessageToBufferingTracker(Slot slot, AndesMessageMetadata andesMessageMetadata) {
        long messageID = andesMessageMetadata.getMessageID();
        boolean isOKToBuffer;
        if (log.isDebugEnabled()) {
            log.debug("Buffering message id = " + messageID + " slot = " + slot.toString());
        }
        String slotID = slot.getId();
        ConcurrentHashMap<Long, MessageData> messagesOfSlot = messageBufferingTracker.get(slotID);
        if (messagesOfSlot == null) {
            messagesOfSlot = new ConcurrentHashMap<Long, MessageData>();
            messageBufferingTracker.put(slotID, messagesOfSlot);
        }
        MessageData trackingData = messagesOfSlot.get(messageID);
        if (trackingData == null) {
            trackingData = new MessageData(messageID, slot,
                    slot.getDestinationOfMessagesInSlot(),
                    System.currentTimeMillis(),
                    andesMessageMetadata.getExpirationTime(),
                    MessageStatus.BUFFERED, andesMessageMetadata.getArrivalTime());
            msgId2MsgData.put(messageID, trackingData);
            messagesOfSlot.put(messageID, trackingData);
            isOKToBuffer = true;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Buffering rejected message id = " + messageID);
            }
            isOKToBuffer = false;
        }
        return isOKToBuffer;
    }

    /**
     * Check if a message is already buffered without adding it to the buffer
     *
     * @param slot      slot of the message
     * @param messageID id of the message
     * @return if message is already been buffered
     */
    public boolean checkIfMessageIsAlreadyBuffered(Slot slot, long messageID) {
        boolean isAlreadyBuffered = false;
        String slotID = slot.getId();
        MessageData trackingData = messageBufferingTracker.get(slotID).get(messageID);
        if (trackingData != null) {
            isAlreadyBuffered = true;
        }
        return isAlreadyBuffered;
    }

    /**
     * Release tracking of all messages belonging to a slot. i.e called when slot is removed. This will remove all
     * buffering tracking of messages and tracking objects. But tracking objects will remain until delivery cycle
     * completed
     *
     * @param slot
     *         slot to release
     */
    public void releaseAllMessagesOfSlotFromTracking(Slot slot) {
        //remove all actual msgData objects
        if (log.isDebugEnabled()) {
            log.debug("Releasing tracking of messages for slot " + slot.toString());
        }
        String slotID = slot.getId();
        ConcurrentHashMap<Long, MessageData> messagesOfSlot = messageBufferingTracker.remove(slotID);
        if (messagesOfSlot != null) {
            for (Long messageId : messagesOfSlot.keySet()) {
                MessageData msgData = getTrackingData(messageId);
                msgData.addMessageStatus(MessageStatus.SLOT_REMOVED);
                if (checkIfReadyToRemoveFromTracking(messageId)) {
                    if (log.isDebugEnabled()) {
                        log.debug("removing tracking object from memory id " + messageId);
                    }
                } else {
                    log.error("Tracking data for message id " + messageId + " removed while in an invalid state. (" + msgData.getStatusHistory() + ")");
                }
                msgId2MsgData.remove(messageId);
            }
        }
    }

    /**
     * Report delivered messages for slot state update
     *
     * @param messagesToRemove
     *         List messages removed from store
     * @throws AndesException
     */
    public void updateMessageDeliveryInSlot(List<AndesRemovableMetadata> messagesToRemove) throws AndesException {
        for (AndesRemovableMetadata message : messagesToRemove) {
            MessageData trackingData = getTrackingData(message.getMessageID());
            decrementMessageCountInSlot(trackingData.slot);
        }
    }

    /**
     * Remove tracking object from memory for a message if this returns true
     *
     * @param messageID id of the message to evaluate
     * @return eligibility to delete tracking object
     */
    private boolean checkIfReadyToRemoveFromTracking(long messageID) {
        MessageData messageTrackingData = getTrackingData(messageID);
        return MessageStatus.isOKToRemove(messageTrackingData.messageStatus);
    }

    /**
     * Clear all tracking when orphan slot situation i.e. call when no active subscription but buffered messages are
     * sent to subscription
     *
     * @param slot slot to release
     */
    public void clearAllTrackingWhenSlotOrphaned(Slot slot) {
        if (log.isDebugEnabled()) {
            log.debug("Orphan slot situation and clear tracking of messages for slot = " + slot);
        }
        String slotID = slot.getId();
        ConcurrentHashMap<Long, MessageData> messagesOfSlot = messageBufferingTracker.remove(slotID);
        if (messagesOfSlot != null) {
            for (Long messageId : messagesOfSlot.keySet()) {
                msgId2MsgData.remove(messageId);
            }
        }
    }

    /**
     * Release tracking that this message is buffered.
     * This will delete reference to tracking object only
     *
     * @param slot      slot message belongs
     * @param messageId id of the message
     */
    public void releaseMessageBufferingFromTracking(Slot slot, long messageId) {
        if (log.isDebugEnabled()) {
            log.debug("Releasing message buffering tacking id= " + messageId);
        }
        String slotID = slot.getId();
        messageBufferingTracker.get(slotID).remove(messageId);
    }

    /**
     * Set message status for a message.
     * This can be buffered, sent, rejected etc
     *
     * @param messageStatus status of the message
     * @param messageID Id of the message to set expired
     */
    public void setMessageStatus(MessageStatus messageStatus, long messageID) {
        MessageData trackingData = getTrackingData(messageID);
        if (null != trackingData) {
            trackingData.addMessageStatus(messageStatus);
        }
    }

    /**
     * Set message status as expired
     *
     * @param messageID id of the message to set expired
     */
    public void stampMessageAsExpired(long messageID) {
        getTrackingData(messageID).addMessageStatus(MessageStatus.EXPIRED);
    }

    /**
     * Get the current status of the message in delivery pipeline
     *
     * @param messageID id of the message to get status
     * @return status of the message
     */
    public MessageStatus getMessageStatus(long messageID) {
        return getTrackingData(messageID).getLatestState();
    }

    /**
     * Get message tracking object for a message. This contains
     * all delivery information and message status of the message
     *
     * @param messageID id of the message
     * @return tracking object for message
     */
    public MessageData getTrackingData(long messageID) {
        return msgId2MsgData.get(messageID);
    }

    /**
     * Permanently remove message from tacker. This will clear the tracking
     * that message is buffered and message is sent and also will remove
     * tracking object from memory
     *
     * @param messageID id of the message
     */
    public void stampMessageAsDLCAndRemoveFromTacking(long messageID) throws AndesException {
        //remove actual object from memory
        if (log.isDebugEnabled()) {
            log.debug("Removing all tracking of message id = " + messageID);
        }
        MessageData trackingData = msgId2MsgData.remove(messageID);
        Slot slot = trackingData.slot;
        for (UUID channelID : trackingData.getAllDeliveredChannels()) {
            LocalSubscription subscription = AndesContext.getInstance().getSubscriptionStore()
                    .getLocalSubscriptionForChannelId(channelID);
            if(null != subscription) {
                subscription.msgRejectReceived(messageID);
            }
        }

        releaseMessageBufferingFromTracking(slot, messageID);

        decrementMessageCountInSlot(slot);
    }

    /**
     * Increment number of times this message is scheduled to be delivered
     * to different subscribers. This value will be equal to the number
     * of subscribers expecting the message at that instance.
     *
     * @param messageID identifier of the message
     * @return num of scheduled times after increment
     */
    public int incrementNumberOfScheduledDeliveries(long messageID) {
        MessageData trackingData = getTrackingData(messageID);
        int numOfSchedules = 0;
        if (trackingData != null) {
            trackingData.addMessageStatus(MessageStatus.SCHEDULED_TO_SEND);
            numOfSchedules = trackingData.numberOfScheduledDeliveries.incrementAndGet();
            if (log.isDebugEnabled()) {
                log.debug("message id= " + messageID + " scheduled. Pending to execute= " + numOfSchedules);
            }
        }
        return numOfSchedules;
    }

    /**
     * Increment number of times this message is scheduled to be delivered
     * to different subscribers. This value will be equal to the number
     * of subscribers expecting the message at that instance.
     *
     * @param messageID identifier of the message
     * @return num of scheduled times after increment
     */
    public int incrementNumberOfScheduledDeliveries(long messageID ,int count) {
        MessageData trackingData = getTrackingData(messageID);
        int numOfSchedules = 0;
        if (trackingData != null) {
            trackingData.addMessageStatus(MessageStatus.SCHEDULED_TO_SEND);
            numOfSchedules = trackingData.numberOfScheduledDeliveries.addAndGet(count);
            if (log.isDebugEnabled()) {
                log.debug("message id= " + messageID + " scheduled. Pending to execute= " + numOfSchedules);
            }
        }
        return numOfSchedules;
    }


    /**
     * Decrement number of times this message is scheduled to be delivered.
     * If message is actually sent to the subscriber this is decreased.
     *
     * @param messageID identifier of the message
     * @return num of scheduled times after decrement
     */
    public int decrementNumberOfScheduledDeliveries(long messageID) {
        MessageData trackingData = getTrackingData(messageID);
        int count = 0;
        if (trackingData != null) {
            count = trackingData.numberOfScheduledDeliveries.decrementAndGet();
            if (count == 0) {
                trackingData.addMessageStatus(MessageStatus.SENT_TO_ALL);
            }
            if (log.isDebugEnabled()) {
                log.debug("message id= " + messageID + " sent. Pending to execute= " + count);
            }
        }
        return count;
    }

    /**
     * Number of times a message is scheduled to deliver.
     * There will be this number of executables ready to
     * send the message.
     *
     * @param messageID identifier of the message
     * @return number of schedules
     */
    public int getNumberOfScheduledDeliveries(long messageID) {
        return getTrackingData(messageID).numberOfScheduledDeliveries.get();
    }

    /**
     * Dump message info to a csv file
     *
     * @param fileToWrite file to dump info
     * @throws AndesException
     */
    public void dumpMessageStatusToFile(File fileToWrite) throws AndesException {

        try {
            FileWriter writer = new FileWriter(fileToWrite);

            writer.append("Message ID");
            writer.append(',');
            writer.append("Message Header");
            writer.append(',');
            writer.append("Destination");
            writer.append(',');
            writer.append("Message status");
            writer.append(',');
            writer.append("Slot Info");
            writer.append(',');
            writer.append("Timestamp");
            writer.append(',');
            writer.append("Expiration time");
            writer.append(',');
            writer.append("NumOfScheduledDeliveries");
            writer.append(',');
            writer.append("Channels sent");
            writer.append('\n');

            for (Long messageID : msgId2MsgData.keySet()) {
                MessageData trackingData = msgId2MsgData.get(messageID);
                writer.append(Long.toString(trackingData.msgID));
                writer.append(',');
                writer.append("null");
                writer.append(',');
                writer.append(trackingData.destination);
                writer.append(',');
                writer.append(trackingData.getStatusHistory());
                writer.append(',');
                writer.append(trackingData.slot.toString());
                writer.append(',');
                writer.append(Long.toString(trackingData.timestamp));
                writer.append(',');
                writer.append(Long.toString(trackingData.expirationTime));
                writer.append(',');
                writer.append(Integer.toString(trackingData.numberOfScheduledDeliveries.get()));
                writer.append(',');
                String deliveries = "";
                for (UUID channelID : trackingData.getAllDeliveredChannels()) {
                    deliveries = deliveries + channelID + " >> " + trackingData
                            .getNumOfDeliveires4Channel(channelID) + " : ";
                }
                writer.append(deliveries);
                writer.append('\n');
            }

            writer.flush();
            writer.close();

        } catch (FileNotFoundException e) {
            log.error("File to write is not found", e);
            throw new AndesException("File to write is not found", e);
        } catch (IOException e) {
            log.error("Error while dumping message status to file", e);
            throw new AndesException("Error while dumping message status to file", e);
        }
    }

}
