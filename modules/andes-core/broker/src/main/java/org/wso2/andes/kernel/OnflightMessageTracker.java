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
     * Map to keep track of message counts pending to read
     */
    private final ConcurrentHashMap<Slot, AtomicInteger> pendingMessagesBySlot = new
            ConcurrentHashMap<>();

    /**
     * Class to keep tracking data of a message
     */

    private OnflightMessageTracker() throws AndesException {

        // We don't know the size of the map at startup. hence using an arbitrary value of 16, Need to test
        // Load factor set to default value 0.75
        // Concurrency level set to 6. Currently SlotDeliveryWorker, AckHandler AckSubscription, DeliveryEventHandler,
        // MessageFlusher access this. To be on the safe side set to 6.
        msgId2MsgData = new ConcurrentHashMap<>(16, 0.75f, 6);

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
        stampMessageAsRejected(channelId, metadata);
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
            SlotDeliveryWorker slotWorker = SlotDeliveryWorkerManager
                    .getInstance().getSlotWorker(slot.getStorageQueueName());

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
            if (trackingData.allAcksReceived()) {
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
    
    public void decrementMessageCountInSlot(long messageID) throws AndesException {
        //release delivery tracing
        MessageData trackingData = getTrackingData(messageID);

        decrementMessageCountInSlot(trackingData.slot);
    }

    /**
     * Track reject of the message
     *
     * @param channel   channel of the message reject
     * @param message metadata of the message reject represent
     */
    public void stampMessageAsRejected(UUID channel, AndesMessageMetadata message) {
        if (log.isDebugEnabled()) {
            log.debug("stamping message as rejected id = " + message.getMessageID());
        }
        MessageData trackingData = message.getTrackingData();
        if (trackingData != null) {
            trackingData.timestamp = System.currentTimeMillis();
            trackingData.addMessageStatus(MessageStatus.REJECTED_AND_BUFFERED);
            //we decrement the pending ack count since we know that the message is rejected
            trackingData.decrementPendingAckCount();
        }
    }

    /**
     * Track that this message is buffered. Return true if eligible to buffer
     *
     * @param slot                 slot message being read in
     * @param andesMessageMetadata metadata to buffer
     * @return eligibility to buffer
     */
    public void addMessageToTracker(Slot slot, AndesMessageMetadata andesMessageMetadata) {
        long messageID = andesMessageMetadata.getMessageID();
        if (log.isDebugEnabled()) {
            log.debug("Buffering message id = " + messageID + " slot = " + slot.toString());
        }
        MessageData trackingData = new MessageData(messageID, slot, slot.getDestinationOfMessagesInSlot(),
                                                   System.currentTimeMillis(), andesMessageMetadata.getExpirationTime(),
                                                   MessageStatus.BUFFERED, andesMessageMetadata.getArrivalTime());
        andesMessageMetadata.setTrackingData(trackingData);
        msgId2MsgData.put(messageID, trackingData);
    }

    public void removeMessageFromTracker(Long messageID) {
        MessageData deletedMessage = msgId2MsgData.remove(messageID);
        if (null != deletedMessage) {
            deletedMessage.markAsStale();
        }
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

        decrementMessageCountInSlot(slot);
    }

    /**
     * Increment number of times this message is scheduled to be delivered
     * to different subscribers. This value will be equal to the number
     * of subscribers expecting the message at that instance.
     *
     * @param message metadata of the message
     * @return num of scheduled times after increment
     */
    public int incrementNumberOfScheduledDeliveries(AndesMessageMetadata message) {
        MessageData trackingData = message.getTrackingData();
        int numOfSchedules = 0;
        if (trackingData != null) {
            trackingData.addMessageStatus(MessageStatus.SCHEDULED_TO_SEND);
            trackingData.incrementPendingAckCount(1);
            numOfSchedules = trackingData.numberOfScheduledDeliveries.incrementAndGet();
            if (log.isDebugEnabled()) {
                log.debug("message id= " + message.getMessageID() + " scheduled. Pending to execute= " + numOfSchedules);
            }
        }
        return numOfSchedules;
    }

    /**
     * Increment number of times this message is scheduled to be delivered
     * to different subscribers. This value will be equal to the number
     * of subscribers expecting the message at that instance.
     *
     * @param message metadata of the message
     * @return num of scheduled times after increment
     */
    public int incrementNumberOfScheduledDeliveries(AndesMessageMetadata message ,int count) {
        MessageData trackingData = message.getTrackingData();
        int numOfSchedules = 0;
        if (trackingData != null) {
            trackingData.addMessageStatus(MessageStatus.SCHEDULED_TO_SEND);
            trackingData.incrementPendingAckCount(count);
            numOfSchedules = trackingData.numberOfScheduledDeliveries.addAndGet(count);
            if (log.isDebugEnabled()) {
                log.debug("message id= " + message.getMessageID() + " scheduled. Pending to execute= " + numOfSchedules);
            }
        }
        return numOfSchedules;
    }


    /**
     * Decrement number of times this message is scheduled to be delivered.
     * If message is actually sent to the subscriber this is decreased.
     *
     * @param message metadata of the message involved
     * @return num of scheduled times after decrement
     */
    public int decrementNumberOfScheduledDeliveries(AndesMessageMetadata message) {
        long messageID = message.getMessageID();
        MessageData trackingData = message.getTrackingData();
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
     * @param message metadata of the message
     * @return number of schedules
     */
    public int getNumberOfScheduledDeliveries(AndesMessageMetadata message) {
        return message.getTrackingData().numberOfScheduledDeliveries.get();
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
