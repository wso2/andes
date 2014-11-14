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
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.wso2.andes.AMQException;
import org.wso2.andes.api.Message;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.slot.SlotDeliveryWorker;
import org.wso2.andes.server.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.server.stats.PerformanceCounter;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.poi.hssf.usermodel.HSSFRow;

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
        SentToAll,
        Acked,
        AckedByAll,
        RejectedAndBuffered,
        ScheduledToSend,
        DeliveryOK,
        DeliveryReject,
        Resent,
        SlotRemoved,
        Expired,
        DLCMessage;


        /**
         * Is OK to remove tracking message
         * @return eligibility to remove
         */
        public static boolean isOKToRemove(List<MessageStatus> messageStatus){
            return ( messageStatus.contains(MessageStatus.AckedByAll) || messageStatus.contains(MessageStatus.Expired)
                    || messageStatus.contains(MessageStatus.DLCMessage));
        }

    }

    /**
     * Class to keep tracking data of a message
     */
    private class MsgData {

        final long msgID;
        boolean ackreceived = false;
        final String destination;
        long timestamp; // timestamp at which the message was taken from store for processing.
        long expirationTime;
        long arrivalTime; // timestamp at which the message entered the first gates of the broker.
        long deliveryID;
        AtomicInteger numberOfScheduledDeliveries;
        Map<UUID, Integer> channelToNumOfDeliveries;
        List<MessageStatus> messageStatus;
        Slot slot;

        private MsgData(long msgID, Slot slot, boolean ackReceived, String destination, long timestamp,
                        long expirationTime, long deliveryID,MessageStatus messageStatus,
                        long arrivalTime) {
            this.msgID = msgID;
            this.slot = slot;
            this.ackreceived = ackReceived;
            this.destination = destination;
            this.timestamp = timestamp;
            this.expirationTime = expirationTime;
            this.deliveryID = deliveryID;
            this.channelToNumOfDeliveries = new HashMap<UUID, Integer>();
            this.messageStatus = new ArrayList<MessageStatus>();
            this.messageStatus.add(messageStatus);
            this.numberOfScheduledDeliveries = new AtomicInteger(0);
            this.arrivalTime = arrivalTime;
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

        private void addMessageStatus(MessageStatus status) {
            messageStatus.add(status);
        }

        private String getStatusHistory() {
            String history = "";
            for(MessageStatus status : messageStatus) {
                history = history + status + ">>";
            }
            return history;
        }

        private MessageStatus getLatestState() {
            MessageStatus latest = null;
            if(messageStatus.size() > 0) {
                latest = messageStatus.get(messageStatus.size() -1);
            }
            return latest;
        }

        private boolean isRedelivered(UUID channelID) {
            Integer numOfDeliveries = channelToNumOfDeliveries.get(channelID);
            return numOfDeliveries > 1;
        }

        private int incrementDeliveryCount(UUID channelID) {
            Integer numOfDeliveries = channelToNumOfDeliveries.get(channelID);
            if(numOfDeliveries == null) {
                numOfDeliveries = 0;
            }
            numOfDeliveries += 1;
            channelToNumOfDeliveries.put(channelID, numOfDeliveries);
            return numOfDeliveries;
        }

        private int decrementDeliveryCount(UUID channelID) {
            Integer numOfDeliveries = channelToNumOfDeliveries.get(channelID);
            numOfDeliveries -= 1;
            if(numOfDeliveries > 0) {
                channelToNumOfDeliveries.put(channelID, numOfDeliveries);
            }  else {
                channelToNumOfDeliveries.remove(channelID);
            }
            return numOfDeliveries;
        }

        private int getNumOfDeliveires4Channel(UUID channelID) {
            return channelToNumOfDeliveries.get(channelID);
        }

        private boolean allAcksReceived() {
            Set<UUID> channelsDelivered = channelToNumOfDeliveries.keySet();
            boolean allAcksReceived = false;
            if(channelsDelivered.isEmpty()) {
                allAcksReceived = true;
            }
            return allAcksReceived;
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
    }

    /**
     * Register that this message is being delivered to client
     *
     * @return boolean if the message is being redelivered
     */
    public boolean checkAndRegisterSent(long messageId, UUID channelID, long deliverTag)
            throws AMQException {

        MsgData trackingData = msgId2MsgData.get(messageId);
        trackingData.deliveryID = deliverTag;

        return addMessageToSendingTracker(channelID, messageId);

    }

    /**
     * Any custom checks or procedures that should be executed before message delivery should
     * happen here. Any message rejected at this stage will be sent to DLC
     * @param messageId id of message metadata entry to evaluate for delivery
     * @return eligibility deliver
     */
    public boolean evaluateDeliveryRules(long messageId, UUID channelID) throws AndesException {
        boolean isOKToDeliver = true;
        MsgData trackingData = getTrackingData(messageId);

        //check if number of redelivery tries has breached.
        int numOfDeliveriesOfCurrentMsg = trackingData.getNumOfDeliveires4Channel(channelID);

        // Get last purged timestamp of the destination queue.
        long lastPurgedTimestampOfQueue = QueueDeliveryWorker.getInstance()
                .getQueueDeliveryInfo(trackingData.destination).getLastPurgedTimestamp();

        if (numOfDeliveriesOfCurrentMsg > maximumRedeliveryTimes) {
            log.warn("Number of Maximum Redelivery Tries Has Breached. Routing Message to DLC : id= " +
                    messageId);
            isOKToDeliver =  false;
            //check if destination entry has expired. Any expired message will not be delivered
        } else if (trackingData.isExpired()) {
            stampMessageAsExpired(messageId);
            log.warn("Message is expired. Routing Message to DLC : id= " + messageId);
            isOKToDeliver =  false;
        } else if (trackingData.arrivalTime <= lastPurgedTimestampOfQueue) {
            log.warn("Message was sent at "+trackingData.arrivalTime+" before last purge event at " +
                    ""+lastPurgedTimestampOfQueue+". Will be skipped. id= " +
                    messageId);
            isOKToDeliver = false;
        }
        if(isOKToDeliver) {
            trackingData.addMessageStatus(MessageStatus.DeliveryOK);
            if(numOfDeliveriesOfCurrentMsg == 1) {
                trackingData.addMessageStatus(MessageStatus.Sent);
            } else if(numOfDeliveriesOfCurrentMsg > 1) {
                trackingData.addMessageStatus(MessageStatus.Resent);
            }

        } else {
            trackingData.addMessageStatus(MessageStatus.DeliveryReject);
        }
        return isOKToDeliver;
    }

    /**
     * Decrement message count in slot and if it is zero check the slot again to resend
     *
     * @param slot Slot whose message count is decremented
     * @throws AndesException
     */
    public void decrementMessageCountInSlotAndCheckToResend(Slot slot)
            throws AndesException {
        AtomicInteger pendingMessageCount = pendingMessagesBySlot.get(slot);
        int messageCount = pendingMessageCount.decrementAndGet();
        if (messageCount == 0) {
            /*
            All the Acks for the slot has bee received. Check the slot again for unsend
            messages and if there are any send them and delete the slot.
             */
            SlotDeliveryWorker slotWorker = SlotDeliveryWorkerManager.getInstance().getSlotWorker(slot
                    .getStorageQueueName());
            if(log.isDebugEnabled()) {
                log.debug("Slot has no pending messages. Now re-checking slot for messages");
            }
            slotWorker.checkForSlotCompletionAndResend(slot);
        }

    }

    /**
     * Increment the message count in a slot
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
     * @param channel  channel of the ack
     * @param messageID id of the message ack is for
     * @return if message is OK to delete (all acks received)
     * @throws AndesException
     */
    public boolean handleAckReceived(UUID channel, long messageID) throws AndesException {
        if(log.isDebugEnabled()) {
            log.debug("Ack Received message id= " + messageID + " channel id= " + channel);
        }

        boolean isOKToDeleteMessage = false;

        //release delivery tracing
        releaseMessageDeliveryFromTracking(channel, messageID);
        MsgData trackingData = getTrackingData(messageID);

        //decrement delivery count
        int numOfDeliveries = trackingData.decrementDeliveryCount(channel);

        setMessageStatus(MessageStatus.Acked, trackingData);

        //we consider ack is received if all acks came for channels message was sent
        if(trackingData.allAcksReceived() && getNumberOfScheduledDeliveries(messageID)== 0) {
            trackingData.ackreceived = true;
            setMessageStatus(MessageStatus.AckedByAll, trackingData);
            sendButNotAckedMessageCount.decrementAndGet();
            //record how much time took between delivery and ack receive
            long timeTook = (System.currentTimeMillis() - trackingData.timestamp);
            PerformanceCounter.recordAckReceived(trackingData.destination, (int) timeTook);
            decrementMessageCountInSlotAndCheckToResend(trackingData.slot);

            isOKToDeleteMessage = true;
            if(log.isDebugEnabled()) {
                log.debug("OK to remove message from store as all acks are received id= " + messageID);
            }
        }

        return isOKToDeleteMessage;
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
        trackingData.addMessageStatus(MessageStatus.RejectedAndBuffered);
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
                                       slot.getDestinationOfMessagesInSlot(),
                                       System.currentTimeMillis(),
                                       andesMessageMetadata.getExpirationTime(), 0,
                    MessageStatus.Buffered,andesMessageMetadata.getArrivalTime());
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
        if (messagesOfSlot != null) {
            for (Long messageIdOfSlot : messagesOfSlot.keySet()) {
                getTrackingData(messageIdOfSlot).addMessageStatus(MessageStatus.SlotRemoved);
                if(checkIfReadyToRemoveFromTracking(messageIdOfSlot)) {
                    if(log.isDebugEnabled()) {
                        log.debug("removing tracking object from memory id= " + messageIdOfSlot);
                    }
                }
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
     * Set message status for a message.
     * This can be buffered, sent, rejected etc
     * @param messageStatus status of the message
     * @param msgData  message tracking object
     */
    public void setMessageStatus(MessageStatus messageStatus, MsgData msgData) {
        msgData.addMessageStatus(messageStatus);
    }

    /**
     * Set message status as expired
     * @param messageID id of the message to set expired
     */
    public void stampMessageAsExpired(long messageID) {
        getTrackingData(messageID).addMessageStatus(MessageStatus.Expired);
    }

    /**
     * Get the current status of the message in delivery pipeline
     * @param messageID id of the message to get status
     * @return status of the message
     */
    public MessageStatus getMessageStatus(long messageID) {
        return getTrackingData(messageID).getLatestState();
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


    /**
     * Stamp a message as sent. This method also evaluate if the
     * message is being redelivered
     * @param channelID id of the connection message is delivering to subscriber
     * @param messageID id of the message
     * @return  if message is redelivered
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
        int numOfCurrentDeliveries = trackingData.incrementDeliveryCount(channelID);

        if(log.isDebugEnabled()) {
            log.debug("Number of current deliveries for message id= " + messageID + " to Channel " + channelID + " is " + numOfCurrentDeliveries);
        }

        sendButNotAckedMessageCount.incrementAndGet();

        //check if this is a redelivered message
        return trackingData.isRedelivered(channelID);
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
        Slot slot = trackingData.slot;
        for(UUID channelID : trackingData.channelToNumOfDeliveries.keySet()) {
            releaseMessageDeliveryFromTracking(channelID, messageID);
        }

        releaseMessageBufferingFromTracking(slot, messageID);

        decrementMessageCountInSlotAndCheckToResend(slot);
    }

    /**
     * Increment number of times this message is scheduled to be delivered
     * to different subscribers. This value will be equal to the number
     * of subscribers expecting the message at that instance.
     * @param messageID identifier of the message
     * @return  num of scheduled times after increment
     */
    public int incrementNumberOfScheduledDeliveries(long messageID) {
        MsgData trackingData =  getTrackingData(messageID);
        trackingData.addMessageStatus(MessageStatus.ScheduledToSend);
        int numOfSchedules =  trackingData.numberOfScheduledDeliveries.incrementAndGet();
        if(log.isDebugEnabled()) {
            log.debug("message id= " + messageID + " scheduled. Pending to execute= " + numOfSchedules);
        }
        return numOfSchedules;
    }

    /**
     *  Decrement number of times this message is scheduled to be delivered.
     *  If message is actually sent to the subscriber this is decreased.
     * @param messageID identifier of the message
     * @return num of scheduled times after decrement
     */
    public int decrementNumberOfScheduledDeliveries(long messageID) {
        MsgData trackingData =  getTrackingData(messageID);
        int count =  trackingData.numberOfScheduledDeliveries.decrementAndGet();
        if(count == 0) {
            trackingData.addMessageStatus(MessageStatus.SentToAll);
        }
        if(log.isDebugEnabled()) {
            log.debug("message id= " + messageID + " sent. Pending to execute= " + count);
        }
        return count;
    }

    /**
     * Number of times a message is scheduled to deliver.
     * There will be this number of executables ready to
     * send the message.
     * @param messageID identifier of the message
     * @return number of schedules
     */
    public int getNumberOfScheduledDeliveries(long messageID) {
        return getTrackingData(messageID).numberOfScheduledDeliveries.get();
    }

    /**
     * Dump message info to a excel sheet
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
                MsgData trackingData = msgId2MsgData.get(messageID);
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
                for (UUID channelID : trackingData.channelToNumOfDeliveries.keySet()) {
                    deliveries = deliveries + channelID + " >> " + trackingData
                            .channelToNumOfDeliveries
                            .get(channelID) + " : ";
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
