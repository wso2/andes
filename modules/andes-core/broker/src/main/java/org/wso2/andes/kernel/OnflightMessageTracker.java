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
import org.wso2.andes.AMQException;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.kernel.slot.SlotDeliveryWorker;
import org.wso2.andes.kernel.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.server.stats.PerformanceCounter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
     * Maximum number of times a message is tried to deliver
     */
    private Integer maximumRedeliveryTimes = 1;

    /**
     * In memory map keeping sent message statistics by message id
     */
    private final ConcurrentHashMap<Long, MsgData> msgId2MsgData;

    /**
     * Map to track messages being buffered to be sent <Id of the slot, messageID, MsgData
     * reference>
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<Long, MsgData>> messageBufferingTracker
            = new ConcurrentHashMap<String, ConcurrentHashMap<Long, MsgData>>();

    /**
     * Map to track messages being sent <channel id, message id, MsgData reference>
     */
    private final ConcurrentHashMap<UUID, ConcurrentHashMap<Long, MsgData>> messageSendingTracker
            = new ConcurrentHashMap<UUID, ConcurrentHashMap<Long, MsgData>>();

    /**
     * Map to keep track of message counts pending to read
     */
    private final ConcurrentHashMap<Slot, AtomicInteger> pendingMessagesBySlot = new
            ConcurrentHashMap<Slot, AtomicInteger>();

    /**
     * Count sent but not acknowledged message count for all the channels
     * key: channelID, value: per channel non acknowledged message count
     */
    private final ConcurrentMap<UUID, AtomicInteger> unAckedMsgCountMap = new ConcurrentHashMap<UUID, AtomicInteger>();

    /**
     * Map to keep track of subscription to slots map.
     * There was no provision to remove messageBufferingTracker when last subscriber close before receive all messages in slot.
     * We use this map to delete remaining tracking when last subscriber close in particular destination.
     */
    private final ConcurrentMap<String, Set<Slot>> subscriptionSlotTracker = new ConcurrentHashMap<String, Set<Slot>>();

    /**
     * Class to keep tracking data of a message
     */
    private class MsgData {

        private final long msgID;
        private final String destination;
        /**
         * timestamp at which the message was taken from store for processing.
         */
        private long timestamp;
        /**
         * Timestamp after which the message should expire.
         */
        private long expirationTime;
        /**
         * timestamp at which the message entered the first gates of the broker.
         */
        private long arrivalTime;
        /**
         * Number of scheduled deliveries. concurrently modified whenever the message is scheduled to be delivered.
         */
        private AtomicInteger numberOfScheduledDeliveries;
        /**
         * Number of deliveries done of this message in each amq channel.
         */
        private Map<UUID, Integer> channelToNumOfDeliveries;
        /**
         * State transition of the message
         */
        private List<MessageStatus> messageStatus;
        /**
         * Parent slot of message.
         */
        private Slot slot;

        private MsgData(long msgID, Slot slot, String destination, long timestamp,
                        long expirationTime, MessageStatus messageStatus,
                        long arrivalTime) {
            this.msgID = msgID;
            this.slot = slot;
            this.destination = destination;
            this.timestamp = timestamp;
            this.expirationTime = expirationTime;
            this.channelToNumOfDeliveries = new ConcurrentHashMap<UUID, Integer>();
            this.messageStatus = new ArrayList<MessageStatus>();
            this.messageStatus.add(messageStatus);
            this.numberOfScheduledDeliveries = new AtomicInteger(0);
            this.arrivalTime = arrivalTime;
        }

        private boolean isExpired() {
            if (expirationTime != 0L) {
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
            for (MessageStatus status : messageStatus) {
                history = history + status + ">>";
            }
            return history;
        }

        private MessageStatus getLatestState() {
            MessageStatus latest = null;
            if (messageStatus.size() > 0) {
                latest = messageStatus.get(messageStatus.size() - 1);
            }
            return latest;
        }

        private boolean isRedelivered(UUID channelID) {
            Integer numOfDeliveries = channelToNumOfDeliveries.get(channelID);
            return numOfDeliveries > 1;
        }

        private int incrementDeliveryCount(UUID channelID) {
            Integer numOfDeliveries = channelToNumOfDeliveries.get(channelID);
            if (numOfDeliveries == null) {
                numOfDeliveries = 0;
            }
            numOfDeliveries++;
            channelToNumOfDeliveries.put(channelID, numOfDeliveries);
            return numOfDeliveries;
        }

        private int decrementDeliveryCount(UUID channelID) {
            Integer numOfDeliveries = channelToNumOfDeliveries.get(channelID);
            numOfDeliveries--;
            if (numOfDeliveries > 0) {
                channelToNumOfDeliveries.put(channelID, numOfDeliveries);
            } else {
                channelToNumOfDeliveries.remove(channelID);
            }
            return numOfDeliveries;
        }

        private int getNumOfDeliveires4Channel(UUID channelID) {
         /* Since sometimes Broker tries to send stored messages when it initialised a subscription
            so then it returns null value for that subscription's channel's amount of deliveries,
            Since we need to the evaluate the rules before we send message, therefore we have to ignore the null value,
            then we have to check the number of deliveries for the particular channel */
            if (null != channelToNumOfDeliveries.get(channelID)) {
                return channelToNumOfDeliveries.get(channelID);
            } else {
                return 0;
            }
        }

        private boolean allAcksReceived() {
            return channelToNumOfDeliveries.isEmpty();
        }
    }


    private OnflightMessageTracker() throws AndesException {

        this.maximumRedeliveryTimes = AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_AMQP_MAXIMUM_REDELIVERY_ATTEMPTS);

        // We don't know the size of the map at startup. hence using an arbitrary value of 16, Need to test
        // Load factor set to default value 0.75
        // Concurrency level set to 6. Currently SlotDeliveryWorker, AckHandler AckSubscription, DeliveryEventHandler,
        // MessageFlusher access this. To be on the safe side set to 6.
        msgId2MsgData = new ConcurrentHashMap<Long, MsgData>(16, 0.75f, 6);

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
     * Register that this message is being delivered to client
     *
     * @return boolean if the message is being redelivered
     */
    public boolean checkAndRegisterSent(long messageId, UUID channelID) throws AMQException {
        return addMessageToSendingTracker(channelID, messageId);
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
            SlotDeliveryWorker slotWorker = SlotDeliveryWorkerManager.getInstance().getSlotWorker(
                    slot
                            .getStorageQueueName());
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
    public void incrementMessageCountInSlot(Slot slot) {
        AtomicInteger pendingMessageCount = pendingMessagesBySlot.get(slot);
        if (null == pendingMessageCount) {
            pendingMessagesBySlot.putIfAbsent(slot, new AtomicInteger());
        }
        pendingMessageCount = pendingMessagesBySlot.get(slot);
        pendingMessageCount.incrementAndGet();
    }

    /**
     * Track acknowledgement for message
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
        releaseMessageDeliveryFromTracking(channel, messageID);
        MsgData trackingData = getTrackingData(messageID);

        //decrement delivery count
        if (trackingData != null) {
            trackingData.decrementDeliveryCount(channel);

            trackingData.addMessageStatus(MessageStatus.ACKED);

            //we consider ack is received if all acks came for channels message was sent
            if (trackingData.allAcksReceived() && getNumberOfScheduledDeliveries(messageID) == 0) {
                trackingData.addMessageStatus(MessageStatus.ACKED_BY_ALL);
                //record how much time took between delivery and ack receive
                long timeTook = (System.currentTimeMillis() - trackingData.timestamp);
                if (log.isDebugEnabled()) {
                    PerformanceCounter.recordAckReceived(trackingData.destination, (int) timeTook);
                }
                decrementMessageCountInSlotAndCheckToResend(trackingData.slot);

                isOKToDeleteMessage = true;
                if (log.isDebugEnabled()) {
                    log.debug("OK to remove message from store as all acks are received id= " + messageID);
                }
            }
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
        MsgData trackingData = getTrackingData(messageID);
        trackingData.timestamp = System.currentTimeMillis();
        trackingData.addMessageStatus(MessageStatus.REJECTED_AND_BUFFERED);
        //release delivery tracing
        releaseMessageDeliveryFromTracking(channel, messageID);
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
        ConcurrentHashMap<Long, MsgData> messagesOfSlot = messageBufferingTracker.get(slotID);
        if (messagesOfSlot == null) {
            messagesOfSlot = new ConcurrentHashMap<Long, MsgData>();
            messageBufferingTracker.put(slotID, messagesOfSlot);
            // track destination to slot
            // use this map to remove messageBufferingTracker when subscriber close before receive all messages in slot
            Set<Slot> subscriptionSlots = subscriptionSlotTracker.get(slot.getDestinationOfMessagesInSlot());
            if(subscriptionSlots == null) {
                Set<Slot> newTrackedSlots = new HashSet<Slot>();
                newTrackedSlots.add(slot);
                subscriptionSlotTracker.put(slot.getDestinationOfMessagesInSlot(), newTrackedSlots);
            } else {
                subscriptionSlots.add(slot);
                subscriptionSlotTracker.put(slot.getDestinationOfMessagesInSlot(), subscriptionSlots);
            }
        }
        MsgData trackingData = messagesOfSlot.get(messageID);
        if (trackingData == null) {
            trackingData = new MsgData(messageID, slot,
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
        MsgData trackingData = messageBufferingTracker.get(slotID).get(messageID);
        if (trackingData != null) {
            isAlreadyBuffered = true;
        }
        return isAlreadyBuffered;
    }

    /**
     * Release tracking of all messages belonging to a slot. i.e called when slot is removed.
     * This will remove all buffering tracking of messages and tracking objects.
     * But tracking objects will remain until delivery cycle completed
     *
     * @param slot slot to release
     */
    public void releaseAllMessagesOfSlotFromTracking(Slot slot) {
        //remove all actual msgData objects
        if (log.isDebugEnabled()) {
            log.debug("Releasing tracking of messages for slot " + slot.toString());
        }
        String slotID = slot.getId();
        ConcurrentHashMap<Long, MsgData> messagesOfSlot = messageBufferingTracker.remove(slotID);
        if (messagesOfSlot != null) {
            for (Long messageId : messagesOfSlot.keySet()) {
                getTrackingData(messageId).addMessageStatus(MessageStatus.SLOT_REMOVED);
                if (checkIfReadyToRemoveFromTracking(messageId)) {
                    if (log.isDebugEnabled()) {
                        log.debug("removing tracking object from memory id " + messageId);
                    }
                    msgId2MsgData.remove(messageId);
                }
            }
        }
    }

    /**
     * Remove tracking object from memory for a message if this returns true
     *
     * @param messageID id of the message to evaluate
     * @return eligibility to delete tracking object
     */
    private boolean checkIfReadyToRemoveFromTracking(long messageID) {
        MsgData messageTrackingData = getTrackingData(messageID);
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
        ConcurrentHashMap<Long, MsgData> messagesOfSlot = messageBufferingTracker.remove(slotID);
        if (messagesOfSlot != null) {
            for (Long messageId : messagesOfSlot.keySet()) {
                msgId2MsgData.remove(messageId);
            }
        }
        subscriptionSlotTracker.remove(slot);
    }

    /**
     * Release tracking of all messages sent from a channel
     *
     * @param channelID id of the channel
     */
    public void releaseAllMessagesOfChannelFromTracking(UUID channelID) {
        if (log.isDebugEnabled()) {
            log.debug("Releasing tracking of messages sent by channel id = " + channelID);
        }
        messageSendingTracker.remove(channelID);
        unAckedMsgCountMap.remove(channelID);
    }

    /**
     * Release tracking that this message is delivered.
     *
     * @param channelID id of the channel
     * @param messageID id of the message to remove
     */
    public void releaseMessageDeliveryFromTracking(UUID channelID, long messageID) {
        if (log.isDebugEnabled()) {
            log.debug("Releasing tracking of message sent id= " + messageID);
        }
        messageSendingTracker.get(channelID).remove(messageID);
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
        MsgData trackingData = getTrackingData(messageID);
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
    private MsgData getTrackingData(long messageID) {
        return msgId2MsgData.get(messageID);
    }

    /**
     * Get destination to slot
     * @return map of destination slot tracker
     */
    public ConcurrentMap<String, Set<Slot>> getSubscriptionSlotTracker() {
        return subscriptionSlotTracker;
    }

    /**
     * Stamp a message as sent. This method also evaluate if the
     * message is being redelivered
     *
     * @param channelID id of the connection message is delivering to subscriber
     * @param messageID id of the message
     * @return if message is redelivered
     */
    public boolean addMessageToSendingTracker(UUID channelID, long messageID) {
        if (log.isDebugEnabled()) {
            log.debug("Adding message to sending tracker channel id = " + channelID + " message id = " +
                    messageID);
        }
        ConcurrentHashMap<Long, MsgData> messagesSentByChannel = messageSendingTracker.get(channelID);

        // NOTE messagesSentByChannel shouldn't be null. At channel creation the map is added.
        // See addNewChannelForTracking(...)
        MsgData trackingData = messagesSentByChannel.get(messageID);
        if (trackingData == null) {
            trackingData = msgId2MsgData.get(messageID);
            if (trackingData != null) {
                messagesSentByChannel.put(messageID, trackingData);
            }
        }
        // increase delivery count
        int numOfCurrentDeliveries = 0;
        if (trackingData != null) {
            numOfCurrentDeliveries = trackingData.incrementDeliveryCount(channelID);
        }

        if (log.isDebugEnabled()) {
            log.debug("Number of current deliveries for message id= " + messageID + " to Channel " + channelID + " is " + numOfCurrentDeliveries);
        }

        //check if this is a redelivered message
        return trackingData != null && trackingData.isRedelivered(channelID);
    }

    /**
     * This initialise internal tracking maps for the given channelID. This needs to be called at channel creation.
     *
     * @param channelID channelID
     */
    public void addNewChannelForTracking(UUID channelID) {
        //We would check if the method returns and object,
        // if it does it means there was a prviouse key linked with the object
        if (null != messageSendingTracker.putIfAbsent(channelID, new ConcurrentHashMap<Long, MsgData>())) {
            log.warn("Trying to initialise tracking for channel " + channelID + " which is already initialised.");
        }
        if (null != unAckedMsgCountMap.putIfAbsent(channelID, new AtomicInteger(0))) {
            log.warn("Trying to initialise tracking for channel " + channelID + " which is already initialised.");
        }
    }

    /**
     * Number of un acknowledged messages for the given channel is returned
     *
     * @param channelID channelID
     * @return number of un acknowledged messages
     */
    public int getNotAckedMessageCount(UUID channelID) {
        if (unAckedMsgCountMap.get(channelID) != null) {
            return unAckedMsgCountMap.get(channelID).get();
        }else{
          return -1;
        }
    }

    /**
     * Decrements non acknowledged message count for a channel
     * <p/>
     * When acknowledgement for a message is received for a given channel by calling this method should be called to
     * decrement the non acknowledged message count
     *
     * @param chanelID channelID
     */
    public void decrementNonAckedMessageCount(UUID chanelID) {
        // NOTE channelID should be in map. ChannelID added to map at channel creation
        int msgCount = unAckedMsgCountMap.get(chanelID).decrementAndGet();
        if (log.isDebugEnabled()) {
            log.debug("Decrement non acked message count. Channel " + chanelID + " pending Count " + msgCount);
        }
    }

    /**
     * Increments the non acknowledged message count for the channel
     * <p/>
     * When a message is sent from Andes non acknowledged message count should be incremented
     *
     * @param channelID channelID
     */
    public void incrementNonAckedMessageCount(UUID channelID) {
        // NOTE channelID should be in map. ChannelID added to map at channel creation
        int intCount = unAckedMsgCountMap.get(channelID).incrementAndGet();
        if (log.isDebugEnabled()) {
            log.debug("Increment non acked message count. Channel " + channelID + " pending Count " + intCount);
        }
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
        MsgData trackingData = msgId2MsgData.remove(messageID);
        Slot slot = trackingData.slot;
        for (UUID channelID : trackingData.channelToNumOfDeliveries.keySet()) {
            releaseMessageDeliveryFromTracking(channelID, messageID);
        }

        releaseMessageBufferingFromTracking(slot, messageID);

        decrementMessageCountInSlotAndCheckToResend(slot);
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
        MsgData trackingData = getTrackingData(messageID);
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
     * Decrement number of times this message is scheduled to be delivered.
     * If message is actually sent to the subscriber this is decreased.
     *
     * @param messageID identifier of the message
     * @return num of scheduled times after decrement
     */
    public int decrementNumberOfScheduledDeliveries(long messageID) {
        MsgData trackingData = getTrackingData(messageID);
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
