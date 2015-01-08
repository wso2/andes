package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.stats.PerformanceCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is the template for metadata object which is delivered
 * to subscribers. A reference is created when a message is read from
 * store and its reference goes on until it is successfully delivered,
 * moved to DLC, expired etc.
 */
public class DeliverableAndesMessageMetadata extends AndesMessageMetadata{

    private static Log log = LogFactory.getLog(DeliverableAndesMessageMetadata.class);
    /**
     * Current status of the message
     */
    private MessageStatus currentMessageStatus;

    /**
     * List of status this message has gone through
     */
    private List<MessageStatus> alreadyPassedStatus;

    /**
     * Delivery information to different channels of this message
     */
    private ConcurrentHashMap<Long, MessageDeliveryInfo> channelDeliveryInfo;

    /**
     * timestamp at which the message was taken from store for processing.
     */
    private long timestamp;

    /**
     * Number of scheduled deliveries. concurrently modified whenever the message is scheduled to be delivered.
     */
    private AtomicInteger numberOfScheduledDeliveries;


    /**
     * Generate DeliverableAndesMessageMetadata instance from a AndesMessageMetadata
     * @param andesMessageMetadata  AndesMessageMetadata object to make deliverable
     */
    public DeliverableAndesMessageMetadata(AndesMessageMetadata andesMessageMetadata) {

        this.alreadyPassedStatus = new ArrayList<MessageStatus>(8);
        this.channelDeliveryInfo = new ConcurrentHashMap<Long, MessageDeliveryInfo>();
        this.numberOfScheduledDeliveries = new AtomicInteger(0);

        this.messageID = andesMessageMetadata.messageID;
        this.metadata = andesMessageMetadata.metadata;
        this.expirationTime = andesMessageMetadata.expirationTime;
        this.isTopic = andesMessageMetadata.isTopic;
        this.arrivalTime = andesMessageMetadata.arrivalTime;
        this.channelId = andesMessageMetadata.channelId;
        this.setDestination(andesMessageMetadata.getDestination());
        this.setStorageQueueName(andesMessageMetadata.getStorageQueueName());
        this.setPersistent(andesMessageMetadata.isPersistent());

        if(andesMessageMetadata.getRedelivered()) {
            this.setRedelivered();
        }

        this.setMessageContentLength(andesMessageMetadata.getMessageContentLength());
        this.setQosLevel(andesMessageMetadata.getQosLevel());
        this.setSlot(andesMessageMetadata.getSlot());
        setMessageStatus(MessageStatus.READ);
    }

    public DeliverableAndesMessageMetadata() {
        super();
        setMessageStatus(MessageStatus.READ);
        this.alreadyPassedStatus = new ArrayList<MessageStatus>(8);
        this.channelDeliveryInfo = new ConcurrentHashMap<Long, MessageDeliveryInfo>();
        this.numberOfScheduledDeliveries = new AtomicInteger(0);
    }

    /**
     * This class is for keeping Channel-wise delivery
     * information of a message
     */
    private class MessageDeliveryInfo {

        /**
         * Number of times message is delivered to channel
         */
        private AtomicInteger numberOfDeliveriesToChannel;

        /**
         * Delivery status for channel
         */
        private ChannelStatus channelStatus;

        MessageDeliveryInfo() {
            this.numberOfDeliveriesToChannel = new AtomicInteger(0);
            this.channelStatus = null;
        }

        /**
         * Increment number of deliveries for channel
         */
        private void incrementNumberOfDeliveries() {
              numberOfDeliveriesToChannel.incrementAndGet();
        }

        /**
         * decrement number of deliveries for channel
         */
        private void decrementNumberOfDeliveries() {
            numberOfDeliveriesToChannel.decrementAndGet();
        }

        /**
         * Get how many times this message is delivered for channel
         * @return  described figure
         */
        private int getNumberOfDeliveries() {
            return numberOfDeliveriesToChannel.get();
        }

        /**
         * Set delivery status for channel
         * @param status status to set
         */
        private void setChannelStatus(ChannelStatus status) {
            this.channelStatus = status;
        }

        /**
         * Get delivery status for channel
         * @return described figure
         */
        private ChannelStatus getChannelStatus() {
            return channelStatus;
        }

    }

    /**
     * Status of this message with respect to a given channel
     */
    private enum ChannelStatus {

        /**
         * Message is sent to channel
         */
        SENT,

        /**
         * Message is acked by channel
         */
        ACKED,

        /**
         * Message is rejected by channel
         */
        REJECTED,

        /**
         * Message is failed to send by channel
         */
        SEND_ERROR,

        /**
         * Channel is closed. Couldn't send
         */
        INACTIVE

    }

    /**
     * Message status to keep track in which state message is
     */
    public enum MessageStatus {

        /**
         * Message has been read from store
         */
        READ,

        /**
         * Message has been buffered for delivery
         */
        BUFFERED,

        /**
         * Message has been sent to its routed consumer
         */
        SENT,

        /**
         * An error has been occurred during send
         */
        SEND_ERROR,

        /**
         * In a topic scenario, message has been sent to all subscribers
         */
        SENT_TO_ALL,

        /**
         * The consumer has acknowledged receipt of the message
         */
        ACKED,

        /**
         * In a topic scenario, all subscribed consumers have acknowledged receipt of message
         */
        ACKED_BY_ALL,


        /**
         * Consumer has rejected the message ad it has been buffered again for delivery (possibly to another waiting
         * consumer)
         */
        REJECTED,


        /**
         * Message has been added to the final async delivery queue (deliverAsynchronously method has been called for
         * the message.)
         */
        SCHEDULED_TO_SEND,

        /**
         * Message has passed all the delivery rules and is eligible to be sent.
         */
        DELIVERY_OK,

        /**
         * Message did not align with one or more delivery rules, and has not been sent.
         */
        DELIVERY_REJECT,

        /**
         * Message has been sent more than once.
         */
        RESENT,

        /**
         * All messages of the slot containing this message have been handled successfully, causing it to be removed
         */
        SLOT_REMOVED,

        /**
         * Message has expired (JMS Expiration duration sent with the message has passed)
         */
        EXPIRED,

        /**
         * Message is moved to the DLC queue
         */
        DLC_MESSAGE,

        /**
         * Message has been cleared from delivery due to a queue purge event.
         */
        PURGED,

        /**
         * Message Metadata removed from Store
         */
        METADATA_REMOVED;


        /**
         * Is OK to remove tracking message
         *
         * @return eligibility to remove
         */
        public static boolean isOKToRemove(List<MessageStatus> messageStatus) {
            return (messageStatus.contains(MessageStatus.ACKED_BY_ALL) || messageStatus.contains(MessageStatus.EXPIRED)
                    || messageStatus.contains(MessageStatus.DLC_MESSAGE));
        }

    }

    /**
     * Set status of the message in its lifecycle
     * @param status status to set
     */
    public void setMessageStatus(MessageStatus status) {
        this.currentMessageStatus = status;
        this.alreadyPassedStatus.add(status);
    }

    /**
     * Get current status of the message
     * @return described figure
     */
    public MessageStatus getCurrentMessageStatus() {
        return currentMessageStatus;
    }

    /**
     * Record that this message is scheduled by Andes to be delivered
     * This figure is useful to decide if message is eligible to delete
     */
    public void recordScheduleToDeliver() {
        numberOfScheduledDeliveries.incrementAndGet();
    }

    /**
     * Record that this message is delivered to given channel
     * @param channelID id of the channel message is delivered
     */
    public void recordDelivery(long channelID) {
        setMessageStatus(MessageStatus.SENT);
        MessageDeliveryInfo messageDeliveryInfo = channelDeliveryInfo.get(channelID);
        if(null == messageDeliveryInfo) {
            messageDeliveryInfo = new MessageDeliveryInfo();
        }
        messageDeliveryInfo.incrementNumberOfDeliveries();
        messageDeliveryInfo.setChannelStatus(ChannelStatus.SENT);
        this.channelDeliveryInfo.put(channelID, messageDeliveryInfo);

        numberOfScheduledDeliveries.decrementAndGet();
    }

    /**
     * Roll-back recording that is message is delivered to the given channel
     * @param channelID  id of the channel delivery failed or rejected
     * @param isChannelInactive is delivery failed due to channel close
     */
    public void rollBackDeliveryRecord(long channelID, boolean isChannelInactive) {
        setMessageStatus(MessageStatus.SEND_ERROR);
        MessageDeliveryInfo messageDeliveryInfo = channelDeliveryInfo.get(channelID);
        messageDeliveryInfo.decrementNumberOfDeliveries();
        if(isChannelInactive) {
            messageDeliveryInfo.setChannelStatus(ChannelStatus.INACTIVE);
        }  else {
            messageDeliveryInfo.setChannelStatus(ChannelStatus.SEND_ERROR);
        }
    }

    /**
     * Record an acknowledgement reached for delivery of this message for given channel
     * @param channelID id of the channel ack reached. Ack comes from delivered
     *                  channel itself
     */
    public void recordAcknowledge(long channelID) {
        setMessageStatus(MessageStatus.ACKED);
        this.channelDeliveryInfo.remove(channelID);
        //we consider ack is received if all acks came for channels message was sent
        //and also all messages scheduled to delivered should have been delivered
        if (isAcknowledgedByAllChannels() && numberOfScheduledDeliveries.get() == 0) {
            setMessageStatus(MessageStatus.ACKED_BY_ALL);
            //record how much time took between delivery and ack receive
            long timeTook = (System.currentTimeMillis() - this.timestamp);
            if (log.isDebugEnabled()) {
                PerformanceCounter.recordAckReceived(super.getDestination(), (int) timeTook);
            }
            if (log.isDebugEnabled()) {
                log.debug(
                        "OK to remove message from store as all acks are received id= " +
                        messageID);
            }
        }
    }

    /**
     * Record this message is rejected by subscriber at given channel
     * @param channelID id of the channel reject is received
     */
    public void recordRejectionByChannel(long channelID) {
        channelDeliveryInfo.get(channelID).setChannelStatus(ChannelStatus.REJECTED);
        setMessageStatus(MessageStatus.REJECTED);
    }

    /**
     * Record a channel close. Record that this message is delivered to that closed
     * channel is removed.
     * @param channelID id of the channel
     */
    public void recordChannelClose(long channelID) {
        channelDeliveryInfo.remove(channelID);
        tryToDeleteMessageFromStore();
    }

    /**
     * Check if this message is acknowledged by all channel it is sent to
     * @return  result of above check
     */
    public boolean isAcknowledgedByAllChannels() {
        boolean isAckedByAllChannels = false;
        if(this.channelDeliveryInfo.isEmpty()) {
            isAckedByAllChannels = true;
        }
        return isAckedByAllChannels;
    }

    /**
     * Update timestamp of the message read from store
     * @param timestamp updated timestamp to set
     */
    public void updateTimeStamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Check if message is expired
     * @return result of above check
     */
    public boolean isExpired() {
        if (expirationTime != 0L) {
            long now = System.currentTimeMillis();
            return (now > expirationTime);
        } else {
            return false;
        }
    }

    /**
     * Get status of the message passed through since it is
     * read from store
     * @return mentioned figure as a string
     */
    public String getStatusHistoryAsString() {
        String history = "";
        for (MessageStatus status : alreadyPassedStatus) {
            history = history + status + ">>";
        }
        return history;
    }

    /**
     * Get latest state of the message
     * @return latest MessageStatus
     */
    public MessageStatus getLatestState() {
        MessageStatus latest = null;
        if (alreadyPassedStatus.size() > 0) {
            latest = alreadyPassedStatus.get(alreadyPassedStatus.size() - 1);
        }
        return latest;
    }

    /**
     * Check if message is already sent to a given channel previously
     * @param channelID id of the channel
     * @return result of above check
     */
    public boolean isRedeliveredToChannel(long channelID) {
        Integer numOfDeliveries = channelDeliveryInfo.get(channelID).getNumberOfDeliveries();
        return numOfDeliveries > 1;
    }

    /**
     * Check if this message is deliverable to the given channel. This is to
     * prevent already delivered messages being sent again due to an sudden
     * inconsistency
     * @param channelID id of the channel
     * @return  result of above check
     */
    public boolean isDeliverableToChannel(long channelID) {
        //Allowed scenarios
        // 1. if message is rejected
        // 2. if a new message
        // 3. if a server side ack timeout happened
        // 4. if send failed
        boolean isDeliverableToChannel = false;
        MessageDeliveryInfo msgDeliveryInfo = channelDeliveryInfo.get(channelID);
        if(null == msgDeliveryInfo || msgDeliveryInfo.getChannelStatus() != ChannelStatus.SENT) {
           isDeliverableToChannel = true;
        }
        return isDeliverableToChannel;
    }

    /**
     * Get how many times message is delivered to the given channel
     * @param channelID id of the channel
     * @return  result of above figure
     */
    public int getNumOfDeliveires4Channel(long channelID) {
        return channelDeliveryInfo.get(channelID).getNumberOfDeliveries();
    }

    /**
     * Check if message is ready to be removed from store
     * @return result of mentioned check
     */
    public boolean isOKToRemoveMessage() {
        return MessageStatus.isOKToRemove(alreadyPassedStatus);
    }

    /**
     * This might called when a topic subscriber suddenly closed. Evaluate and
     * trigger delete from store if possible
     * @return  is OK to delete
     */
    public boolean tryToDeleteMessageFromStore() {
        boolean isOKToRemoveMessage = isOKToRemoveMessage();
        if(isOKToRemoveMessage) {
            //TODO: trigger delete
        }
        return isOKToRemoveMessage;
    }
}
