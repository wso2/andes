package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.stats.PerformanceCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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



    public DeliverableAndesMessageMetadata(AndesMessageMetadata andesMessageMetadata) {

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
        this.alreadyPassedStatus = new ArrayList<MessageStatus>(8);
        this.channelDeliveryInfo = new ConcurrentHashMap<Long, MessageDeliveryInfo>();
        this.numberOfScheduledDeliveries = new AtomicInteger(0);

    }

    public DeliverableAndesMessageMetadata() {
        super();
        setMessageStatus(MessageStatus.READ);
        this.alreadyPassedStatus = new ArrayList<MessageStatus>(8);
        this.channelDeliveryInfo = new ConcurrentHashMap<Long, MessageDeliveryInfo>();
        this.numberOfScheduledDeliveries = new AtomicInteger(0);
    }

    private class MessageDeliveryInfo {

        private AtomicInteger numberOfDeliveriesToChannel;
        private ChannelStatus channelStatus;

        MessageDeliveryInfo() {
            this.numberOfDeliveriesToChannel = new AtomicInteger(0);
            this.channelStatus = null;
        }

        private void incrementNumberOfDeliveries() {
              numberOfDeliveriesToChannel.incrementAndGet();
        }

        private void decrementNumberOfDeliveries() {
            numberOfDeliveriesToChannel.decrementAndGet();
        }

        private int getNumberOfDeliveries() {
            return numberOfDeliveriesToChannel.get();
        }

        private void setChannelStatus(ChannelStatus status) {
            this.channelStatus = status;
        }

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

    public void setMessageStatus(MessageStatus status) {
        this.currentMessageStatus = status;
        this.alreadyPassedStatus.add(status);
    }

    public MessageStatus getCurrentMessageStatus() {
        return currentMessageStatus;
    }

    public void recordScheduleToDeliver() {
        numberOfScheduledDeliveries.incrementAndGet();
    }

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

    public void recordRejectionByChannel(long channelID) {
        channelDeliveryInfo.get(channelID).setChannelStatus(ChannelStatus.REJECTED);
        setMessageStatus(MessageStatus.REJECTED);
    }

    public void recordChannelClose(long channelID) {
        channelDeliveryInfo.remove(channelID);
        //TODO : try to delete message
    }


    public boolean isAcknowledgedByAllChannels() {
        boolean isAckedByAllChannels = false;
        if(this.channelDeliveryInfo.isEmpty()) {
            isAckedByAllChannels = true;
        }
        return isAckedByAllChannels;
    }

    public void updateTimeStamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isExpired() {
        if (expirationTime != 0L) {
            long now = System.currentTimeMillis();
            return (now > expirationTime);
        } else {
            return false;
        }
    }

    public String getStatusHistoryAsString() {
        String history = "";
        for (MessageStatus status : alreadyPassedStatus) {
            history = history + status + ">>";
        }
        return history;
    }

    public MessageStatus getLatestState() {
        MessageStatus latest = null;
        if (alreadyPassedStatus.size() > 0) {
            latest = alreadyPassedStatus.get(alreadyPassedStatus.size() - 1);
        }
        return latest;
    }

    public boolean isRedeliveredToChannel(long channelID) {
        Integer numOfDeliveries = channelDeliveryInfo.get(channelID).getNumberOfDeliveries();
        return numOfDeliveries > 1;
    }

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

    public int getNumOfDeliveires4Channel(long channelID) {
        return channelDeliveryInfo.get(channelID).getNumberOfDeliveries();
    }

    public boolean isOKToRemoveMessage() {
        return MessageStatus.isOKToRemove(alreadyPassedStatus);
    }

    public boolean tryToDeleteMessageFromStore() {
        boolean isOKToRemoveMessage = isOKToRemoveMessage();
        if(isOKToRemoveMessage) {
            //TODO: trigger delete
        }
        return isOKToRemoveMessage;
    }
}
