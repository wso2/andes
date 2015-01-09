/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AndesChannel keep track of the states of the local channels
 */
public class AndesChannel {
    /**
     * Class logger
     */
    private static Log log = LogFactory.getLog(AndesChannel.class);

    /**
     * Used to generate unique IDs for channels
     */
    private static AtomicLong idGenerator = new AtomicLong(0);

    /**
     * Lister used to communicate with the local channels
     */
    private final FlowControlListener listener;

    /**
     * This is the limit used to release flow control on the channel
     */
    private final Integer flowControlLowLimit;

    /**
     * This is the limit used to enforce the flow control on the channel
     */
    private final Integer flowControlHighLimit;

    /**
     * Flow control manager used to handle gobal flow control events
     */
    private final FlowControlManager flowControlManager;

    /**
     * Channel of the current channel
     */
    private final long id;

    /**
     * Scheduled executor service used to create flow control timeout events
     */
    private final ScheduledExecutorService executor;

    /**
     * Flow control timeout task for current channel
     */
    private Runnable flowControlTimeoutTask = new FlowControlTimeoutTask();

    /**
     * Number of messages waiting in the buffer
     */
    private AtomicInteger messagesOnBuffer;

    /**
     * Indicate if the flow control is enabled for this channel
     */
    private boolean flowControlEnabled;

    /**
     * Track global flow control status
     */
    private boolean globalFlowControlEnabled;

    /**
     * Used to close the flow control timeout task if not required
     */
    private ScheduledFuture<?> scheduledFlowControlTimeoutFuture;

    /**
     * Messages delivered through this channel
     */
    private ConcurrentHashMap<Long, DeliverableAndesMessageMetadata> messagesSentToChannel;

    /**
     * Track number of sent but not acknowledged messages
     */
    private AtomicLong numberOfSentButNotAckedMessages;

    /**
     * Is this channel suspended for messages. This is for subscriber
     * side flow control
     */
    private boolean channelSuspended;

    /**
     * Maximum number of delivered but not acked number of messages allowed
     * through this channel before suspending
     */
    private Integer maxNumberOfDeliveredButUnAckedMessages = 5000;

    /**
     * Channel suspension is released when delivered
     * but not acked number of messages dropped down to this limit
     */
    private int channelSubscriptionReleaseThreshold;

    public AndesChannel(FlowControlManager flowControlManager, FlowControlListener listener,
                        boolean globalFlowControlEnabled) {
        this.flowControlManager = flowControlManager;
        this.listener = listener;
        // Used the same executor used by the flow control manager
        this.executor = flowControlManager.getScheduledExecutor();
        this.globalFlowControlEnabled = globalFlowControlEnabled;

        // Read limits
        this.flowControlLowLimit = flowControlManager.getChannelLowLimit();
        this.flowControlHighLimit = flowControlManager.getChannelHighLimit();

        this.id = idGenerator.incrementAndGet();
        this.messagesOnBuffer = new AtomicInteger(0);
        this.flowControlEnabled = false;

        this.messagesSentToChannel = new ConcurrentHashMap<Long, DeliverableAndesMessageMetadata>();
        this.numberOfSentButNotAckedMessages = new AtomicLong(0);
        this.maxNumberOfDeliveredButUnAckedMessages = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_ACK_HANDLING_MAX_UNACKED_MESSAGES);
        this.channelSubscriptionReleaseThreshold = maxNumberOfDeliveredButUnAckedMessages * (60 / 100);
        this.channelSuspended = false;

        log.info("Channel created with ID: " + id);
    }

    /**
     * Get unique id of the channel
     * @return channel identifier
     */
    public long getChannelID() {
        return id;
    }

    /**
     * This method is called by the flow control manager when flow control is enforced globally
     */
    public void notifyGlobalFlowControlActivation() {
        globalFlowControlEnabled = true;
    }

    /**
     * This method is called by the flow control manager when flow control is not enforced globaly
     */
    public void notifyGlobalFlowControlDeactivation() {
        globalFlowControlEnabled = false;
        unblockLocalChannel();
    }

    /**
     * Notify local channel to unblock channel
     */
    private synchronized void unblockLocalChannel() {
        if (flowControlEnabled) {
            scheduledFlowControlTimeoutFuture.cancel(false);
            flowControlEnabled = false;
            listener.unblock();

            log.info("Flow control disabled for channel " + id + ".");
        }
    }

    /**
     * This method should be called when a message is put into the buffer
     *
     * @param size
     *         Number of items added to buffer
     */
    public void recordAdditionToBuffer(int size) {
        flowControlManager.notifyAddition(size);

        int count = messagesOnBuffer.addAndGet(size);

        if (!flowControlEnabled && (globalFlowControlEnabled || count >= flowControlHighLimit)) {
            blockLocalChannel();
        }
    }

    /**
     * Notify local channel to block channel temporary
     */
    private synchronized void blockLocalChannel() {
        if (!flowControlEnabled) {
            flowControlEnabled = true;
            listener.block();
            scheduledFlowControlTimeoutFuture = executor.schedule(flowControlTimeoutTask, 1, TimeUnit.MINUTES);

            log.info("Flow control enabled for channel " + id + ".");
        }
    }

    /**
     * This method should be called after a message is processed and no longer required in the buffer.
     *
     * @param size
     *         Number of items removed from buffer
     */
    public void recordRemovalFromBuffer(int size) {
        flowControlManager.notifyRemoval(size);

        int count = messagesOnBuffer.addAndGet(-size);

        if (flowControlEnabled && !globalFlowControlEnabled && count <= flowControlLowLimit) {
            unblockLocalChannel();
        }
    }

    /**
     * This timeout task avoid flow control being enforced forever. This can happen if the recordAdditionToBuffer get a
     * context switch after evaluating the existing condition and during that time all the messages get processed from
     * the StateEventHandler.
     */
    private class FlowControlTimeoutTask implements Runnable {
        @Override
        public void run() {
            if (flowControlEnabled && !globalFlowControlEnabled && messagesOnBuffer.get() <= flowControlLowLimit) {
                unblockLocalChannel();
            }
        }
    }


    /**
     * This is called when message is tried to be sent through the channel.
     * It will record and keep a reference of the message sent
     * @param message message to be sent
     */
    public void registerMessageDelivery(DeliverableAndesMessageMetadata message) {
        messagesSentToChannel.put(message.getMessageID(), message);
        incrementNotAckedMessageCount();
    }

    /**
     * Remove message reference saved in channel that it is sent through.
     * This is called when a delivery is roll-backed
     * @param message message to be removed
     */
    public void unRegisterMessageDelivery(DeliverableAndesMessageMetadata message) {
        messagesSentToChannel.remove(message.getMessageID());
    }

    /**
     * Acknowledgement received for message from channel. This will
     * remove metadata reference from channel and return it
     * @param messageID id of acknowledged message
     * @return metadata reference of acked message.
     */
    public DeliverableAndesMessageMetadata acknowledgeMessage(long messageID) {
        decrementNotAckedMessageCount();
        return messagesSentToChannel.remove(messageID);
    }


    /**
     * Reject for the message reached via the channel. If client did not ask to requeue
     * metadata reference will be removed
     * @param messageID id of the rejected message
     * @param reQueue if to re-queue message for the client
     * @return metadata reference
     */
    public DeliverableAndesMessageMetadata rejectMessage(long messageID, boolean reQueue) {
        DeliverableAndesMessageMetadata rejectedMessage = messagesSentToChannel.get(messageID);
        rejectedMessage.setChannelId(id);
        if(!reQueue) {
            messagesSentToChannel.remove(messageID);
        }
        decrementNotAckedMessageCount();
        return rejectedMessage;
    }

    /**
     * Increment delivered but not acked message count for this channel
     */
    public void incrementNotAckedMessageCount() {
        numberOfSentButNotAckedMessages.incrementAndGet();
        if (!channelSuspended && numberOfSentButNotAckedMessages.get() > maxNumberOfDeliveredButUnAckedMessages) {
            channelSuspended = true;
            log.info("Subscriber with Channel Id " + id + " is Suspended as It Has Failed To Ack " +
                     maxNumberOfDeliveredButUnAckedMessages + " Messages");
        }
    }

    /**
     * Decrement delivered but not acked message count for this channel
     */
    public void decrementNotAckedMessageCount() {
        numberOfSentButNotAckedMessages.decrementAndGet();
        if(channelSuspended && numberOfSentButNotAckedMessages.get() < channelSubscriptionReleaseThreshold ) {
            channelSuspended = false;
            log.info("Subscriber with Channel Id " + id + " is Responding. Releasing Suspension At" +
                     channelSubscriptionReleaseThreshold + " Messages");
        } else {
            log.info("Subscriber with Channel Id " + id + " is Still Suspended. " +
                     "Pending Ack Count= " + numberOfSentButNotAckedMessages +" Will Release Suspension At" +
                     channelSubscriptionReleaseThreshold);
        }
    }

    /**
     * Channel is suspended with respect to a subscriber. If the number of sent
     * but not acked message count breached a configured limit channel is suspended
     * until some messages are acknowledged
     * @return  is this channel suspended
     */
    public boolean isChannelSuspended() {
        return channelSuspended;
    }

    //should call when send to DLC

    /**
     * Specifically remove reference of message sent by this channel
     * @param messageID id of the message
     */
    public void removeMessageFromChannel(long messageID) {
        messagesSentToChannel.remove(messageID);
    }

    /**
     * Channel close
     */
    public void close() {
        for(Map.Entry<Long, DeliverableAndesMessageMetadata> msgEntry : messagesSentToChannel.entrySet()) {
            msgEntry.getValue().recordChannelClose(id);
        }
    }
}
