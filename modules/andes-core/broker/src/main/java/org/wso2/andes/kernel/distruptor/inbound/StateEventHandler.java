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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.distruptor.inbound;

import com.lmax.disruptor.EventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.kernel.AndesSubscriptionManager;
import org.wso2.andes.kernel.OnflightMessageTracker;
import org.wso2.andes.kernel.slot.SlotMessageCounter;
import org.wso2.andes.server.stats.PerformanceCounter;

import java.util.List;
import java.util.UUID;

/**
 * State changes related to Andes for inbound events are handled through this handler
 */
public class StateEventHandler implements EventHandler<InboundEvent> {

    private static Log log = LogFactory.getLog(StateEventHandler.class);

    /**
     * reference to MessagingEngine
     */
    private final MessagingEngine messagingEngine;

    StateEventHandler(MessagingEngine messagingEngine) {
        this.messagingEngine = messagingEngine;
    }

    @Override
    public void onEvent(InboundEvent event, long sequence, boolean endOfBatch) throws Exception {


        if (log.isDebugEnabled()) {
            log.debug("[ sequence " + sequence + " ] Event received from disruptor. Event type: "
                    + event.getEventType() + " " + this);
        }
        try {
            switch (event.getEventType()) {
                case MESSAGE_EVENT:
                    updateSlotsAndQueueCounts(event.messageList);
                    event.getChannel().recordRemovalFromBuffer(getProcessedAmount(event.messageList));
                    break;
                case CHANNEL_CLOSE_EVENT:
                    clientConnectionClosed((UUID) event.getData());
                    break;
                case CHANNEL_OPEN_EVENT:
                    clientConnectionOpened((UUID) event.getData());
                    break;
                case STOP_MESSAGE_DELIVERY_EVENT:
                    stopMessageDelivery();
                    break;
                case START_MESSAGE_DELIVERY_EVENT:
                    startMessageDelivery();
                    break;
                case START_EXPIRATION_WORKER_EVENT:
                    startMessageExpirationWorker();
                    break;
                case STOP_EXPIRATION_WORKER_EVENT:
                    stopMessageExpirationWorker();
                    break;
                case SHUTDOWN_MESSAGING_ENGINE_EVENT:
                    shutdownMessagingEngine();
                    break;
                case OPEN_SUBSCRIPTION_EVENT:
                    openLocalSubscription((LocalSubscription) event.getData(), event);
                    break;
                case CLOSE_SUBSCRIPTION_EVENT:
                    closeLocalSubscription((LocalSubscription) event.getData());
                    break;
            }

        } finally {
            // This is the final handler that visits the slot in ring buffer. Hence after processing is done clear the
            // slot so that in next iteration of the first event handler over the same slot won't find garbage from
            // previous iterations.

            event.clear();
        }
    }

    private int getProcessedAmount(List<AndesMessage> messages) {
        int count = 0;
        for (AndesMessage message : messages) {
            count = count + message.getContentChunkList().size();
        }
        return count;
    }

    /**
     * Update slot message counters and queue counters
     *
     * @param messageList AndesMessage List
     */
    public void updateSlotsAndQueueCounts(List<AndesMessage> messageList) {

        // update last message ID in slot message counter. When the slot is filled the last message
        // ID of the slot will be submitted to the slot manager by SlotMessageCounter
        if (AndesContext.getInstance().isClusteringEnabled()) {
            SlotMessageCounter.getInstance().recordMetaDataCountInSlot(messageList);
        }

        for (AndesMessage message : messageList) {
            // For each message increment by 1. Underlying messaging engine will handle the increment destination
            // wise.
            messagingEngine.incrementQueueCount(message.getMetadata().getDestination(), 1);
        }

        //record the successfully written message count
        PerformanceCounter.recordIncomingMessageWrittenToStore();

        if (log.isTraceEnabled()) {
            StringBuilder messageIds = new StringBuilder();
            for (AndesMessage message : messageList) {
                messageIds.append(message.getMetadata().getMessageID()).append(" , ");
            }
            log.debug("Messages STATE UPDATED: " + messageIds);
        }
    }

    /**
     * Handle client connection open event state change
     *
     * @param channelID channel ID of the opened channel
     */
    public void clientConnectionOpened(UUID channelID) {
        OnflightMessageTracker.getInstance().addNewChannelForTracking(channelID);
    }

    /**
     * Handle event for closing connection
     *
     * @param channelID channel ID of the closing connection
     */
    public void clientConnectionClosed(UUID channelID) {
        OnflightMessageTracker.getInstance().releaseAllMessagesOfChannelFromTracking(channelID);
    }

    /**
     * Handle new local subscription creation event. Update the internal state of Andes
     *
     * @param localSubscription LocalSubscription
     */
    public void openLocalSubscription(LocalSubscription localSubscription, InboundEvent event) {
        AndesSubscriptionManager subscriptionManager = ClusterResourceHolder.getInstance().getSubscriptionManager();
        try {
            subscriptionManager.addSubscription(localSubscription);
            event.getFuture().set("success");
        } catch (AndesException e) {
            event.getFuture().setException(e);
            log.error("Error occurred while opening local subscription. Subscription id "
                    + localSubscription.getSubscriptionID(), e);
        }
    }

    /**
     * Handle closing of local subscription event. Update the internal state of Andes
     *
     * @param localSubscription LocalSubscription
     */
    public void closeLocalSubscription(LocalSubscription localSubscription) {
        AndesSubscriptionManager subscriptionManager = ClusterResourceHolder.getInstance().getSubscriptionManager();
        try {
            subscriptionManager.closeLocalSubscription(localSubscription);
        } catch (AndesException e) {
            log.error("Error occurred while closing subscription. Subscription id "
                    + localSubscription.getSubscriptionID(), e);
        }
    }

    /**
     * Start message delivery threads in Andes
     */
    public void startMessageDelivery() {
        messagingEngine.startMessageDelivery();
    }

    /**
     * Stop message delivery threads in Andes
     */
    public void stopMessageDelivery() {
        messagingEngine.stopMessageDelivery();
    }

    /**
     * Handle event of start message expiration worker
     */
    public void startMessageExpirationWorker() {
        MessagingEngine.getInstance().startMessageExpirationWorker();
    }

    /**
     * Handle stopping message expiration worker
     */
    public void stopMessageExpirationWorker() {
        messagingEngine.stopMessageExpirationWorker();
    }

    /**
     * Handle event of shutting down MessagingEngine
     */
    public void shutdownMessagingEngine() {
        try {
            messagingEngine.close();
        } catch (InterruptedException e) {
            log.error("Interrupted while closing messaging engine. ", e);
        }
    }
}
