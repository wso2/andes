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

package org.wso2.andes.kernel.distrupter;

import com.lmax.disruptor.EventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.AndesSubscriptionManager;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.slot.SlotMessageCounter;
import org.wso2.andes.server.stats.PerformanceCounter;

import java.util.*;

/**
 * State changes related to Andes for inbound events are handled through this handler
 */
public class StateEventHandler implements EventHandler<InboundEvent> {

    private static Log log = LogFactory.getLog(StateEventHandler.class);

    private List<AndesMessage> messageList;
    private int maxBatchSize;

    public StateEventHandler() {
        maxBatchSize = 50;
        messageList = new ArrayList<AndesMessage>(maxBatchSize);
    }

    @Override
    public void onEvent(InboundEvent event, long sequence, boolean endOfBatch) throws Exception {

        if(log.isDebugEnabled()) {
            log.debug("[ sequence " + sequence + " ] Event received from disruptor. Event type: "
                    + event.getEventType() );
        }

        switch (event.getEventType()) {
            case MESSAGE_EVENT:
                // TODO Batch event processing?
                batchAndUpdateOnMetaDataEvent(event, endOfBatch);
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
                openLocalSubscription((LocalSubscription) event.getData());
                break;
            case CLOSE_SUBSCRIPTION_EVENT:
                closeLocalSubscription((LocalSubscription) event.getData());
                break;
        }
    }

    /**
     * Batch Metadata related state change events and update Slots counter
     * @param event InboundEvent
     * @param endOfBatch true if end of batch in disruptor and wise versa
     * @throws AndesException
     */
    private void batchAndUpdateOnMetaDataEvent(InboundEvent event, boolean endOfBatch) throws AndesException {
        messageList.addAll(event.messageList);

        if (endOfBatch || messageList.size() > maxBatchSize) {
            updateSlotsAndQueueCounts(messageList);
            messageList.clear();
        }
    }

    /**
     * Update slot message counters and queue counters
     * @param messageList AndesMessage List
     * @throws AndesException
     */
    public static void updateSlotsAndQueueCounts(List<AndesMessage> messageList)
            throws AndesException {

        // update last message ID in slot message counter. When the slot is filled the last message
        // ID of the slot will be submitted to the slot manager by SlotMessageCounter
        if (AndesContext.getInstance().isClusteringEnabled()) {
            SlotMessageCounter.getInstance().recordMetaDataCountInSlot(messageList);
        }

        Map<String, Integer> destinationSeparatedMetadataCount = new HashMap<String, Integer>();
        for (AndesMessage message : messageList) {
            //separate metadata queue-wise
            Integer msgCount = destinationSeparatedMetadataCount.get(message.getMetadata().getDestination());
            if (msgCount == null) {
                msgCount = 0;
            }
            msgCount = msgCount + 1;
            destinationSeparatedMetadataCount.put(message.getMetadata().getDestination(), msgCount);

            //record the successfully written message count
            PerformanceCounter.recordIncomingMessageWrittenToStore();
        }
        //increment message count for queues
        for (Map.Entry<String, Integer> entry : destinationSeparatedMetadataCount.entrySet()) {
            AndesContext.getInstance().getAndesContextStore().incrementMessageCountForQueue(entry.getKey(),
                    entry.getValue());
        }

    }

    /**
     * Handle client connection open event state change
     * @param channelID channel ID of the opened channel
     */
    public static void clientConnectionOpened(UUID channelID) {
        OnflightMessageTracker.getInstance().addNewChannelForTracking(channelID);
    }

    /**
     * Handle event for closing connection
     * @param channelID channel ID of the closing connection
     */
    public static void clientConnectionClosed(UUID channelID) {
        OnflightMessageTracker.getInstance().releaseAllMessagesOfChannelFromTracking(channelID);
    }

    /**
     * Handle new local subscription creation event. Update the internal state of Andes
     * @param localSubscription LocalSubscription
     */
    public static void openLocalSubscription(LocalSubscription localSubscription) {
        AndesSubscriptionManager subscriptionManager = ClusterResourceHolder.getInstance().getSubscriptionManager();
        try {
            subscriptionManager.addSubscription(localSubscription);
        } catch (AndesException e) {
            log.error("Error occurred while opening local subscription. Subscription id "
                    + localSubscription.getSubscriptionID());
        }
    }

    /**
     * Handle closing of local subscription event. Update the internal state of Andes
     * @param localSubscription LocalSubscription
     */
    public static void closeLocalSubscription(LocalSubscription localSubscription) {
        AndesSubscriptionManager subscriptionManager = ClusterResourceHolder.getInstance().getSubscriptionManager();
        try {
            subscriptionManager.closeLocalSubscription(localSubscription);
        } catch (AndesException e) {
            log.error("Error occurred while closing subscription. Subscription id "
                    + localSubscription.getSubscriptionID());
        }
    }

    /**
     * Start message delivery threads in Andes
     */
    public static void startMessageDelivery() {
        MessagingEngine.getInstance().startMessageDelivery();
    }

    /**
     * Stop message delivery threads in Andes
     */
    public static void stopMessageDelivery() {
        MessagingEngine.getInstance().stopMessageDelivery();
    }

    /**
     * Handle event of start message expiration worker
     */
    public static void startMessageExpirationWorker() {
        try {
            MessagingEngine.getInstance().startMessageExpirationWorker();
        } catch (AndesException e) {
            // TODO: throw a fatal error and stop the disruptor. Don't Ignore
            log.error("Error occurred while initialising message expiration worker", e);
        }
    }

    /**
     * Handle stopping message expiration worker
     */
    public static void stopMessageExpirationWorker() {
        MessagingEngine.getInstance().stopMessageExpirationWorker();
    }

    /**
     * Handle event of shutting down MessagingEngine
     */
    public static void shutdownMessagingEngine() {
        MessagingEngine.getInstance().close();
    }

 }
