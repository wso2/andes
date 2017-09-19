/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.kernel.disruptor.inbound;


import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContextInformationManager;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.subscription.StorageQueue;

/**
 * Inbound event for disruptor representing a queue change notification
 */
public class InboundQueueSyncEvent implements AndesInboundStateEvent {


    private static Log log = LogFactory.getLog(InboundQueueEvent.class);

    /**
     * Supported state events
     */
    private enum EventType {

        /**
         * Queue purging event related event type
         */
        SYNC_QUEUE_PURGE_EVENT,

        /**
         * Is queue deletable check related event type
         */
        IS_QUEUE_DELETABLE_EVENT,

        /**
         * Delete the queue from DB related event type
         */
        SYNC_QUEUE_DELETE_EVENT,

        /**
         * Create a queue in Andes related event type
         */
        SYNC_QUEUE_CREATE_EVENT,
    }

    /**
     * Queue information encoded as a string
     */
    private String encodedQueueInfo;

    /**
     * Event type this event
     */
    private EventType eventType;

    /**
     * Reference to AndesContextInformationManager to update create/ remove queue state
     */
    private AndesContextInformationManager contextInformationManager;


    /**
     * Purged message count as a future. When a purging is done purged message count is set. Interested user can use
     * InboundQueueEvent#getPurgedCount method to get the purged count from this async event.
     * InboundQueueEvent#getPurgedCount method is a blocking call.
     */
    private SettableFuture<Integer> purgedCount;

    /**
     * Each event is associated with a particular task. An InboundQueueEvent object could be of any of the types: Queue
     * addition, queue deletion, purge, etc. Even though these operations are processed asynchronously through the
     * disruptor, there are situations where it is needed to wait until the event is processed.
     * <p/>
     * A settable future object is to block the thread until the process is complete. By calling the method get on the
     * variable, the caller will have to wait until the operation is complete
     */
    private SettableFuture<Boolean> isEventComplete;


    /**
     * Create a event representing a queue change
     *
     * @param encodedQueueInfo encoded information of a
     *                         {@link org.wso2.andes.kernel.subscription.StorageQueue}
     */
    public InboundQueueSyncEvent(String encodedQueueInfo) {
        this.encodedQueueInfo = encodedQueueInfo;
        this.purgedCount = SettableFuture.create();
        this.isEventComplete = SettableFuture.create();
    }

    /**
     * Get a mock representation of a StorageQueue coming with event.
     * This decodes the string representation and create the object.
     *
     * @return a mock representation of a StorageQueue
     */
    public StorageQueue toStorageQueue() {
        return new StorageQueue(encodedQueueInfo);
    }


    @Override
    public void updateState() throws AndesException {
        switch (eventType) {
            case SYNC_QUEUE_CREATE_EVENT:
                contextInformationManager.syncQueueCreate(this);
                //mark the completion of the queue addition operation
                isEventComplete.set(true);
                break;
            case SYNC_QUEUE_DELETE_EVENT:
                contextInformationManager.syncQueueDelete(this);
                break;
            case SYNC_QUEUE_PURGE_EVENT:
                handlePurgeEvent();
                break;
            default:
                log.error("Event type not set properly " + eventType);
                break;
        }
    }

    @Override
    public String eventInfo() {
        return eventType.toString();
    }

    @Override
    public boolean isActionableWhenPassive() {
        return true;
    }

    private void handlePurgeEvent() {
        int count = 0;
        try {
            contextInformationManager.handleQueuePurgeNotification(this);
            purgedCount.set(count);
        } catch (AndesException e) {
            purgedCount.setException(e);
        } finally {
            // For other exceptions value will be -1;
            purgedCount.set(count);
        }
    }

    /**
     * Update the event to a create queue sync event
     *
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForSyncCreateQueue(AndesContextInformationManager contextInformationManager) {
        eventType = EventType.SYNC_QUEUE_CREATE_EVENT;
        this.contextInformationManager = contextInformationManager;
    }

    /**
     * Update the event to be a delete queue sync event
     *
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForSyncDeleteQueue(AndesContextInformationManager contextInformationManager) {
        eventType = EventType.SYNC_QUEUE_DELETE_EVENT;
        this.contextInformationManager = contextInformationManager;
    }

    /**
     * Update the event to be a queue purging sync event
     *
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForSyncPurgeQueue(AndesContextInformationManager contextInformationManager) {
        eventType = EventType.SYNC_QUEUE_PURGE_EVENT;
        this.contextInformationManager = contextInformationManager;
    }

}
