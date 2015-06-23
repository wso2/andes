/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.disruptor.inbound;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContextInformationManager;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.MessagingEngine;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Queue related inbound events are handled through this method
 */
public class InboundQueueEvent extends AndesQueue implements AndesInboundStateEvent {

    private static Log log = LogFactory.getLog(InboundQueueEvent.class);

    /**
     * Supported state events
     */
    private enum EventType {

        /**
         * Queue purging event related event type
         */
        QUEUE_PURGE_EVENT,

        /**
         * Is queue deletable check related event type
         */
        IS_QUEUE_DELETABLE_EVENT,

        /**
         * Delete the queue from DB related event type
         */
        DELETE_QUEUE_EVENT,

        /**
         * Create a queue in Andes related event type
         */
        CREATE_QUEUE_EVENT,
    }

    /**
     * Event type this event
     */
    private EventType eventType;

    /**
     * Reference to AndesContextInformationManager to update create/ remove queue state
     */
    private AndesContextInformationManager contextInformationManager;

    /**
     * Reference to MessagingEngine for queue purging
     */
    private MessagingEngine messagingEngine;

    /**
     * Whether the queue is used to store topic messages
     */
    private boolean isTopic;

    /**
     * Purged message count as a future. When a purging is done purged message count is set. Interested user can use 
     * InboundQueueEvent#getPurgedCount method to get the purged count from this async event. 
     * InboundQueueEvent#getPurgedCount method is a blocking call.  
     */
    SettableFuture<Integer> purgedCount;

    /**
     * Future is used to retrieve the value set when by State event handler based state change event.
     * By calling the get method on this future, caller of the method can wait till the operation is done
     */
    SettableFuture<Boolean> isQueueDeletable;

    /**
     * create an instance of andes queue
     *
     * @param queueName   name of the queue
     * @param queueOwner  owner of the queue (virtual host)
     * @param isExclusive is queue exclusive
     * @param isDurable   is queue durable
     */
    public InboundQueueEvent(String queueName, String queueOwner, boolean isExclusive, boolean isDurable) {
        super(queueName, queueOwner, isExclusive, isDurable);
        purgedCount = SettableFuture.create();
        isQueueDeletable = SettableFuture.create();
        isTopic = false;
    }

    /**
     * create an instance of andes queue
     *
     * @param queueAsStr queue information as encoded string
     */
    public InboundQueueEvent(String queueAsStr) {
        super(queueAsStr);
        purgedCount = SettableFuture.create();
        isTopic = false;
    }

    @Override
    public void updateState() throws AndesException {
        switch (eventType) {
            case CREATE_QUEUE_EVENT:
                contextInformationManager.createQueue(this);
                break;
            case DELETE_QUEUE_EVENT:
                contextInformationManager.deleteQueue(queueName);
                break;
            case QUEUE_PURGE_EVENT:
                handlePurgeEvent();
                break;
            case IS_QUEUE_DELETABLE_EVENT:
                handleIsQueueDeletableEvent();
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

    private void handleIsQueueDeletableEvent() {
        boolean queueDeletable = false;
        try {
            queueDeletable = contextInformationManager.checkIfQueueDeletable(queueName);
        } catch (AndesException e) {
            isQueueDeletable.setException(e);
        } finally {
            // For other exceptions value will be set to false
            isQueueDeletable.set(queueDeletable);
        }
    }

    private void handlePurgeEvent() {
        int count = -1;
        try {
            count = messagingEngine.purgeMessages(queueName, queueOwner, isTopic,null);
            purgedCount.set(count);
        } catch (AndesException e) {
            purgedCount.setException(e);
        } finally {
            // For other exceptions value will be -1;
            purgedCount.set(count);
        }
    }

    /**
     * Update the event to a create Queue event
     *
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForCreateQueue(AndesContextInformationManager contextInformationManager) {
        eventType = EventType.CREATE_QUEUE_EVENT;
        this.contextInformationManager = contextInformationManager;
    }

    /**
     * Update the event to be a delete queue event
     *
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForDeleteQueue(AndesContextInformationManager contextInformationManager) {
        eventType = EventType.DELETE_QUEUE_EVENT;
        this.contextInformationManager = contextInformationManager;
    }

    /**
     * Update the event to be a queue purging event
     *
     * @param messagingEngine MessagingEngine
     * @param isTopic         is the queue is used to store topic messages
     */
    public void purgeQueue(MessagingEngine messagingEngine, boolean isTopic) {
        eventType = EventType.QUEUE_PURGE_EVENT;
        this.isTopic = isTopic;
        this.messagingEngine = messagingEngine;
    }

    /**
     * Returns Number of messages purged. This wait till Disruptor process the purge event and return the purge count
     * or timeout exceeds 
     * @param timeout Timeout to wait for 
     * @param timeUnit TimeUnit
     * @return Purged message count as an Integer
     * @throws AndesException
     * @throws TimeoutException
     */
    public Integer getPurgedCount(long timeout, TimeUnit timeUnit) throws AndesException, TimeoutException {
        try {
            return purgedCount.get(timeout, timeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new AndesException("Error while purging queue " + queueName, e);
        }
        return -1;
    }

    /**
     * Prepare event for check if queue deletable
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForCheckIfQueueDeletable(AndesContextInformationManager contextInformationManager) {
        eventType = EventType.IS_QUEUE_DELETABLE_EVENT;
        this.contextInformationManager = contextInformationManager;
    }

    /**
     * Returns whether the queue can be deletable on not 
     * @return True if deletable and wise versa
     * @throws AndesException
     */
    public boolean IsQueueDeletable() throws AndesException {
        try {
            return isQueueDeletable.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new AndesException("Error occurred while checking is queue deletable", e);
        }
        return false;
    }

}