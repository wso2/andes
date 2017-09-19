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

import java.util.concurrent.ExecutionException;

/**
 * Binding related inbound event
 */
public class InboundBindingEvent implements AndesInboundStateEvent {

    private static Log log = LogFactory.getLog(InboundBindingEvent.class);

    /**
     * Supported state events
     */
    private enum EventType {

        /**
         * Create a binding in Andes related event type
         */
        ADD_BINDING_EVENT,

        /**
         * Delete a binding in Andes related event type
         */
        REMOVE_BINDING_EVENT,
    }

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
     * Reference to AndesContextInformationManager for add/remove binding
     */
    private AndesContextInformationManager contextInformationManager;

    /**
     * Andes binding related event type of this event
     */
    private EventType eventType;

    /**
     * Name of the message router queue is bound to
     */
    private String boundMessageRouterName;

    /**
     * Storage queue to which binding is created
     */
    private QueueInfo boundedQueue;

    /**
     * Routing key by which storage queue binds to message router
     */
    private String routingKey;


    /**
     * Create a binding event to be published to disruptor. Also prepare the event before publishing by
     * calling prepare methods.
     *
     * @param boundedQueue           Information on the queue bound
     * @param boundMessageRouterName name of message router
     * @param routingKey             binding key
     */
    public InboundBindingEvent(QueueInfo boundedQueue, String boundMessageRouterName, String routingKey) {
        this.boundMessageRouterName = boundMessageRouterName;
        this.boundedQueue = boundedQueue;
        this.routingKey = routingKey;
        this.isEventComplete = SettableFuture.create();
    }

    /**
     * Get name of the message router of binding event
     *
     * @return names of AndesMessageRouter
     */
    public String getBoundMessageRouterName() {
        return boundMessageRouterName;
    }

    /**
     * Get Information on queue associated to binding.
     *
     * @return QueueInfo bean
     */
    public QueueInfo getBoundedQueue() {
        return boundedQueue;
    }

    /**
     * Get binding key of binding event
     *
     * @return binding key
     */
    public String getBindingKey() {
        return routingKey;
    }

    /**
     * {@inheritDoc}
     *
     * @throws AndesException
     */
    @Override
    public void updateState() throws AndesException {
        switch (eventType) {
            case ADD_BINDING_EVENT:
                handleCreateBinding();
                break;
            case REMOVE_BINDING_EVENT:
                handleRemoveBinding();
                break;
            default:
                log.error("Event type not set properly." + eventType);
                break;
        }
    }

    /**
     * Perform binding creation
     */
    private void handleCreateBinding() {
        boolean isComplete = false;
        try {
            contextInformationManager.createBinding(this);
            isComplete = true;
        } catch (AndesException e) {
            isEventComplete.setException(e);
        } finally {
            isEventComplete.set(isComplete);
        }
    }

    /**
     * Perform binding removal
     */
    private void handleRemoveBinding() {
        boolean isComplete = false;
        try {
            contextInformationManager.removeBinding(this);
            isComplete = true;
        } catch (AndesException e) {
            isEventComplete.setException(e);
        } finally {
            isEventComplete.set(isComplete);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String eventInfo() {
        return eventType.toString();
    }

    @Override
    public boolean isActionableWhenPassive() {
        return false;
    }

    /**
     * Update event to be an add binding event
     *
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForAddBindingEvent(AndesContextInformationManager contextInformationManager) {
        this.contextInformationManager = contextInformationManager;
        eventType = EventType.ADD_BINDING_EVENT;
    }

    /**
     * Update event to be a remove binding event
     *
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForRemoveBinding(AndesContextInformationManager contextInformationManager) {
        this.contextInformationManager = contextInformationManager;
        eventType = EventType.REMOVE_BINDING_EVENT;
    }


    /**
     * Wait until event is completed. The caller will be blocked until the
     * event execution is carried out by disruptor
     *
     * @return true if event is successfully completed
     * @throws AndesException
     */
    public boolean waitForCompletion() throws AndesException {
        try {
            return isEventComplete.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new AndesException("Error while processing inbound binding event for queue " + boundedQueue
                    .getQueueName(), e);
        }
        return false;
    }

}
