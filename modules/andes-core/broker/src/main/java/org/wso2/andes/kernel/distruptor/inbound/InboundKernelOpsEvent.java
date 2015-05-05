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

package org.wso2.andes.kernel.distruptor.inbound;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.slot.SlotManagerClusterMode;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.registry.ApplicationRegistry;

import java.util.concurrent.ExecutionException;

/**
 * Handles events related to basic kernel operations.
 * Starting and shutting down tasks etc 
 */
public class InboundKernelOpsEvent implements AndesInboundStateEvent {

    private static Log log = LogFactory.getLog(InboundKernelOpsEvent.class);

    public enum EventType {

        /** Stop message delivery in Andes core */
        STOP_MESSAGE_DELIVERY_EVENT,

        /** Start message delivery in Andes core event */
        START_MESSAGE_DELIVERY_EVENT,

        /** Shutdown andes broker messaging engine event*/
        SHUTDOWN_MESSAGING_ENGINE_EVENT,

        /** Start expired message deleting task, notification event */
        START_EXPIRATION_WORKER_EVENT,

        /** Stop expired message deleting task, notification event */
        STOP_EXPIRATION_WORKER_EVENT
    }

    /**
     * Kernel operation event handled by InboundKernelOpsEvent
     */
    private EventType eventType;

    /**
     * Reference to MessagingEngine to process kernel events
     */
    private MessagingEngine messagingEngine;

    /**
     * Future to wait till the task is completed by Disruptor. This is used to make the
     * method calls blocking.
     */
    private SettableFuture<Boolean> taskStatus;
    
    @Override
    public void updateState() throws AndesException {
        Boolean taskComplete =false;

        try {

            switch (eventType) {
                case STOP_MESSAGE_DELIVERY_EVENT:
                    stopMessageDelivery();
                    taskComplete = true;
                    break;
                case START_MESSAGE_DELIVERY_EVENT:
                    startMessageDelivery();
                    taskComplete = true;
                    break;
                case START_EXPIRATION_WORKER_EVENT:
                    startMessageExpirationWorker();
                    taskComplete = true;
                    break;
                case STOP_EXPIRATION_WORKER_EVENT:
                    stopMessageExpirationWorker();
                    taskComplete = true;
                    break;
                default:
                    log.error("Event type not set properly " + eventType);
                    break;
            }
        } catch (Throwable t) {
            // In any type of exception we need to set it so caller waiting on future can be released
            taskStatus.setException(t);
            throw new AndesException("Exception occurred while processing " + eventType, t);
        } finally {
            taskStatus.set(taskComplete);
        }
    }

    @Override
    public String eventInfo() {
        return eventType.toString();
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
        messagingEngine.startMessageExpirationWorker();
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

    public void completePendingMessageStoringOperations() {
        try {
            messagingEngine.completePendingStoreOperations();
        } catch (InterruptedException e) {
            log.error("Interrupted while trying to complete pending message storing operations.", e);
        }
    }

    /**
     * Update event to start message delivery event
     * @param messagingEngine MessagingEngine
     */
    public void prepareForStartMessageDelivery(MessagingEngine messagingEngine) {
        eventType = EventType.START_MESSAGE_DELIVERY_EVENT;
        this.messagingEngine = messagingEngine;
        taskStatus = SettableFuture.create();
    }

    /**
     * Update event to stop message delivery
     * @param messagingEngine MessagingEngine
     */
    public void prepareForStopMessageDelivery(MessagingEngine messagingEngine) {
        eventType = EventType.STOP_MESSAGE_DELIVERY_EVENT;
        this.messagingEngine = messagingEngine;
        taskStatus = SettableFuture.create();
    }

    /**
     * Update event to start message expiration worker event 
     * @param messagingEngine MessagingEngine
     */
    public void prepareForStartMessageExpirationWorker(MessagingEngine messagingEngine) {
        eventType = EventType.START_EXPIRATION_WORKER_EVENT;
        this.messagingEngine = messagingEngine;
        taskStatus = SettableFuture.create();
    }

    /**
     * Update event to start message expiration worker event 
     * @param messagingEngine MessagingEngine
     */
    public void prepareForStopMessageExpirationWorker(MessagingEngine messagingEngine) {
        eventType = EventType.STOP_EXPIRATION_WORKER_EVENT;
        this.messagingEngine = messagingEngine;
        taskStatus = SettableFuture.create();
    }

    /**
     * Sequentially shutting down all andes dependency task when graceful shutdown hook triggered.
     *
     * @param messagingEngine MessageEngine
     * @throws AndesException
     */
    public void gracefulShutdown(MessagingEngine messagingEngine) throws AndesException {

        Boolean taskComplete =false;
        this.messagingEngine = messagingEngine;
        taskStatus = SettableFuture.create();

        try {
            // Stop SlotDeliveryWorkers
            // Stop Thrift Service
            // Stop SlotMessageCounter
            stopMessageDelivery();

            // Close subscriptions
            ClusterResourceHolder.getInstance().getSubscriptionManager().closeAllLocalSubscriptionsOfNode();

            // notify cluster this MB node is shutting down. For other nodes to do recovery tasks
            ClusterResourceHolder.getInstance().getClusterManager().shutDownMyNode();

            //Stop Recovery threads
            AndesKernelBoot.stopHouseKeepingThreads();

            // Shut down Store writing tasks - (after waiting for completion)
            // Shut down message store
            completePendingMessageStoringOperations();

            //Stop Slot manager in coordinator
            if (AndesContext.getInstance().isClusteringEnabled() && AndesContext.getInstance().getClusteringAgent().isCoordinator()) {
                SlotManagerClusterMode.getInstance().shutDownSlotManager();
            }

            // Removes the MinaNetworkHandler, Authentication Handlers, etc. Refer ApplicationRegistry.close()
            ApplicationRegistry.remove();

            // We need this until ApplicationRegistry is done.
            AndesContext.getInstance().getAndesContextStore().close();

            taskComplete = true;

        } finally {
            taskStatus.set(taskComplete);
        }

    }

    public Boolean waitForTaskCompletion() throws AndesException {
        try {
            return taskStatus.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new AndesException("Error occurred while processing event " + eventType, e);
        }
        return false;
    }
}
