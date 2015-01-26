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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessagingEngine;

import static org.wso2.andes.kernel.distruptor.inbound.AndesInboundStateEvent.StateEvent.*;

/**
 * Handles events related to basic kernel operations.
 * Starting and shutting down tasks etc 
 */
public class InboundKernelOpsEvent implements AndesInboundStateEvent {

    private static Log log = LogFactory.getLog(InboundKernelOpsEvent.class);
    
    /**
     * Kernel operation event handled by InboundKernelOpsEvent
     */
    private StateEvent eventType;

    /**
     * Reference to MessagingEngine to process kernel events
     */
    private MessagingEngine messagingEngine;
    
    @Override
    public void updateState() throws AndesException {
        switch (eventType) {
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
            default:
                log.error("Event type not set properly " + eventType);
                break;
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

    /**
     * Update event to start message delivery event
     * @param messagingEngine MessagingEngine
     */
    public void prepareForStartMessageDelivery(MessagingEngine messagingEngine) {
        eventType = START_MESSAGE_DELIVERY_EVENT;
        this.messagingEngine = messagingEngine;
    }

    /**
     * Update event to stop message delivery
     * @param messagingEngine MessagingEngine
     */
    public void prepareForStopMessageDelivery(MessagingEngine messagingEngine) {
        eventType = STOP_MESSAGE_DELIVERY_EVENT;
        this.messagingEngine = messagingEngine;
    }

    /**
     * Update event to start message expiration worker event 
     * @param messagingEngine MessagingEngine
     */
    public void prepareForStartMessageExpirationWorker(MessagingEngine messagingEngine) {
        eventType = START_EXPIRATION_WORKER_EVENT;
        this.messagingEngine = messagingEngine;
    }

    /**
     * Update event to start message expiration worker event 
     * @param messagingEngine MessagingEngine
     */
    public void prepareForStopMessageExpirationWorker(MessagingEngine messagingEngine) {
        eventType = STOP_EXPIRATION_WORKER_EVENT;
        this.messagingEngine = messagingEngine;
    }

    /**
     * Update event to shutdown messaging engine event 
     * @param messagingEngine MessagingEngine
     */
    public void prepareForShutdownMessagingEngine(MessagingEngine messagingEngine) {
        eventType = SHUTDOWN_MESSAGING_ENGINE_EVENT;
        this.messagingEngine = messagingEngine;
    }

    @Override
    public StateEvent getEventType() {
        return eventType;
    }
}
