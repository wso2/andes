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
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.SubscriptionAlreadyExistsException;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.kernel.subscription.SubscriptionException;

import java.util.concurrent.ExecutionException;

/**
 * Inbound event for disruptor representing a subscription change notification
 */
public class InboundSubscriptionSyncEvent implements AndesInboundStateEvent {

    private static Log log = LogFactory.getLog(InboundSubscriptionSyncEvent.class);

    /**
     * Supported state events
     */
    public enum EventType {

        /**
         * New remote subscription created event
         */
        SYNC_SUBSCRIPTION_CREATE_EVENT,

        /**
         * Close a remote subscription event
         */
        SYNC_SUBSCRIPTION_CLOSE_EVENT,

    }

    /**
     * Type of subscription event
     */
    private EventType eventType;

    /**
     * Future to wait for subscription open/close event to be completed. Disruptor based async call will become a
     * blocking call by waiting on this future.
     * This will assure ordered event processing through Disruptor plus synchronous behaviour through future get call
     * after publishing event to Disruptor.
     */
    private SettableFuture<Boolean> future = SettableFuture.create();


    /**
     * Reference to subscription manager to update subscription event
     */
    private AndesSubscriptionManager subscriptionManager;

    private String encodedSubscriptionEventInfo;

    /**
     * Create a event representing a subscription change
     *
     * @param encodedSubscription encoded information of a
     *                            {@link org.wso2.andes.kernel.subscription.AndesSubscription}
     */
    public InboundSubscriptionSyncEvent(String encodedSubscription) {
        this.encodedSubscriptionEventInfo = encodedSubscription;
    }

    /**
     * Get string representation of a {
     *
     * @return String with subscriber information
     * @link org.wso2.andes.kernel.subscription.AndesSubscription}
     */
    public String getEncodedSubscription() {
        return encodedSubscriptionEventInfo;
    }


    @Override
    public void updateState() throws AndesException {
        switch (eventType) {
            case SYNC_SUBSCRIPTION_CREATE_EVENT:
                handleOpenSubscriptionEvent();
                break;
            case SYNC_SUBSCRIPTION_CLOSE_EVENT:
                handleCloseSubscriptionEvent();
                break;
            default:
                log.error("Event type not set properly " + eventType);
                break;
        }
    }


    private void handleCloseSubscriptionEvent() {
        boolean isComplete = false;
        try {
            subscriptionManager.closeRemoteSubscription(this);
            isComplete = true;
        } catch (AndesException e) {
            future.setException(e);
        } finally {
            future.set(isComplete);
        }
    }

    private void handleOpenSubscriptionEvent() {
        boolean isComplete = false;

        try {
            subscriptionManager.addRemoteSubscription(this);
            isComplete = true;
        } catch (SubscriptionException e) {
            // exception will be handled by receiver
            future.setException(e);
        } finally {
            future.set(isComplete);
        }

    }

    /**
     * Prepare remote subscription create event to publish to disruptor. This will
     * modify this node's subscription store adding the remote subscription information
     *
     * @param subscriptionManager AndesSubscriptionManager
     */
    public void prepareForRemoteSubscriptionAdd(AndesSubscriptionManager subscriptionManager) {
        eventType = EventType.SYNC_SUBSCRIPTION_CREATE_EVENT;
        this.subscriptionManager = subscriptionManager;
    }

    /**
     * Prepare remote subscription close event to publish to disruptor. This will
     * modify this node's subscription store removing the remote subscription information
     *
     * @param subscriptionManager AndesSubscriptionManager
     */
    public void prepareForRemoteSubscriptionClose(AndesSubscriptionManager subscriptionManager) {
        eventType = EventType.SYNC_SUBSCRIPTION_CLOSE_EVENT;
        this.subscriptionManager = subscriptionManager;
    }


    public boolean waitForCompletion() throws SubscriptionAlreadyExistsException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SubscriptionAlreadyExistsException) {
                throw (SubscriptionAlreadyExistsException) e.getCause();
            } else {
                // No point in throwing an exception here and disrupting the server. A warning is sufficient.
                log.warn("Error occurred while processing event '" + eventType + "' for subscriber "
                        + encodedSubscriptionEventInfo);
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String eventInfo() {
        return eventType.toString();
    }
}
