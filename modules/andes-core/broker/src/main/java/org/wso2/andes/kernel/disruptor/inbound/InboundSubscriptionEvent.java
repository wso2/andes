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
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.SubscriptionAlreadyExistsException;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.kernel.subscription.SubscriberConnection;

import java.util.concurrent.ExecutionException;

/**
 * Class to hold information relevant to open and close subscription
 */
public class InboundSubscriptionEvent implements AndesInboundStateEvent {

    /**
     * A thread to send consume-ok frame after opening the subscription.
     */
    private Runnable postOpenSubscriptionAction;

    public Runnable getPostOpenSubscriptionAction() {
        return postOpenSubscriptionAction;
    }

    private static Log log = LogFactory.getLog(InboundSubscriptionEvent.class);

    /**
     * Supported state events
     */
    public enum EventType {

        /**
         * New local subscription created event
         */
        OPEN_SUBSCRIPTION_EVENT,

        /**
         * Close a local subscription event
         */
        CLOSE_SUBSCRIPTION_EVENT

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

    /**
     * Local subscription reference related to subscription event
     */
    private SubscriberConnection subscriberConnection;

    private ProtocolType protocol;

    private String subscriptionIdentifier;

    private String boundStorageQueueName;

    private String routingKey;

    public InboundSubscriptionEvent(ProtocolType protocol,
                                    String subscriptionIdentifier,
                                    String boundStorageQueueName,
                                    String routingKey,
                                    SubscriberConnection subscription,
                                    Runnable sendConsumeOk) {
        this.protocol = protocol;
        this.subscriptionIdentifier = subscriptionIdentifier;
        this.boundStorageQueueName = boundStorageQueueName;
        this.routingKey = routingKey;
        this.subscriberConnection = subscription;
        this.postOpenSubscriptionAction = sendConsumeOk;
    }

    public InboundSubscriptionEvent(ProtocolType protocol,
                                    String subscriptionIdentifier,
                                    String boundStorageQueueName,
                                    String routingKey,
                                    SubscriberConnection subscription) {
        this.protocol = protocol;
        this.subscriptionIdentifier = subscriptionIdentifier;
        this.boundStorageQueueName = boundStorageQueueName;
        this.routingKey = routingKey;
        this.subscriberConnection = subscription;
        this.postOpenSubscriptionAction = new Runnable() {
            @Override
            public void run() {
                //Setting empty runnable to avoid NPE when try to run post subscription action later.
            }
        };
    }

    public SubscriberConnection getSubscriber() {
        return subscriberConnection;
    }

    public String getBoundStorageQueueName() {
        return boundStorageQueueName;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    /**
     * Get the protocol subscription is made
     * @return ProtocolType
     */
    public ProtocolType getProtocol() {
        return protocol;
    }

    /**
     * Identifier of the subscription. For durable topic subscriptions
     * this would be the subscription ID
     * @return subscription identifier
     */
    public String getSubscriptionIdentifier() {
        return subscriptionIdentifier;
    }


    @Override
    public void updateState() throws AndesException {
        switch (eventType) {
            case OPEN_SUBSCRIPTION_EVENT:
                handleOpenSubscriptionEvent();
                break;
            case CLOSE_SUBSCRIPTION_EVENT:
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
            subscriptionManager.closeLocalSubscription(this);
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
            subscriptionManager.addLocalSubscription(this);
            isComplete = true;
        } catch (AndesException e) {
            // exception will be handled by receiver
            future.setException(e);
        } finally {
            future.set(isComplete);
        }

    }

    /**
     * Prepare new subscription to publish to disruptor.
     *
     * @param subscriptionManager AndesSubscriptionManager
     */
    public void prepareForNewSubscription(AndesSubscriptionManager subscriptionManager) {
        eventType = EventType.OPEN_SUBSCRIPTION_EVENT;
        this.subscriptionManager = subscriptionManager;
    }
    
    public void prepareForCloseSubscription(AndesSubscriptionManager subscriptionManager) {
        eventType = EventType.CLOSE_SUBSCRIPTION_EVENT;
        this.subscriptionManager = subscriptionManager;
    }
    
    public boolean waitForCompletion() throws AndesException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            Throwable originalException = e.getCause();
            if (originalException instanceof SubscriptionAlreadyExistsException) {
                throw (SubscriptionAlreadyExistsException) originalException;
            } else if (originalException instanceof AndesException) {
                throw (AndesException) originalException;
            } else {
                // No point in throwing an exception here and disrupting the server. A warning is sufficient.
                log.warn("Error occurred while processing event '" + eventType  + "' for channel id "
                        + subscriberConnection.getProtocolChannelID(), originalException);
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
