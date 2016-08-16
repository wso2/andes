/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 *
 */

package org.wso2.andes.server.cluster.coordination;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesExchange;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.BindingListener;
import org.wso2.andes.kernel.ExchangeListener;
import org.wso2.andes.kernel.QueueListener;
import org.wso2.andes.kernel.SubscriptionListener;
import org.wso2.andes.subscription.BasicSubscription;

/**
 * ClusterNotificationHandler handles cluster notification received.
 */
public class ClusterNotificationHandler {

    private QueueListener queueListener;
    private BindingListener bindingListener;
    private ExchangeListener exchangeListener;
    private SubscriptionListener subscriptionListener;

    private static final Logger log = Logger.getLogger(ClusterNotificationHandler.class);

    /**
     * Register a listener interested on queue changes in cluster
     *
     * @param listener listener to be registered
     */
    public void addQueueListener(QueueListener listener) {
        queueListener = listener;
    }

    /**
     * Register a listener interested on binding changes in cluster
     *
     * @param listener listener to be registered
     */
    public void addBindingListener(BindingListener listener) {
        bindingListener = listener;
    }

    /**
     * Register a listener interested in queue changes in cluster
     *
     * @param listener listener to be registered
     */
    public void addExchangeListener(ExchangeListener listener) {
        exchangeListener = listener;
    }

    /**
     * Register a listener interested in cluster subscription changes
     *
     * @param listener listener to be registered
     */
    public void addSubscriptionListener(SubscriptionListener listener) {
        subscriptionListener = listener;
    }

    /**
     * Notifies the queue listener that a queue has changed.
     *
     * @param queueDetails contains the string representation of the changed queue
     * @param change       type of the queue change. could be one of ADDED/DELETED/PURGED
     */
    public void handleClusterQueuesChanged(String queueDetails, QueueListener.QueueEvent change) {
        if (log.isDebugEnabled()) {
            log.debug("Handling cluster gossip: received a queue change notification " + queueDetails);
        }
        try {
            queueListener.handleClusterQueuesChanged(new AndesQueue(queueDetails), change);
        } catch (AndesException e) {
            log.error("error while handling cluster queue change notification", e);
        }
    }

    /**
     * Notifies the binding listener that a binding has changed.
     *
     * @param bindingDetails contains the string representation of the changed binding
     * @param change         type of the binding change. could be one of ADDED/DELETED
     */
    public void handleClusterBindingsChanged(String bindingDetails, BindingListener.BindingEvent change) {
        if (log.isDebugEnabled()) {
            log.debug("Handling cluster gossip: received a binding change notification " + bindingDetails);
        }
        try {
            bindingListener.handleClusterBindingsChanged(new AndesBinding(bindingDetails), change);
        } catch (AndesException e) {
            log.error("error while handling cluster binding change notification", e);
        }
    }

    /**
     * Notifies the exchange listener that an exchange has changed.
     *
     * @param exchangeDetails contains the string representation of the changed exchange
     * @param change          type of the exchange change. could be one of ADDED/DELETED
     */
    public void handleClusterExchangesChanged(String exchangeDetails, ExchangeListener.ExchangeChange change) {
        if (log.isDebugEnabled()) {
            log.debug("Handling cluster gossip: received a exchange change notification " + exchangeDetails);
        }
        try {
            exchangeListener.handleClusterExchangesChanged(new AndesExchange(exchangeDetails), change);
        } catch (AndesException e) {
            log.error("error while handling cluster exchange change notification", e);
        }
    }

    /**
     * Notifies the subscription listener that a subscription has changed..
     *
     * @param subscriptionDetails contains the string representation of the changed subscription
     * @param change              type of the subscription change. could be one of ADDED/DISCONNECTED/DELETED/MERGED
     */
    public void handleClusterSubscriptionsChanged(String subscriptionDetails, SubscriptionListener.SubscriptionChange
            change) {
        if (log.isDebugEnabled()) {
            log.debug("Handling cluster gossip: received a subscription change notification " + subscriptionDetails);
        }
        try {
            subscriptionListener.handleClusterSubscriptionsChanged(new BasicSubscription(subscriptionDetails), change);
        } catch (AndesException e) {
            log.error("error while handling cluster subscription change notification", e);
        }
    }

}
