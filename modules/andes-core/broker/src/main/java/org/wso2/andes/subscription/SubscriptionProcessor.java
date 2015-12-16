/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.andes.subscription;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.DestinationType;

import java.util.*;

/**
 * <p>
 * Process adding, removing, updating and retrieving subscriptions managing each subscription type accordingly.
 * The main responsibility is to redirect requests to handle wildcard subscriptions to their specific subscription
 * processor. But can be used to handle all subscriptions.
 * </p>
 * <p>
 * If different subscription types needs different handlers to process subscriptions they can extend {@link
 * SubscriptionHandler} and use {@link SubscriptionProcessorBuilder} to get {@link
 * SubscriptionProcessor} intialized with relevant subscription handler for each subscription type.
 * </p>
 * <p>
 * For one subscription type only one subscription handler is allowed.
 * </p>
 */
public class SubscriptionProcessor {


    /**
     * Keeps all the handlers for each subscription type.
     */
    private Map<ProtocolType, SubscriptionHandler> subscriptionHandlers = new EnumMap<>(ProtocolType.class);

    /**
     * Add a processor for a given protocol
     *
     * @param protocolType The subscription processor to handle the given protocol
     * @throws AndesException
     */
    protected void addProtocolType(ProtocolType protocolType,
                                   SubscriptionHandler subscriptionHandler) throws AndesException {
        subscriptionHandlers.put(protocolType, subscriptionHandler);
    }

    /**
     * Always retrieve the correct subscription handler through this method so validations can happen and can avoid
     * unnecessary null pointers in case the subscription type is not found.
     *
     * @param protocolType The subscription type of the handler
     * @return The subscription handler
     */
    private SubscriptionHandler getSubscriptionHandler(ProtocolType protocolType) throws
            AndesException {
        SubscriptionHandler subscriptionHandler = subscriptionHandlers.get(protocolType);

        if (null == subscriptionHandler) {
            throw new AndesException("Subscription type " + protocolType + " is not recognized.");
        }

        return subscriptionHandler;
    }

    /**
     * Add a subscription to it's specific subscription handler.
     *
     * @param subscription The subscription to be added
     * @throws AndesException
     */
    public void addSubscription(AndesSubscription subscription) throws AndesException {
        SubscriptionHandler subscriptionHandler = getSubscriptionHandler(subscription.getProtocolType());
        subscriptionHandler.addSubscription(subscription);
    }

    /**
     * Update a wildcard subscription to it's specific subscription handler.
     * @param subscription he subscription to be updated
     * @throws AndesException
     */
    public void updateSubscription(AndesSubscription subscription) throws AndesException {
        SubscriptionHandler subscriptionHandler = getSubscriptionHandler(subscription.getProtocolType());
        subscriptionHandler.updateSubscription(subscription);
    }

    /**
     * Check if a subscription is already available.
     *
     * @param subscription The subscription to be checked
     * @return True if subscription is found in the handler
     * @throws AndesException
     */
    public boolean isSubscriptionAvailable(AndesSubscription subscription) throws AndesException  {
        SubscriptionHandler subscriptionHandler = getSubscriptionHandler(subscription.getProtocolType());
        return subscriptionHandler.isSubscriptionAvailable(subscription);
    }

    /**
     * Remove a subscription from it's specific subscription handler.
     *
     * @param subscription The subscription to remove
     * @throws AndesException
     */
    public void removeSubscription(AndesSubscription subscription) throws AndesException  {
        SubscriptionHandler subscriptionHandler = getSubscriptionHandler(subscription.getProtocolType());
        subscriptionHandler.removeSubscription(subscription);
    }

    /**
     * Get valid subscriptions for a given non-wildcard destination from it's specific subscription handler.
     *
     * @param destination The non-wildcard destination
     * @param protocolType The subscription type to resolve the specific subscription handler
     * @param destinationType The type of the destination to retrieve subscriptions for
     * @return Set of matching subscriptions
     * @throws AndesException
     */
    public Set<AndesSubscription> getMatchingSubscriptions(String destination, ProtocolType protocolType,
                                                           DestinationType destinationType)
            throws AndesException {
        SubscriptionHandler subscriptionHandler = getSubscriptionHandler(protocolType);
        return subscriptionHandler.getMatchingSubscriptions(destination, destinationType);
    }

    /**
     * Get all active subscribers registered within all types of subscription handlers for a specific node.
     *
     * @param nodeID The Id of the node
     * @return Set of active subscriptions for the given node
     */
    public Set<AndesSubscription> getActiveSubscribersForNode(String nodeID) {
        Set<AndesSubscription> subscriptions = new HashSet<>();

        for (Map.Entry<ProtocolType, SubscriptionHandler> entry : subscriptionHandlers.entrySet()) {
            for (AndesSubscription subscription : entry.getValue().getAllSubscriptions()) {
                if (subscription.getSubscribedNode().equals(nodeID) && subscription.hasExternalSubscriptions()) {
                    subscriptions.add(subscription);
                }
            }
        }

        return subscriptions;
    }

    /**
     * Get all destinations that these subscribers have subscribed to
     *
     * @param destinationType The type of the destination to retrieve all destinations for
     * @return Set of all topics
     */
    public Set<String> getAllDestinations(DestinationType destinationType) {
        Set<String> topics = new HashSet<>();

        for (Map.Entry<ProtocolType, SubscriptionHandler> entry : subscriptionHandlers.entrySet()) {
            topics.addAll(entry.getValue().getAllDestinations(destinationType));
        }

        return topics;
    }

    /**
     * Retrieve all the subscriptions contained in all the handlers.
     *
     * This method can be used where all available subscriptions should be checked. This method can be used instead
     * of retrieving all the destinations, and then retrieving all the available subscriptions for those destination
     * in a loop.
     *
     * @return All the subscriptions that are saved in memory
     */
    public Set<AndesSubscription> getAllSubscriptions() {
        Set<AndesSubscription> allSubscriptions = new HashSet<>();

        for (Map.Entry<ProtocolType, SubscriptionHandler> entry : subscriptionHandlers.entrySet()) {
            allSubscriptions.addAll(entry.getValue().getAllSubscriptions());
        }

        return allSubscriptions;
    }

    /**
     * Get all subscriptions for a specific destination type of a protocol type.
     *
     * @param protocolType The protocol for which the subscriptions needs to be retrieved
     * @param destinationType The destination type for which the subscriptions needs to be retrieved
     * @return Set of matching subscriptions
     * oiajsdfja09320398_akafixthis_************
     */
    public Set<AndesSubscription> getAllSubscriptionsForDestinationType(ProtocolType protocolType, DestinationType destinationType) {
        Set<AndesSubscription> subscriptionsForDestinationType = new HashSet<>();
        for (AndesSubscription subscription : subscriptionHandlers.get(protocolType).getAllSubscriptions()) {
            if (subscription.getDestinationType() == destinationType) {
                subscriptionsForDestinationType.add(subscription);
            }
        }

        return subscriptionsForDestinationType;
    }
}
