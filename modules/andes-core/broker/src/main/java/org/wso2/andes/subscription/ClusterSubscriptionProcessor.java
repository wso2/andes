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
import org.wso2.andes.kernel.AndesSubscription.SubscriptionType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * Process adding, removing, updating and retrieving subscriptions managing each subscription type accordingly.
 * The main responsibility is to redirect requests to handle wildcard subscriptions to their specific subscription
 * processor. But can be used to handle all subscriptions.
 * </p>
 * <p>
 * If different subscription types needs different handlers to process subscriptions they can extend {@link
 * ClusterSubscriptionHandler} and use {@link ClusterSubscriptionProcessorBuilder} to get {@link
 * ClusterSubscriptionProcessor} intialized with relevant subscription handler for each subscription type.
 * </p>
 * <p>
 * For one subscription type only one subscription handler is allowed.
 * </p>
 */
public class ClusterSubscriptionProcessor {


    /**
     * Keeps all the bitmap handlers for each subscription type.
     */
    private Map<SubscriptionType, ClusterSubscriptionHandler> subscriptionHandlers = new HashMap<SubscriptionType,
            ClusterSubscriptionHandler>();

    /**
     * Add a processor for a given subscription type.
     *
     * @param subscriptionType The subscription processor to handle the given subscription type
     * @throws AndesException
     */
    protected void addSubscriptionType(SubscriptionType subscriptionType,
                                       ClusterSubscriptionHandler subscriptionHandler) throws AndesException {
        subscriptionHandlers.put(subscriptionType, subscriptionHandler);
    }

    /**
     * Always retrieve the correct subscription handler through this method so validations can happen and can avoid
     * unnecessary null pointers in case the subscription type is not found.
     *
     * @param subscriptionType The subscription type of the bitmap handler
     * @return The bitmap handler
     */
    private ClusterSubscriptionHandler getClusterSubscriptionHandler(SubscriptionType subscriptionType) throws
            AndesException {
        ClusterSubscriptionHandler subscriptionHandler = subscriptionHandlers.get(subscriptionType);

        if (null == subscriptionHandler) {
            throw new AndesException("Subscription type " + subscriptionType + " is not recognized.");
        }

        return subscriptionHandler;
    }

    /**
     * Add a wildcard subscription to it's specific subscription handler.
     *
     * @param subscription The subscription to be added
     * @throws AndesException
     */
    public void addWildCardSubscription(AndesSubscription subscription) throws AndesException {
        ClusterSubscriptionHandler bitMapHandler = getClusterSubscriptionHandler(subscription.getSubscriptionType());
        bitMapHandler.addWildCardSubscription(subscription);
    }

    /**
     * Update a wildcard subscription to it's specific subscription handler.
     * @param subscription he subscription to be updated
     * @throws AndesException
     */
    public void updateWildCardSubscription(AndesSubscription subscription) throws AndesException {
        ClusterSubscriptionHandler bitMapHandler = getClusterSubscriptionHandler(subscription.getSubscriptionType());
        bitMapHandler.updateWildCardSubscription(subscription);
    }

    /**
     * Check if a subscription is already available.
     *
     * @param subscription The subscription to be checked
     * @return True if subscription is found in the handler
     * @throws AndesException
     */
    public boolean isSubscriptionAvailable(AndesSubscription subscription) throws AndesException  {
        ClusterSubscriptionHandler bitMapHandler = getClusterSubscriptionHandler(subscription.getSubscriptionType());
        return bitMapHandler.isSubscriptionAvailable(subscription);
    }

    /**
     * Remove a wildcard subscription from it's specific subscription handler.
     *
     * @param subscription The subscription to remove
     * @throws AndesException
     */
    public void removeWildCardSubscription(AndesSubscription subscription) throws AndesException  {
        ClusterSubscriptionHandler bitMapHandler = getClusterSubscriptionHandler(subscription.getSubscriptionType());
        bitMapHandler.removeWildCardSubscription(subscription);
    }

    /**
     * Get valid subscriptions for a given non-wildcard destination from it's specific subscription handler.
     *
     * @param destination The non-wildcard destination
     * @param subscriptionType The subscription type to resolve the specific subscription handler
     * @return Set of matching subscriptions
     * @throws AndesException
     */
    public Set<AndesSubscription> getMatchingSubscriptions(String destination, SubscriptionType subscriptionType)
            throws AndesException {
        ClusterSubscriptionHandler bitMapHandler = getClusterSubscriptionHandler(subscriptionType);
        return bitMapHandler.getMatchingWildCardSubscriptions(destination);
    }

    /**
     * Get all active subscribers registered within all types of subscription handlers for a specific node.
     *
     * @param nodeID The Id of the node
     * @return Set of active subscriptions for the given node
     */
    public Set<AndesSubscription> getActiveClusterSubscribersForNode(String nodeID) {
        Set<AndesSubscription> subscriptions = new HashSet<AndesSubscription>();

        for (Map.Entry<SubscriptionType, ClusterSubscriptionHandler> entry : subscriptionHandlers.entrySet()) {
            for (AndesSubscription subscription : entry.getValue().getAllWildCardSubscriptions()) {
                if (subscription.getSubscribedNode().equals(nodeID) && subscription.hasExternalSubscriptions()) {
                    subscriptions.add(subscription);
                }
            }
        }

        return subscriptions;
    }

    /**
     * Get all topics that these subscribers have subscribed to
     *
     * @return Set of all topics
     */
    public Set<String> getAllTopics() {
        Set<String> topics = new HashSet<>();

        for (Map.Entry<SubscriptionType, ClusterSubscriptionHandler> entry : subscriptionHandlers.entrySet()) {
            topics.addAll(entry.getValue().getAllTopics());
        }

        return topics;
    }
}
