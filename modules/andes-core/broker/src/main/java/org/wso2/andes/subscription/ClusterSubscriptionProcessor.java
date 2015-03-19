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

public class ClusterSubscriptionProcessor {


    /**
     * Keeps all the bitmap handlers for each subscription type.
     */
    private Map<SubscriptionType, ClusterSubscriptionHandler> subscriptionHandlers = new HashMap<SubscriptionType, ClusterSubscriptionHandler>();

    /**
     * Add a processor for a
     * @param subscriptionType
     * @throws AndesException
     */
    protected void addSubscriptionType(SubscriptionType subscriptionType, ClusterSubscriptionHandler subscriptionHandler) throws AndesException {
        subscriptionHandlers.put(subscriptionType, subscriptionHandler);
    }

    /**
     * Always retrieve the correct bitmap handler through this method so validations can happen and can avoid
     * unnecessary null pointers in case the subscription type is not found.
     *
     * @param subscriptionType The subscription type of the bitmap handler
     * @return The bitmap handler
     */
    private ClusterSubscriptionHandler getBitMapHandler(SubscriptionType subscriptionType) throws AndesException {
        ClusterSubscriptionHandler subscriptionHandler = subscriptionHandlers.get(subscriptionType);

        if (null == subscriptionHandler) {
            throw new AndesException("Subscription type " + subscriptionType + " is not recognized.");
        }

        return subscriptionHandler;
    }

    public void addWildCardSubscription(AndesSubscription subscription) throws AndesException {
        ClusterSubscriptionHandler bitMapHandler = getBitMapHandler(subscription.getSubscriptionType());
        bitMapHandler.addWildCardSubscription(subscription);
    }
    public void updateWildCardSubscription(AndesSubscription subscription) throws AndesException  {
        ClusterSubscriptionHandler bitMapHandler = getBitMapHandler(subscription.getSubscriptionType());
        bitMapHandler.updateWildCardSubscription(subscription);
    }
    public void removeWildCardSubscription(AndesSubscription subscription) throws AndesException  {
        ClusterSubscriptionHandler bitMapHandler = getBitMapHandler(subscription.getSubscriptionType());
        bitMapHandler.removeWildCardSubscription(subscription);
    }

    public Set<AndesSubscription> getMatchingSubscriptions(String destination, SubscriptionType subscriptionType)
            throws AndesException {
        ClusterSubscriptionHandler bitMapHandler = getBitMapHandler(subscriptionType);
        return bitMapHandler.getMatchingWildCardSubscriptions(destination);
    }

    public Set<AndesSubscription> getActiveClusterSubscribersForNode(String nodeID) {
        Set<AndesSubscription> subscriptions = new HashSet<AndesSubscription>();

        for (Map.Entry<SubscriptionType, ClusterSubscriptionHandler> entry : subscriptionHandlers.entrySet()) {
            for (AndesSubscription subscription : entry.getValue().getAllWildCardSubscriptions()) {
                if (subscription.getSubscribedNode().equals(nodeID)) {
                    subscriptions.add(subscription);
                }
            }
        }

        return subscriptions;
    }
}
