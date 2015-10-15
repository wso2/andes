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

import java.util.List;
import java.util.Set;

/**
 * Interface to handle cluster subscriptions when wildcards are involved.
 */
public interface ClusterSubscriptionHandler {

    /**
     * Add a wildcard subscription to the underlying data structure.
     *
     * @param subscription The subscription to be added
     * @throws AndesException
     */
    public void addWildCardSubscription(AndesSubscription subscription) throws AndesException;

    /**
     * Update a wildcard subscription to the underlying data structure
     * @param subscription The subscription to be updated
     */
    public void updateWildCardSubscription(AndesSubscription subscription);

    /**
     * Check if a subscription is already available.
     *
     * @param subscription The subscription to check for existence
     * @return True if available
     */
    public boolean isSubscriptionAvailable(AndesSubscription subscription);

    /**
     * Remove a wildcard subscription from the underlying data structure.
     *
     * @param subscription The subscription to remove
     */
    public void removeWildCardSubscription(AndesSubscription subscription);

    /**
     * Get subscription that are matching for a given non-wildcard destination.
     *
     * @param destination The non-wildcard destintion to match
     * @return Set of matching subscriptions
     */
    public Set<AndesSubscription> getMatchingWildCardSubscriptions(String destination);

    /**
     * Get all the subscriptions that are saved in the underlying data structure.
     *
     * @return A set of all the subscriptions saved
     */
    public List<AndesSubscription> getAllWildCardSubscriptions();

    /**
     * Get all topics that these subscribers have subscribed to
     *
     * @return Set of all topics
     */
    public Set<String> getAllTopics();
}
