/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 */

package org.wso2.andes.kernel;

import org.wso2.andes.subscription.LocalSubscription;

/**
 * Subscription Listener Interface. This has methods related to local subscription changes
 * and cluster subscription changes. Any handler handling a subscription change should implement
 * this interface
 */
public interface SubscriptionListener {

	static enum SubscriptionChange{
        ADDED,
        DELETED,
        DISCONNECTED,
        /**
         * Merge subscriber after a split brain
         */
        MERGED
    }

    /**
     * handle subscription changes in cluster
     * @param subscription subscription changed
     * @param changeType type of change happened
     * @throws AndesException
     */
	public void handleClusterSubscriptionsChanged(AndesSubscription subscription, SubscriptionChange changeType) throws AndesException;

    /**
     * handle local subscription changes
     * @param subscription subscription changed
     * @param changeType type of change happened
     * @throws AndesException
     */
	public void handleLocalSubscriptionsChanged(LocalSubscription subscription, SubscriptionChange changeType) throws AndesException;

}
