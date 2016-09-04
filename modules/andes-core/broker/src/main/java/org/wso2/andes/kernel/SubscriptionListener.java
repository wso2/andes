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

import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;

/**
 * Subscription Listener Interface.  Any handler handling a local subscription change should implement
 * this interface. These should get registered in
 * {@link AndesSubscriptionManager}
 * only.
 */
public interface SubscriptionListener {

    /**
     * Handle create/close subscription event
     *
     * @param subscription subscription updated
     * @param changeType change made, create/close
     * @throws AndesException on an issue
     */
    void handleSubscriptionsChange(AndesSubscription subscription,
                                   ClusterNotificationListener.SubscriptionChange changeType) throws AndesException;

}
