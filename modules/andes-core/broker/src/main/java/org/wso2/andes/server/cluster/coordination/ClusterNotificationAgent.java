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

package org.wso2.andes.server.cluster.coordination;


import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.ClusterNotificationListener;
import org.wso2.andes.kernel.router.AndesMessageRouter;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.StorageQueue;

/**
 * Interface for implementations notifying message router, queue, binding and subscription
 * changes to cluster. Any handler listening to these notifications should
 * implement {@link org.wso2.andes.kernel.ClusterNotificationListener}
 */
public interface ClusterNotificationAgent {

    /**
     * Notify a message router change
     *
     * @param messageRouter message router
     * @param changeType    change made
     * @throws AndesException
     */
    void notifyMessageRouterChange(AndesMessageRouter messageRouter,
                                   ClusterNotificationListener.MessageRouterChange changeType) throws AndesException;

    /**
     * Notify a queue change
     *
     * @param storageQueue queue
     * @param changeType   change made
     * @throws AndesException
     */
    void notifyQueueChange(StorageQueue storageQueue, ClusterNotificationListener.QueueChange changeType)
            throws AndesException;

    /**
     * Notify a binding change
     *
     * @param binding    binding
     * @param changeType change made
     * @throws AndesException
     */
    void notifyBindingsChange(AndesBinding binding, ClusterNotificationListener.BindingChange changeType)
            throws AndesException;

    /**
     * Notify a subscription change
     *
     * @param subscription subscription
     * @param changeType   change made
     * @throws AndesException
     */
    void notifySubscriptionsChange(AndesSubscription subscription,
                                   ClusterNotificationListener.SubscriptionChange changeType) throws AndesException;

    /**
     * Notify any DB change in general
     *
     * @throws AndesException
     */
    void notifyAnyDBChange() throws AndesException;

}
