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
 * This class represents a ClusterNotificationAgent which does nothing.
 * Usually this is set when operating in standalone mode
 */
public class StandaloneMockNotificationAgent implements ClusterNotificationAgent {

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyMessageRouterChange(AndesMessageRouter messageRouter, ClusterNotificationListener
            .MessageRouterChange changeType) throws AndesException {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyQueueChange(StorageQueue storageQueue, ClusterNotificationListener.QueueChange changeType)
            throws AndesException {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyBindingsChange(AndesBinding binding, ClusterNotificationListener.BindingChange changeType)
            throws AndesException {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifySubscriptionsChange(AndesSubscription subscription, ClusterNotificationListener
            .SubscriptionChange changeType) throws AndesException {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyAnyDBChange() throws AndesException {

    }
}
