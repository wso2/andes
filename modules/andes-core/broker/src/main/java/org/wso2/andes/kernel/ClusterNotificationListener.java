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

package org.wso2.andes.kernel;


import org.wso2.andes.server.cluster.coordination.ClusterNotification;

/**
 * Interface for Listener implementations listening to
 * message router, queue, binding and subscription
 * changes in cluster sent from  an
 * implementation of {@link org.wso2.andes.server.cluster.coordination.ClusterNotificationAgent}
 */
public interface ClusterNotificationListener {

    /**
     * Enum for artifacts that is notified
     */
    enum NotifiedArtifact {
        MessageRouter,
        Binding,
        Queue,
        Subscription,
        DBUpdate,
        DynamicDiscoveryUpdate
    }

    /**
     * Enum for Message Router changes
     */
    enum MessageRouterChange {
        Added,
        Deleted
    }

    /**
     * Enum for Message binding changes
     */
    enum BindingChange {
        Added,
        Deleted
    }

    /**
     * Enum for queue changes
     */
    enum QueueChange {
        Added,
        Deleted,
        Purged
    }

    /**
     * Enum for subscription changes
     */
    enum SubscriptionChange {
        Added,
        Closed
    }

    /**
     * Handle cluster notification
     *
     * @param notification notification to handle
     */
    void handleClusterNotification(ClusterNotification notification);

}
