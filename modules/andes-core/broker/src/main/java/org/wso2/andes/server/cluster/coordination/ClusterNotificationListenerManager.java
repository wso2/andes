/*
 * Copyright (c)2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.server.cluster.coordination;

import org.wso2.andes.kernel.AndesContextInformationManager;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;

/**
 * The ClusterNotificationListenerManager initializes receiving cluster notifications.
 */
public interface ClusterNotificationListenerManager {

    /**
     * Initializes the listener.
     *
     * @throws AndesException
     */
    void initializeListener(InboundEventManager inboundEventManager, AndesSubscriptionManager subscriptionManager,
                            AndesContextInformationManager contextInformationManager) throws AndesException;

    /**
     * Re-initialize listener
     *
     * @throws AndesException
     */
    void reInitializeListener() throws AndesException;

    /**
     * Clears all persisted cluster notifications at server startup.
     *
     * @throws AndesException
     */
    void clearAllClusterNotifications() throws AndesException;

    /**
     * Stops the cluster event listener.
     *
     * @throws AndesException
     */
    void stopListener() throws AndesException;

}
