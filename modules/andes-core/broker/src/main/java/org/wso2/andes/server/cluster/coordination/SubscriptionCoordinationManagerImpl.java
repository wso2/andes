/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.pool.AndesExecuter;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;

import java.util.ArrayList;
import java.util.List;

/**
 * This class handles all subscription management tasks
 */
public class SubscriptionCoordinationManagerImpl implements SubscriptionCoordinationManager {


    private static Log log = LogFactory.getLog(SubscriptionCoordinationManagerImpl.class);

    /**
     * HazelcastAgent instance.
     */
    private HazelcastAgent hazelcastAgent;

    /**
     * A list of listeners who should be notified about changes of subscriptions.
     */
    private List<SubscriptionListener> subscriptionListeners = new ArrayList<SubscriptionListener>();

    /**
     * This method must be called just after initializing the SubscriptionCoordinationManagerImpl class
     * for the first time.
     */
    @Override
    public void init() {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            hazelcastAgent = HazelcastAgent.getInstance();
        }
    }

    /**
     * Notify the listeners about the changes happened to Subscriptions.
     *
     * @param subscriptionNotification contains the information about the subscription change.
     */
    @Override
    public void notifySubscriptionChange(final SubscriptionNotification subscriptionNotification) {
        if (log.isDebugEnabled()) {
            log.debug("Handling cluster gossip: Notifying subscribers on Subscription changes ");
        }
        Runnable r = new Runnable() {
            @Override
            public void run() {
                /**
                 *TODO:Currently only the listener implemented for QPID (VirtualHostConfigSynchronizer) is available.
                 * Other listeners should be implemented for MQTT etc.
                 */
                for (SubscriptionListener listener : subscriptionListeners) {
                    try {
                        listener.subscriptionsChanged(subscriptionNotification);
                    } catch (Exception e) {
                        log.error("Error handling the subscription change ", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        // Here we do not want to block the Thread which invoked this method.
        // Since Subscription Listener may take long time time to handle the event
        AndesExecuter.runAsync(r);
    }

    /**
     * This method get called when a subscription which is in current node has changed.
     * If clustering is enabled, cluster wide subscription changed notification will be sent.
     * Otherwise, local listeners will be notified to handle subscription change.
     *
     * @param subscriptionNotification contains the information about the changed subscription
     */
    @Override
    public void handleLocalSubscriptionChange(SubscriptionNotification subscriptionNotification) {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            // Notify global listeners
            hazelcastAgent.notifySubscriberChanged(subscriptionNotification);
        } else {
            //notify local listeners
            notifySubscriptionChange(subscriptionNotification);
        }
    }

    /**
     * This method get called when the cluster wide notification about subscription change is received.
     *
     * @param subscriptionNotification contains the information about the changed subscription,
     */
    @Override
    public void handleClusterSubscriptionChange(SubscriptionNotification subscriptionNotification) {
        notifySubscriptionChange(subscriptionNotification);
    }

    /**
     * Register listeners which needs to be triggered when a subscription is changed.
     *
     * @param listener Subscription Listener implementation which will handle the subscription changes
     */
    @Override
    public void registerSubscriptionListener(SubscriptionListener listener) {
        if (listener == null) {
            throw new RuntimeException("Error while registering subscribers : invalid argument listener = null");
        }

        this.subscriptionListeners.add(listener);
    }

    /**
     * Unregister a listener.
     *
     * @param listener which should be unregistered.
     */
    @Override
    public void removeSubscriptionListener(SubscriptionListener listener) {
        if (this.subscriptionListeners.contains(listener)) {
            this.subscriptionListeners.remove(listener);
        }
    }
}
