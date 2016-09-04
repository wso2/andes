/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.server.cluster.error.detection.HazelcastBasedNetworkPartitionDetector;
import org.wso2.andes.server.cluster.error.detection.NetworkPartitionDetector;

import com.hazelcast.core.LifecycleEvent.LifecycleState;

/**
 * Hazelcast Lifecycle events are monitored through this listener. MB state and Hazelcast data structures are updated
 * appropriately depending on the {LifecycleEvent}
 */
public class HazelcastLifecycleListener implements LifecycleListener {

    private static Log log = LogFactory.getLog(HazelcastLifecycleListener.class);

    /**
     * {@link NetworkPartitionDetector} is required to know about clusters being merged.
     */
    private HazelcastBasedNetworkPartitionDetector networkPartitionDetector;
    
    /**
     * the constructor
     * 
     * @param networkPartitionDetector
     *            an implementation of how the network partition hare being
     *            detected.
     */
    public HazelcastLifecycleListener(HazelcastBasedNetworkPartitionDetector networkPartitionDetector) {
		this.networkPartitionDetector = networkPartitionDetector;
	}
    
    
    /**
     * On {@link com.hazelcast.core.LifecycleEvent.LifecycleState} MERGED event all the topic listeners for the local node is added back. Since the data structures except for
     * IMaps are not merged after a split brain scenario within Hazelcast (data structures from MERGED nodes are
     * discarded)
     * @param lifecycleEvent {@link LifecycleEvent}
     */
    @Override
    public void stateChanged(LifecycleEvent lifecycleEvent) {
        try {
            log.info("Hazelcast instance lifecycle changed state to " + lifecycleEvent.getState());
            if (lifecycleEvent.getState() == LifecycleEvent.LifecycleState.MERGED) {
                log.info("Hazelcast cluster merge detected after a split brain. Updating unmerged data structures");
                AndesContext.getInstance().getClusterNotificationListenerManager().reInitializeListener();
                AndesSubscriptionManager andesSubscriptionManager = AndesContext.getInstance()
                        .getAndesSubscriptionManager();
                if(null != andesSubscriptionManager) {
                    andesSubscriptionManager.updateSubscriptionsAfterClusterMerge();
                } else {
                    log.error("Andes Subscription Manager is not set. Local subscriptions are not synced with the " +
                            "main cluster");
                }
                
                // Notify that network partition has occurred.
                networkPartitionDetector.networkPartitionMerged();

            } else if (lifecycleEvent.getState() == LifecycleState.SHUTDOWN){
                networkPartitionDetector.clusterOutageOccurred();
            }


        } catch (Throwable e) {
            log.error("Error occurred while handling Hazelcast state change event " + lifecycleEvent.getState(), e);
        }
    }
}
