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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.server.cluster.error.detection;

import com.hazelcast.core.HazelcastInstance;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Detects network partitions (and minimum node count is not being in the
 * cluster) based on hazelcast member joined/left, cluster merged events
 */
public class HazelcastBasedNetworkPartitionDetector implements NetworkPartitionDetector {

    /**
     * log for this class
     */
    private Log log = LogFactory.getLog(HazelcastBasedNetworkPartitionDetector.class);

    /**
     * Keeps track of entities who are interested in network-partitions related
     * events.
     */
    private SortedMap<Integer, NetworkPartitionListener> networkPartitionListeners
                            = Collections.synchronizedSortedMap(new TreeMap<Integer, NetworkPartitionListener>());

    /**
     * Minimum number of nodes in the cluster ( or in a particular network
     * partition). value is configured in broker.xml
     */
    private int minimumClusterSize;

    /**
     * Reference to hazelcast instance
     */
    private HazelcastInstance hazelcastInstance;

    /**
     * a flag keeps track of network is currently partitioned ( cluster size <
     * minimum node count) or not.
     */
    private boolean isNetworkPartitioned;

    
    /**
     * The constructor 
     * @param hazelcastInstance hazelcast instance
     */
    public HazelcastBasedNetworkPartitionDetector(HazelcastInstance hazelcastInstance) {
        this.minimumClusterSize =
            AndesConfigurationManager.readValue(AndesConfiguration.RECOVERY_NETWORK_PARTITIONS_MINIMUM_CLUSTER_SIZE);
        this.hazelcastInstance = hazelcastInstance;
        this.isNetworkPartitioned = false;
    }

    /**
     * Detects if the network is partition or not based on,
     * <ul>
     * <li>Type of hazelcast events and the order of which they happened</li>
     * <li>current size of the hazelcast cluster ( node count)</li>
     * </ul>
     *
     * @param eventType
     *            Type of network partition even
     * @param clusterSize The number of members in the cluster.
     */
    private synchronized void detectNetworkPartitions(PartitionEventType eventType, int clusterSize) {

        log.info("Network partition event received: " + eventType +  " current cluster size: " + clusterSize);

        if (eventType == PartitionEventType.START_UP) {
            
           if (clusterSize < minimumClusterSize) {
               this.isNetworkPartitioned = true; 
               minimumNodeCountNotFulfilled(clusterSize);
                
            } else {
               minimumNodeCountFulfilled(clusterSize);
            }

        } else if ((isNetworkPartitioned == false) && (clusterSize < minimumClusterSize)) {

            log.info("Current cluster size has reduced below minimum cluster size, current cluster size: "
                                                                                                        + clusterSize);
            
            this.isNetworkPartitioned = true;
            
            minimumNodeCountNotFulfilled(clusterSize);

        } else if ((isNetworkPartitioned == true) && (clusterSize >= minimumClusterSize)) {

            log.info("Current cluster size satisfies minimum required. current cluster size: " + clusterSize);

            this.isNetworkPartitioned = false;
            
            minimumNodeCountFulfilled(clusterSize);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: Method is synchronized to avoid a rare situation where a
     * {@link NetworkPartitionListener} being added (during a start up) and
     * Simultaneously a hazelcast member left/join event being fired.
     * <p>
     */
    @Override
    public synchronized void addNetworkPartitionListener(int priority, NetworkPartitionListener listener) {
        NetworkPartitionListener previous = networkPartitionListeners.put(priority, listener);

        // Throwing a RuntimeException when the same priority level is used twice. No need to replace the previous
        // value in the map since server is unusable after this exception thrown. Exception type is chosen as
        // RuntimeException since this is an error from the developer.
        if (null != previous) {
            throw new IllegalArgumentException("Priority value is already used. Please use a different priority level"
                                               + " for " + listener);
        }

        if (isNetworkPartitioned) {
            log.warn("network partition listener added while in cluster nodes doesn't meet minimum node count: " +
                     minimumClusterSize + " listener : " + listener.toString());

            listener.minimumNodeCountNotFulfilled(-1);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        detectNetworkPartitions(PartitionEventType.START_UP, hazelcastInstance.getCluster().getMembers().size());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void memberAdded(Object member, int clusterSize) {
        detectNetworkPartitions(PartitionEventType.MEMBER_ADDED, clusterSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void memberRemoved(Object member, int clusterSize) {
        detectNetworkPartitions(PartitionEventType.MEMBER_REMOVED, clusterSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void networkPartitionMerged() {
        detectNetworkPartitions(PartitionEventType.CLUSTER_MERGED, hazelcastInstance.getCluster().getMembers().size());
    }

    /**
     * Notifying listeners that minimum number of node count is not fulfilled.
     *
     * @param currentClusterSize The size of the cluster(The number of members).
     */
    private void minimumNodeCountNotFulfilled(int currentClusterSize) {
        for (NetworkPartitionListener listener : networkPartitionListeners.values()) {
            listener.minimumNodeCountNotFulfilled(currentClusterSize);
        }
    }

    /**
     * Notifying listeners that minimum number of node count is fulfilled.
     *
     * @param currentClusterSize The size of the cluster(The number of members).
     */
    private void minimumNodeCountFulfilled(int currentClusterSize) {
        for (NetworkPartitionListener listener : networkPartitionListeners.values()) {
            listener.minimumNodeCountFulfilled(currentClusterSize);
        }
    }

    /**
     * Convenient enum indicating possible network event types that occurs.
     */
    private enum PartitionEventType {
        /**
         * Indicates start of detection ( - usual the server start up)
         */
        START_UP,
        /**
         * Indicates a member/node being added to cluster
         */
        MEMBER_ADDED,
        /**
         * Indicates a member/node being removed from cluster.
         */
        MEMBER_REMOVED,
        
        /**
         * Indicates a cluster merged network partitions and recovered from a split brain.
         */
        CLUSTER_MERGED
    }
    
}
