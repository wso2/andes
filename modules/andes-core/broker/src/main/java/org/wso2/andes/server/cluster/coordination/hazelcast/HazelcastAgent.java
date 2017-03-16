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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.core.ITopic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.server.cluster.CoordinationConfigurableClusterAgent;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;

/**
 * This is a singleton class, which contains all Hazelcast related operations.
 */
public class HazelcastAgent {

    private static final Log log = LogFactory.getLog(HazelcastAgent.class);

    /**
     * Value used to indicate the cluster initialization success state
     */
    private static final long INIT_SUCCESSFUL = 1L;

    /**
     * Singleton HazelcastAgent Instance.
     */
    private static HazelcastAgent hazelcastAgentInstance = new HazelcastAgent();

    /**
     * Hazelcast instance exposed by Carbon.
     */
    private HazelcastInstance hazelcastInstance;

    /**
     * Lock used to initialize the Slot map used by the Slot manager.
     */
    private ILock initializationLock;

    /**
     * This is used to indicate if the cluster initialization was done properly. Used a atomic long
     * since am atomic boolean is not available in the current Hazelcast implementation.
     */
    private IAtomicLong initializationDoneIndicator;


    /**
     * Private constructor.
     */
    private HazelcastAgent() {

    }

    /**
     * Get singleton HazelcastAgent.
     *
     * @return HazelcastAgent
     */
    public static synchronized HazelcastAgent getInstance() {
        return hazelcastAgentInstance;
    }

    /**
     * Initialize HazelcastAgent instance.
     *
     * @param hazelcastInstance obtained hazelcastInstance from the OSGI service
     */
    @SuppressWarnings("unchecked")
    public void init(HazelcastInstance hazelcastInstance) {
        log.info("Initializing Hazelcast Agent");
        this.hazelcastInstance = hazelcastInstance;

        // Set cluster agent in Andes Context
        CoordinationConfigurableClusterAgent clusterAgent = new CoordinationConfigurableClusterAgent(hazelcastInstance);
        AndesContext.getInstance().setClusterAgent(clusterAgent);

        /*
          Initialize distributed lock and boolean related to slot map initialization
         */
        initializationLock = hazelcastInstance.getLock(CoordinationConstants.INITIALIZATION_LOCK);
        initializationDoneIndicator = hazelcastInstance
                .getAtomicLong(CoordinationConstants.INITIALIZATION_DONE_INDICATOR);

        log.info("Successfully initialized Hazelcast Agent");
    }


    /**
     * Create a reliable topic and configure it inside Hazelcast
     *
     * @param topicName        the name of the reliable topic. The configuration will be stored under the same name
     * @param enableStatistics whether to enable statistics
     * @param readBatchSize    he maximum number of items that will be read at a single try. If the number of items
     *                         that are present is less than the readBatchSize, the available items will be read
     * @param capacity         the size of the ring buffer. Defines the number of messages that will be stored
     * @param timeToLive       the time it takes for a message published to a reliable topic to expire
     * @return Hazelcast topic created
     */
    ITopic<ClusterNotification> createReliableTopic(String topicName,
            boolean enableStatistics,
            int readBatchSize,
            int capacity,
            int timeToLive) {
        addReliableTopicConfig(topicName, enableStatistics, readBatchSize);
        addRingBufferConfig(topicName, capacity, timeToLive);
        return hazelcastInstance.getReliableTopic(topicName);
    }

    /**
     * Acquire the distributed lock related to cluster initialization. This lock is required to
     * avoid two nodes initializing the map twice.
     */
    public void acquireInitializationLock() {
        if (log.isDebugEnabled()) {
            log.debug("Trying to acquire initialization lock.");
        }

        initializationLock.lock();

        if (log.isDebugEnabled()) {
            log.debug("Initialization lock acquired.");
        }
    }

    /**
     * Inform other members in the cluster that the cluster was initialized properly.
     */
    public void indicateSuccessfulInitilization() {
        initializationDoneIndicator.set(INIT_SUCCESSFUL);
    }

    /**
     * Check if a member has already initialized the cluster
     *
     * @return true if cluster is already initialized
     */
    public boolean isClusterInitializedSuccessfully() {
        return initializationDoneIndicator.get() == INIT_SUCCESSFUL;
    }

    /**
     * Release the initialization lock.
     */
    public void releaseInitializationLock() {
        initializationLock.unlock();

        if (log.isDebugEnabled()) {
            log.debug("Initialization lock released.");
        }
    }

    /**
     * Method to check if the hazelcast instance has shutdown.
     * @return boolean
     */
    public boolean isActive() {
        if (null != hazelcastInstance) {
            return hazelcastInstance.getLifecycleService().isRunning();
        } else {
            return false;
        }
    }

    /**
     * Method to configure a given reliable topic of the Hazelcast instance. Refer to Hazelcast Reliable
     * Topic Configurations for more information on the configuration parameters
     *
     * @param reliableTopicName the name of the reliable topic. The configuration will be stored under the same name
     * @param enableStatistics  whether to enable statistics
     * @param readBatchSize     the maximum number of items that will be read at a single try. If the number of items
     *                          that are present is less than the readBatchSize, the available items will be read
     */
    private void addReliableTopicConfig(String reliableTopicName, boolean enableStatistics,
                                        int readBatchSize) {

        Config config = hazelcastInstance.getConfig();

        // Create a new Hazelcast reliable topic configuration with the given values
        ReliableTopicConfig topicConfig = new ReliableTopicConfig(reliableTopicName);
        topicConfig.setStatisticsEnabled(enableStatistics);
        topicConfig.setReadBatchSize(readBatchSize);

        // Add the current reliable topic configuration to the configurations of the Hazelcast instance
        config.addReliableTopicConfig(topicConfig);
    }

    /**
     * Method to configure a given ring buffer of the Hazelcast instance. Refer to Hazelcast Ring Buffer
     * configurations for more information on the configuration parameters
     *
     * @param ringBufferName the name from which the ring buffer should be created.
     *                       Same as the associated reliable topic name
     * @param capacity       the size of the ring buffer. Defines the number of messages that will be stored
     * @param timeToLive     the time it takes for a message published to a reliable topic to expire
     */
    private void addRingBufferConfig(String ringBufferName, int capacity, int timeToLive) {
        Config config = hazelcastInstance.getConfig();

        // Create a new ring buffer configuration with the given name and values
        RingbufferConfig ringConfig = new RingbufferConfig(ringBufferName);
        ringConfig.setCapacity(capacity);
        ringConfig.setTimeToLiveSeconds(timeToLive);

        // Add the current ring buffer configuration to the configurations of the Hazelcast instance
        config.addRingBufferConfig(ringConfig);
    }
}
