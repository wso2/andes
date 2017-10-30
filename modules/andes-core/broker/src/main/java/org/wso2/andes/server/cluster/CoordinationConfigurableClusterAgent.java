/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.server.cluster;

import com.hazelcast.core.HazelcastInstance;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.BrokerConfigurationService;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;

/**
 * Hazelcast based cluster agent implementation
 */
public class CoordinationConfigurableClusterAgent implements ClusterAgent {

    /**
     * Class logger
     */
    private static final Log log = LogFactory.getLog(CoordinationConfigurableClusterAgent.class);

    /**
     * Coordination algorithm used to elect the coordinator
     */
    private final CoordinationStrategy coordinationStrategy;

    /**
     * Unique id of local member used for message ID generation
     */
    private int uniqueIdOfLocalMember;

    /**
     * Cluster manager used to indicate membership change events
     */
    private ClusterManager manager;

    
    public CoordinationConfigurableClusterAgent(HazelcastInstance hazelcastInstance) {
        this();
    }

    public CoordinationConfigurableClusterAgent() {
        coordinationStrategy = new RDBMSCoordinationStrategy();
    }

    /**
     * Membership listener calls this method when a new node joins the cluster
     *
     * @param nodeId Node ID of the new member
     */
    public void memberAdded(String nodeId) {
        manager.memberAdded(nodeId);
    }

    /**
     * Membership listener calls this method when a node leaves the cluster
     *
     * @param nodeId Node ID of the member who left
     * @throws AndesException
     */
    public void memberRemoved(String nodeId) throws AndesException {
        manager.memberRemoved(nodeId);
    }

    public void becameCoordinator() {
        manager.localNodeElectedAsCoordinator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUniqueIdForLocalNode() {
        return uniqueIdOfLocalMember;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCoordinator() {
        return coordinationStrategy.isCoordinator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String start(ClusterManager manager) throws AndesException{
        this.manager = manager;

        String localNodeIdentifier = getLocalNodeIdentifier();

        // TODO: We need generate a node wise unique ID if more than one node is publishing messages
        // This to avoid message ID duplication.
        // Modulus is taken to make sure we get a 8 bit long id
        this.uniqueIdOfLocalMember = localNodeIdentifier.hashCode() % 256;
        if (log.isDebugEnabled()) {
            log.debug("Unique ID generation for message ID generation:" + uniqueIdOfLocalMember);
        }

        String thriftCoordinatorServerIP = AndesContext.getInstance().getThriftServerHost();
        int thriftCoordinatorServerPort = AndesContext.getInstance().getThriftServerPort();
        InetSocketAddress thriftAddress = new InetSocketAddress(thriftCoordinatorServerIP, thriftCoordinatorServerPort);

        coordinationStrategy.start(this, localNodeIdentifier, thriftAddress);

        log.info("Initializing Cluster Mode. Current Node ID:" + localNodeIdentifier);
        return localNodeIdentifier;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        coordinationStrategy.stop();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLocalNodeIdentifier() {
        String nodeId;

        // Get Node ID configured by user in broker.xml (if not "default" we must use it as the ID)
        nodeId = BrokerConfigurationService.getInstance().getBrokerConfiguration().getCoordination().getNodeID();

        // If the config value is "default" we must generate the ID. If we want to reuse the ID for same node we can
        // persist the entry locally in CARBON_HOME.
        if (nodeId.equals("default")) {
            nodeId = UUID.randomUUID().toString();
        }

        return nodeId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllNodeIdentifiers() throws AndesException {
        return coordinationStrategy.getAllNodeIdentifiers();
    }

    /**
     * Callback method for coordinator state lost event
     */
    public void coordinatorStateLost() {
        manager.coordinatorStateLost();
    }
}
