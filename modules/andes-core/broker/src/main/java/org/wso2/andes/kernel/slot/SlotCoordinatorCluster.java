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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.slot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.server.cluster.error.detection.NetworkPartitionListener;
import org.wso2.andes.thrift.MBThriftClient;

/**
 * This class is responsible of coordinating with the cluster mode Slot Manager
 */
public class SlotCoordinatorCluster implements SlotCoordinator, NetworkPartitionListener {

    private static Log log = LogFactory.getLog(SlotCoordinatorCluster.class);
    private String nodeId;

    private volatile SlotCoordinator instance;

    public SlotCoordinatorCluster(){
        nodeId = AndesContext.getInstance().getClusterAgent().getLocalNodeIdentifier();
        instance = new ThriftSlotCoordinator();
        AndesContext.getInstance().getClusterAgent().addNetworkPartitionListener(this);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Slot getSlot(String queueName) throws ConnectionException {
        return instance.getSlot(queueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateMessageId(String queueName,
                                long startMessageId, long endMessageId, long localSafeZone) throws ConnectionException {
        instance.updateMessageId(queueName,startMessageId,endMessageId, localSafeZone);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSlotDeletionSafeZone(long currentSlotDeleteSafeZone) throws ConnectionException {
        instance.updateSlotDeletionSafeZone(currentSlotDeleteSafeZone);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteSlot(String queueName, Slot slot) throws ConnectionException {
        return instance.deleteSlot(queueName, slot);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reAssignSlotWhenNoSubscribers(String queueName) throws ConnectionException {
        instance.reAssignSlotWhenNoSubscribers(queueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearAllActiveSlotRelationsToQueue(String queueName) throws ConnectionException {
        instance.clearAllActiveSlotRelationsToQueue(queueName);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void minimumNodeCountNotFulfilled(int currentNodeCount) {
       
       // Do nothing as of now. 
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void minimumNodeCountFulfilled(int currentNodeCount) {
        // Do nothing as of now.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Disabled all communications with Coordinator code (so that this node will
     * not affect other nodes (or entire cluster's ) state.
     */
    @Override
    public void clusteringOutage() {
        if (instance instanceof ThriftSlotCoordinator) {
            log.info("disabling slot coordination due to cluster outage");
            instance = new DisabledSlotCoordinator();
        }
    }
   
    /**
     * Inner class to wire {@link SlotCoordinatorCluster} with
     * {@link MBThriftClient}
     */
    private class ThriftSlotCoordinator implements SlotCoordinator {

        @Override
        public Slot getSlot(String queueName) throws ConnectionException {
            return MBThriftClient.getSlot(queueName, nodeId);
        }

        @Override
        public void updateMessageId(String queueName, long startMessageId, long endMessageId,
                                    long localSafeZone) throws ConnectionException {
            MBThriftClient.updateMessageId(queueName,nodeId,startMessageId,endMessageId, localSafeZone); 
        }

        @Override
        public void updateSlotDeletionSafeZone(long currentSlotDeleteSafeZone) throws ConnectionException {
            MBThriftClient.updateSlotDeletionSafeZone(currentSlotDeleteSafeZone, nodeId);
            if(log.isDebugEnabled()) {
                log.debug("Submitted safe zone from node : " + nodeId + " | safe zone : " +
                        currentSlotDeleteSafeZone);
            }  
        }

        @Override
        public boolean deleteSlot(String queueName, Slot slot) throws ConnectionException {
            return MBThriftClient.deleteSlot(queueName, slot, nodeId);
        }

        @Override
        public void reAssignSlotWhenNoSubscribers(String queueName) throws ConnectionException {
            MBThriftClient.reAssignSlotWhenNoSubscribers(nodeId, queueName);
        }

        @Override
        public void clearAllActiveSlotRelationsToQueue(String queueName) throws ConnectionException {
            MBThriftClient.clearAllActiveSlotRelationsToQueue(queueName);
        }   
    }
    

    /**
     * Inner class to make all communications disabled with coordinator when
     * there is a cluster error.
     */
    private class DisabledSlotCoordinator implements SlotCoordinator {

        @Override
        public Slot getSlot(String queueName) throws ConnectionException {
            throw new ConnectionException("cluster error detected, not connectng to cooridnator");
        }

        @Override
        public void updateMessageId(String queueName, long startMessageId, long endMessageId,
                                    long localSafeZone) throws ConnectionException {
            throw new ConnectionException("cluster error detected, not connectng to cooridnator");            
        }

        @Override
        public void updateSlotDeletionSafeZone(long currentSlotDeleteSafeZone) throws ConnectionException {
            throw new ConnectionException("cluster error detected, not connectng to cooridnator");
        }

        @Override
        public boolean deleteSlot(String queueName, Slot slot) throws ConnectionException {
            throw new ConnectionException("cluster error detected, not connectng to cooridnator");
        }

        @Override
        public void reAssignSlotWhenNoSubscribers(String queueName) throws ConnectionException {
            throw new ConnectionException("cluster error detected, not connectng to cooridnator");
        }

        @Override
        public void clearAllActiveSlotRelationsToQueue(String queueName) throws ConnectionException {
            throw new ConnectionException("cluster error detected, not connectng to cooridnator");
        }
        
    }
    
}
