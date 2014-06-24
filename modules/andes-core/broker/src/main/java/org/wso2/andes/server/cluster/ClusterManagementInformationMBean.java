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
package org.wso2.andes.server.cluster;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.management.common.mbeans.ClusterManagementInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanConstructor;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;
import org.wso2.andes.server.management.AMQManagedObject;

import javax.management.JMException;

import java.util.List;


/**
 * <code>ClusterManagementInformationMBean</code> The the JMS MBean that expose cluster management information
 */
public class ClusterManagementInformationMBean extends AMQManagedObject implements ClusterManagementInformation {

    private ClusterManager clusterManager;

    @MBeanConstructor("Creates an MBean exposing an Cluster Manager")
    public ClusterManagementInformationMBean(ClusterManager clusterManager) throws JMException {
        super(ClusterManagementInformation.class , ClusterManagementInformation.TYPE);
        this.clusterManager = clusterManager;
    }

    public String getZkServer() {
        return clusterManager.getZkConnectionString();
    }


    public String getObjectInstanceName() {
        return ClusterManagementInformation.TYPE;
    }

    public String[] getGlobalQueuesAssigned(int nodeId) {
        return clusterManager.getGlobalQueuesAssigned(nodeId);
    }
    
    public boolean updateWorkerForQueue(String queueToBeMoved,String newNodeToAssign) {
        return clusterManager.updateWorkerForQueue(queueToBeMoved,newNodeToAssign);
    }

    public boolean isClusteringEnabled() {
        return clusterManager.isClusteringEnabled();
    }

    public String getMyNodeID() {
        return clusterManager.getMyNodeID();
    }

    public List<Integer> getZkNodes() {
        return clusterManager.getZkNodes();
    }

    public int getMessageCount(@MBeanOperationParameter(name = "queueName", description = "Name of the queue which message count is required") String queueName) {
        int count = 0;
        try{
            count = clusterManager.numberOfMessagesInGlobalQueue(queueName);
        } catch (AndesException e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    public String getIPAddressForNode(@MBeanOperationParameter(name = "nodeID", description = "Zookeeper ID of the node") int ZKID) {
        return clusterManager.getNodeAddress(ZKID);
    }

    public List<String> getDestinationQueuesOfCluster() {
        return clusterManager.getDestinationQueuesInCluster();
    }

    public int getNodeQueueMessageCount(int zkId, String destinationQueue) {
        try {
        return clusterManager.getNodeQueueMessageCount(zkId, destinationQueue);
        } catch (AndesException e) {
            throw new RuntimeException(e);
        }
    }

    public int getNodeQueueSubscriberCount(int zkId, String destinationQueue) {
    	throw new UnsupportedOperationException();
    }

    public List<String> getTopics() {
        List<String> topics = null;
        try {
            topics = clusterManager.getTopics();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return topics;
    }

    public List<String> getSubscribers(String topic){
    	throw new UnsupportedOperationException("Check what this should return (subscription IDs?)");
    }

    public int getSubscriberCount(@MBeanOperationParameter(name = "Topic", description = "Topic name") String topic) {
    	throw new UnsupportedOperationException("Check what this should return (subscription IDs?)");

    }

    public String getNodeAddress(@MBeanOperationParameter(name="Node Id",description =
            "Node id assigned by Cluster manager")
                                 int nodeId) {
        return clusterManager.getNodeAddress(nodeId);
    }
}
