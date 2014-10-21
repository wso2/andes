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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.andes.server.cluster;

import org.wso2.andes.kernel.*;
import org.wso2.andes.management.common.mbeans.ClusterManagementInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanConstructor;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;
import org.wso2.andes.server.management.AMQManagedObject;

import javax.management.JMException;

import java.util.ArrayList;
import java.util.List;


/**
 * <code>ClusterManagementInformationMBean</code> The the JMS MBean that expose cluster management information
 */
public class ClusterManagementInformationMBean extends AMQManagedObject implements ClusterManagementInformation {

    /**
     * ClusterManager instance to get the information to expose
     */
    private ClusterManager clusterManager;

    /**
     * Public MBean Constructor.
     *
     * @param clusterManager holds the information which should be exposed
     * @throws JMException
     */
    @MBeanConstructor("Creates an MBean exposing an Cluster Manager")
    public ClusterManagementInformationMBean(ClusterManager clusterManager) throws JMException {
        super(ClusterManagementInformation.class, ClusterManagementInformation.TYPE);
        this.clusterManager = clusterManager;
    }

    /**
     * Get the class type.
     *
     * @return class type as a String
     */
    public String getObjectInstanceName() {
        return ClusterManagementInformation.TYPE;
    }

    /**
     * Get the array of global queues assigned for a given node.
     *
     * @param nodeId to represent the node
     * @return array of global queue names
     */
    public String[] getGlobalQueuesAssigned(String nodeId) {
        return clusterManager.getGlobalQueuesAssigned(nodeId);
    }

    /**
     * Move a queue from current node to another node.
     *
     * @param queueToBeMoved  name of the queue to be moved
     * @param newNodeToAssign node ID of the new node
     * @return true if the queue is successfully moved
     */
    public boolean updateWorkerForQueue(String queueToBeMoved, String newNodeToAssign) {
        return clusterManager.updateWorkerForQueue(queueToBeMoved, newNodeToAssign);
    }

    /**
     * Check whether clustering is enabled
     *
     * @return true if clustering is enabled
     */
    public boolean isClusteringEnabled() {
        return AndesContext.getInstance().isClusteringEnabled();
    }

    /**
     * Get the node ID of the local node
     *
     * @return the node ID of the local node
     */
    public String getMyNodeID() {
        return clusterManager.getMyNodeID();
    }

    /**
     * Get the message count of a given queue
     *
     * @param queueName name of the queue
     * @return message count
     */
    public int getMessageCount(@MBeanOperationParameter(name = "queueName", description = "Name of the queue which message count is required") String queueName) {
        int count;
        try {
            count = MessagingEngine.getInstance().getMessageCountOfQueue(queueName);
        } catch (AndesException e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    /**
     * Get the IP of a given node
     *
     * @param nodeId ID of the node
     * @return the IP of the node as a string
     */
    public String getIPAddressForNode(@MBeanOperationParameter(name = "nodeID", description = "Node ID") String nodeId) {
        try {
            return clusterManager.getNodeAddress(nodeId);
        } catch (AndesException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a list of queues in cluster
     *
     * @return list of queue names
     */
    public List<String> getDestinationQueuesOfCluster() {
        List<String> queueList = new ArrayList<String>();
        try {
            for (AndesQueue queue : AndesContext.getInstance().getAMQPConstructStore().getQueues()) {
                queueList.add(queue.queueName);
            }
        } catch (AndesException e) {
            throw new RuntimeException(e);
        }
        return queueList;
    }

    @Override
    public int getNodeQueueMessageCount(@MBeanOperationParameter(name = "nodeId",
                                                                 description = "node id") String
                                                    nodeId,
                                        @MBeanOperationParameter(name = "destinationQueue",
                                                                 description = "destination queue" +
                                                                                      " name") String destinationQueue) {
        return 0;
    }

    public int getNodeQueueSubscriberCount(String nodeId, String destinationQueue) {
        //TODO:Should be implemented.
        throw new UnsupportedOperationException();
    }

    /**
     * Get all topics
     *
     * @return list of topics
     */
    public List<String> getTopics() {
        List<String> topics;
        try {
            topics = AndesContext.getInstance().getSubscriptionStore().getTopics();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return topics;
    }

    public List<String> getSubscribers(String topic) {
        throw new UnsupportedOperationException("Check what this should return (subscription IDs?)");
    }

    public int getSubscriberCount(@MBeanOperationParameter(name = "Topic", description = "Topic name") String topic) {
        throw new UnsupportedOperationException("Check what this should return (subscription IDs?)");

    }
}
