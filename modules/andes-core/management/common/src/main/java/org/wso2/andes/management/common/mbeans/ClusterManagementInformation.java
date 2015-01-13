package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;

import java.util.*;


/**
 * <code>ClusterManagementInformation</code>
 * Exposes the Cluster Management related information
 */
public interface ClusterManagementInformation {

    /**
     * MBean typ name
     */
    static final String TYPE = "ClusterManagementInformation";

    /**
     * Checks if clustering is enabled
     *
     * @return whether clustering is enabled
     */
    @MBeanAttribute(name = "isClusteringEnabled", description = "is in clustering mode")
    boolean isClusteringEnabled();

    /**
     * Gets node ID assigned for the node
     *
     * @return node ID
     */
    @MBeanAttribute(name = "getMyNodeID", description = "Node Id assigned for the node")
    String getMyNodeID();

    /**
     * Gets message Count in a queue
     *
     * @param queueName queue name
     * @return message count
     */
    @MBeanAttribute(name = "getMessageCount", description = "Message Count in the queue")
    int getMessageCount(
            @MBeanOperationParameter(name = "queueName", description = "Name of the queue which message count is required") String queueName);

    /**
     * List of topics where subscribers are available
     *
     * @return list of topic names
     */
    @MBeanAttribute(name = "getTopics", description = "Topics where subscribers are available")
    List<String> getTopics();

    /**
     * Gets subscribers for a given topic
     *
     * @param topic topic name
     * @return subscriber names
     */
    @MBeanAttribute(name = "getSubscriberCount", description = "Subscribers for a given topic")
    List<String> getSubscribers(
            @MBeanOperationParameter(name = "Topic", description = "Topic name") String topic);

    /**
     * Gets number of subscribers for a given topic
     *
     * @param topic topic name
     * @return subscriber count
     */
    @MBeanAttribute(name = "getSubscriberCount", description = "Number of subscribers for a given topic")
    int getSubscriberCount(
            @MBeanOperationParameter(name = "Topic", description = "Topic name") String topic);

    /**
     * Gets IP address for given node
     *
     * @param nodeId node ID
     * @return IP address
     */
    @MBeanAttribute(name = "getIPAddressForNode", description = "Get IP address for given node")
    String getIPAddressForNode(
            @MBeanOperationParameter(name = "nodeID", description = "Node ID") String nodeId);

    /**
     * Gets destination queues in cluster
     *
     * @return list of destination queues in a cluster
     */
    @MBeanAttribute(name = "getDestinationQueuesOfCluster", description = "Get destination queues in cluster")
    List<String> getDestinationQueuesOfCluster();

    /**
     * Gets message count of node addressed to given destination queue
     *
     * @param nodeId nodeID
     * @param destinationQueue destination queue name
     * @return message count
     */
    @Deprecated
    @MBeanAttribute(name = "getNodeQueueMessageCount", description = "Get message count of node addressed to given destination queue")
    int getNodeQueueMessageCount(
            @MBeanOperationParameter(name = "nodeId", description = "node id") String nodeId,
            @MBeanOperationParameter(name = "destinationQueue", description = "destination queue name") String destinationQueue);

    /**
     * Gets subscriber count of node subscribed to given destination queue
     *
     * @param nodeId node ID
     * @param destinationQueue  destination queue name
     * @return subscriber count
     */
    @Deprecated
    @MBeanAttribute(name = "getNodeQueueSubscriberCount", description = "Get subscriber count of node subscribed to given destination queue")
    int getNodeQueueSubscriberCount(
            @MBeanOperationParameter(name = "nodeId", description = "node id") String nodeId,
            @MBeanOperationParameter(name = "destinationQueue", description = "destination queue name") String destinationQueue);

    /**
     * Gets the coordinator node's address
     *
     * @return Address of the coordinator node
     */
    @MBeanAttribute(name = "getCoordinatorNodeAddress", description = "Gets the coordinator nodes address")
    String getCoordinatorNodeAddress();

    /**
     * Gets all the address of the nodes in a cluster
     *
     * @return A list of address of the nodes in a cluster
     */
    @MBeanAttribute(name = "getAllClusterNodeAddresses", description = "Gets the addresses of the members in a cluster")
    List<String> getAllClusterNodeAddresses();
}
