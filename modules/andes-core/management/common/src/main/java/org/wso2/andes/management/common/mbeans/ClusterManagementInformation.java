package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;

import java.util.*;


/**
 * <code>ClusterManagementInformation</code>
 * Exposes the Cluster Management related information
 */
public interface ClusterManagementInformation {

    static final String TYPE = "ClusterManagementInformation";

     //Individual attribute name constants
    String ATTR_NODE_ID = "nodeId" ;
    String ATTR_ADDRESS = "Address";
    String ATTR_PORT = "Port";

     //All attribute names constant
    static final List<String> CLUSTER_ATTRIBUTES
            = Collections.unmodifiableList(
             new ArrayList<String>(
                     new HashSet<String>(
                             Arrays.asList(
                                     ATTR_NODE_ID,
                                     ATTR_ADDRESS,
                                     ATTR_PORT))));

    @MBeanAttribute(name = "isClusteringEnabled", description = "is in clustering mode")
    boolean isClusteringEnabled();

    @MBeanAttribute(name = "getMyNodeID", description = "Zookeeper Node Id assigned for the node")
    String getMyNodeID();

    @MBeanAttribute(name = "getGlobalQueuesAssigned", description = "Existing global queues currently assigned to the node")
    String[] getGlobalQueuesAssigned(String nodeId);

    @MBeanAttribute(name = "updateWorkerForQueue", description = "Move the given global queue Worker Handler to a new node")
    boolean updateWorkerForQueue(@MBeanOperationParameter(name="queueToMove",description = "name of queue whose queue worker to move") String queueToBeMoved,
                                 @MBeanOperationParameter(name="newNode",description = "name of new node to assign queue worker") String newNodeToAssign);

    @MBeanAttribute(name = "getMessageCount" , description = "Message Count in the queue")
    int getMessageCount(@MBeanOperationParameter(name = "queueName",description = "Name of the queue which message count is required")String queueName);

    @MBeanAttribute(name = "getTopics" ,description = "Topics where subscribers are available")
    List<String> getTopics();

    @MBeanAttribute(name = "getSubscriberCount",description = "Subscribers for a given topic")
    List<String> getSubscribers(@MBeanOperationParameter(name="Topic",description = "Topic name") String topic);

    @MBeanAttribute(name = "getSubscriberCount",description = "Number of subscribers for a given topic")
    int getSubscriberCount(@MBeanOperationParameter(name="Topic",description = "Topic name") String topic);

    @MBeanAttribute(name = "getIPAddressForNode" , description = "get IP address for given node")
    String getIPAddressForNode(@MBeanOperationParameter(name = "nodeID", description = "Node ID") String nodeId);

    @MBeanAttribute(name = "getDestinationQueuesOfCluster", description = "get destination queues in cluster")
    List<String> getDestinationQueuesOfCluster();

    @MBeanAttribute(name = "getNodeQueueMessageCount", description = "get message count of node addressed to given destination queue")
    int getNodeQueueMessageCount(@MBeanOperationParameter(name = "nodeId", description = "node id") String nodeId,
                                 @MBeanOperationParameter(name = "destinationQueue" , description = "destination queue name") String destinationQueue);

    @MBeanAttribute(name = "getNodeQueueSubscriberCount", description = "get subscriber count of node subscribed to given destination queue")
    int getNodeQueueSubscriberCount(@MBeanOperationParameter(name = "nodeId", description = "node id") String nodeId,
                                 @MBeanOperationParameter(name = "destinationQueue" , description = "destination queue name") String destinationQueue);

}
