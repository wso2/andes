package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;

import java.util.List;

/**
 * <code>ClusterManagementInformation</code>
 * Exposes the Cluster Management related information
 */
public interface ClusterManagementInformation {

    /**
     * MBean type name
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
