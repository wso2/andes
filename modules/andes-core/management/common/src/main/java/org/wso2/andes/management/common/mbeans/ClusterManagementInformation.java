package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;

import javax.management.JMException;
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
     * Gets all the address of the nodes in a cluster
     *
     * @return A list of address of the nodes in a cluster
     */
    @MBeanAttribute(name = "getAllClusterNodeAddresses", description = "Gets the addresses of the members in a cluster")
    List<String> getAllClusterNodeAddresses() throws JMException;

    /**
     * Gets the message store's health status
     *
     * @return true if healthy, else false.
     */
    @MBeanAttribute(name = "getStoreHealth", description = "Gets the message stores health status")
    boolean getStoreHealth();

    /**
     * Get the all live node's IP address and port which is bound to AMQP
     * @return Transport data objects which contains IPs and the Ports
     * @throws AndesException
     */
    @MBeanAttribute(name = "getBrokerDetail", description = "Gets the all live nodes")
   String getBrokerDetail() throws JMException;

}