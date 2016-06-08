package org.wso2.andes.kernel.management.mbeans;


import org.wso2.andes.kernel.ProtocolType;

import java.util.List;
import java.util.Set;

/**
 * Exposes the Cluster Management related information
 */
public interface BrokerManagementInformationMXBean {

    /**
     * Gets the supported protocols of the broker.
     *
     * @return A set of protocols.
     */
    Set<ProtocolType> getSupportedProtocols();

    /**
     * Checks if clustering is enabled
     *
     * @return whether clustering is enabled
     */
    boolean isClusteringEnabled();

    /**
     * Gets node ID assigned for the node
     *
     * @return node ID
     */
    String getMyNodeID();

    /**
     * Gets the coordinator node's address
     *
     * @return Address of the coordinator node
     */
    String getCoordinatorNodeAddress();

    /**
     * Gets all the address of the nodes in a cluster
     *
     * @return A list of address of the nodes in a cluster
     */
    List<String> getAllClusterNodeAddresses();

    /**
     * Gets the message store's health status
     *
     * @return true if healthy, else false.
     */
    boolean getStoreHealth();
}
