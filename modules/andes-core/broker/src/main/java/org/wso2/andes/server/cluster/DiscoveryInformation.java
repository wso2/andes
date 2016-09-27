/*
* Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.andes.server.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationAgent;
import org.wso2.andes.server.cluster.coordination.CoordinationComponentFactory;

import java.util.List;


public class DiscoveryInformation {

    /**
     * Transport data object list.
     */
    public static List<TransportData> transportDataList;
    /**
     * Class logger
     */
    protected final Logger logger = LoggerFactory.getLogger(DiscoveryInformation.class);

    /**
     * Creating Coordination Component Factory.
     */
    private CoordinationComponentFactory coordinationComponentFactory;

    /**
     * This constructor.
     */
    public DiscoveryInformation() {
        this.coordinationComponentFactory = new CoordinationComponentFactory();
    }

    /**
     * Getter for transport data list.
     *
     * @return transport data object list (node identifier,IP addresses, AMQP port, AMQP SSL port).
     */
    public static List<TransportData> getTransportDataList() {

        return transportDataList;
    }

    /**
     * Setter for transport data list.
     *
     * @param transportData contains transport data objects.
     */
    public static void setTransportDataList(List<TransportData> transportData) {

        transportDataList = transportData;
    }

    /**
     * Get Discovery Information (IP list, AMQP port, AMQP SSL port and Node Identifier).
     */
    public void getDiscoveryInformation() throws AndesException {

        if (AndesContext.getInstance().isClusteringEnabled()) {

            IpAddressRetriever ipAddressRetriever = AndesContext.getInstance().getAddressRetriever();
            AndesContextStore andesContextStore = AndesContext.getInstance().getAndesContextStore();
            ClusterAgent clusterAgent = AndesContext.getInstance().getClusterAgent();
            ClusterNotificationAgent clusterNotificationAgent = coordinationComponentFactory.createClusterNotificationAgent();
            //noinspection ConstantConditions
            int amqpPort = AndesConfigurationManager.readValue(AndesConfiguration.TRANSPORTS_AMQP_DEFAULT_CONNECTION_PORT);
            //noinspection ConstantConditions
            int amqpSslPort = AndesConfigurationManager.readValue(AndesConfiguration.TRANSPORTS_AMQP_SSL_CONNECTION_PORT);
            String nodeIdentifier = clusterAgent.getLocalNodeIdentifier();
            //add amqp host and the port
            andesContextStore.addAmqpAddress(nodeIdentifier, amqpPort, amqpSslPort);

            String configuredAdpAddress = AndesConfigurationManager.readValue(AndesConfiguration
                    .TRANSPORTS_AMQP_BIND_ADDRESS);

            if (configuredAdpAddress.equals("0.0.0.0")) {

                List<MultipleAddressDetails> addressDetailsList = ipAddressRetriever.getLocalAddress();
                for (MultipleAddressDetails anAddressDetailsList : addressDetailsList) {
                    andesContextStore.addAddressDetails(nodeIdentifier, anAddressDetailsList.getInterfaceDetail(),
                            anAddressDetailsList.getIpAddress());
                }
            } else {
                andesContextStore.addAddressDetails(nodeIdentifier, "configure", configuredAdpAddress);
            }
            DiscoveryInformation.setTransportDataList(andesContextStore.getAllTransportDetails());
            clusterNotificationAgent.publishDyanamicDiscovery();
        }
    }

    /**
     * Add transport data to list and send hazelcast notification
     *
     * @throws AndesException
     */
    void addTransportData(String addedNodeId) throws AndesException {

        AndesContextStore andesContextStore = AndesContext.getInstance().getAndesContextStore();
        List<TransportData> checkListTransportdata = andesContextStore.getAllTransportDetails();

        for (TransportData aCheckListTransportdata : checkListTransportdata) {
            if (aCheckListTransportdata.getNodeIdentifier().equals(addedNodeId)) {
                transportDataList.add(aCheckListTransportdata);
            }

        }

    }

    /**
     * Update Transport data list.
     *
     * @param deletedNodeId Node Identifier.
     */
    public void updateTransportData(String deletedNodeId) {

        for (int listIncreaser = 0; listIncreaser < transportDataList.size(); ) {

            if (transportDataList.get(listIncreaser).getNodeIdentifier().equals(deletedNodeId)) {
                transportDataList.remove(listIncreaser);
                listIncreaser++;
            } else {
                listIncreaser++;
            }
        }
    }

    /**
     * Remove Node details form all the list and database.
     *
     * @param nodeId Node identifier.
     * @throws AndesException
     */
    public void deleteTransportData(String nodeId) throws AndesException {

        if (AndesContext.getInstance().isClusteringEnabled()) {

            for (int listIncreaser = 0; listIncreaser < transportDataList.size(); ) {
                if (transportDataList.get(listIncreaser).getNodeIdentifier().equals(nodeId)) {
                    transportDataList.remove(listIncreaser);
                    listIncreaser++;
                } else {
                    listIncreaser++;
                }
            }
            //Remove node details when shutdown
            clearAmqpHostAddress(nodeId);
        }
    }

    /**
     * Clear AMQP details when node is going to shutdown.
     *
     * @param nodeID Node identifier
     * @throws AndesException
     */
    private void clearAmqpHostAddress(String nodeID) throws AndesException {
        logger.info("Clearing the Node with ID " + nodeID);
        AndesContextStore andesContextStore = AndesContext.getInstance().getAndesContextStore();
        //remove node from nodes list
        andesContextStore.removeAmqpAddress(nodeID);
    }
}

