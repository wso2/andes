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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.management.common.mbeans.ClusterManagementInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanConstructor;
import org.wso2.andes.server.management.AMQManagedObject;

import javax.management.JMException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;

/**
 * <code>ClusterManagementInformationMBean</code> The the JMS MBean that expose cluster management information
 * Exposes the Cluster Management related information using MBeans
 */
public class ClusterManagementInformationMBean extends AMQManagedObject implements ClusterManagementInformation {

    /**
     * Class logger
     */
    private static final Log logger = LogFactory.getLog(ClusterManagementInformationMBean.class);

    /**
     * Use for separate IP address and interface name.
     */
    private static final String separator = "=";

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
     * {@inheritDoc}
     */
    @Override
    public String getObjectInstanceName() {
        return ClusterManagementInformation.TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClusteringEnabled() {
        return AndesContext.getInstance().isClusteringEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMyNodeID() {
        return clusterManager.getMyNodeID();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllClusterNodeAddresses() throws JMException {
        try {
            return this.clusterManager.getAllClusterNodeAddresses();
        } catch (AndesException e) {
            logger.error("Error occurred while retrieving cluster details", e);
            throw new JMException("Error occurred while retrieving cluster details");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getStoreHealth() {
        return this.clusterManager.getStoreHealth();
    }

    /**
     * {@inheritDoc}
     */
    public synchronized String getBrokerDetail() throws JMException {
        try {

            List<TransportData> transportDataList = DiscoveryInformation.getTransportDataList();
            Collections.rotate(transportDataList, -1);
            Document document = makeXml(transportDataList);

            // write the content into xml file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(document);

            //Convert DOM object to string
            StringWriter writer = new StringWriter();
            StreamResult result = new StreamResult(writer);
            transformer.transform(source, result);
            return writer.toString();

        } catch (TransformerException | ParserConfigurationException e) {
            logger.error("Error occurred while retrieving live broker details", e);
            throw new JMException("Error occurred while retrieving broker live details"+e.toString());
        }
    }

    /**
     * Making xml document.
     * @param transportDataList List of transport data objects.
     * @return Document.
     * @throws ParserConfigurationException
     */
    private Document makeXml(List<TransportData> transportDataList) throws ParserConfigurationException {

        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        // root elements
        Document document = docBuilder.newDocument();

        String IP_LIST = "IpList";
        Element rootElement = document.createElement(IP_LIST);
        document.appendChild(rootElement);
        List<String> allIpList;

        for (TransportData transportDataObject : transportDataList) {

            String NODE = "Node";
            Element node = document.createElement(NODE);
            rootElement.appendChild(node);

            String ADDRESSES = "Addresses";
            Element addresses = document.createElement(ADDRESSES);
            node.appendChild(addresses);

            /*String aTransportDataListAmqpHost = transportDataObject.getAmqpHost();
            StringTokenizer stringTokenizer = new StringTokenizer(aTransportDataListAmqpHost, ",");*/

                for (int w = 0; w<transportDataObject.getMultipleAddressDetails().size(); w++) {

                    String ID = "id";
                    node.setAttribute(ID, transportDataObject.getNodeIdentifier());
                    String ADDRESS = "Address";
                    Element address = document.createElement(ADDRESS);
                    addresses.appendChild(address);
                    String IP = "ip";
                    address.setAttribute(IP,transportDataObject.getMultipleAddressDetails().get(w).getIpAddress() );
                    String PORT = "port";
                    address.setAttribute(PORT, String.valueOf(transportDataObject.getAmqpPort()));
                    String SSL_PORT = "ssl-port";
                    address.setAttribute(SSL_PORT, String.valueOf(transportDataObject.getSslAmqpPort()));
                    String INTERFACE_NAME = "interface-name";
                    address.setAttribute(INTERFACE_NAME, transportDataObject.getMultipleAddressDetails().get(w).getInterfaceDetail());
                }

            }


        return document;
    }

}
