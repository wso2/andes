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

import java.util.List;

/**
 * Class that contains getters and setters of transport data.
 */
public class TransportData {

    private String nodeIdentifier;
    private List<MultipleAddressDetails> multipleAddressDetails;
    private int amqpPort;
    private int sslAmqpPort;

    /**
     * This constructor set these parameters
     * @param nodeIdentifier Node details
     * @param multipleAddressDetailses AMQP host address
     * @param amqpPort AMQP port
     * @param sslAmqpPort AMQP SSL port
     */
    public TransportData(String nodeIdentifier, List<MultipleAddressDetails> multipleAddressDetailses, int amqpPort, int sslAmqpPort) {
        this.nodeIdentifier=nodeIdentifier;
        this.multipleAddressDetails = multipleAddressDetailses;
        this.amqpPort = amqpPort;
        this.sslAmqpPort = sslAmqpPort;
    }

    public String getNodeIdentifier() {
        return nodeIdentifier;
    }

    public void setNodeIdentifier(String nodeIdentifier) {
        this.nodeIdentifier = nodeIdentifier;
    }

    public List<MultipleAddressDetails> getMultipleAddressDetails() {
        return multipleAddressDetails;
    }

    public void setMultipleAddressDetails(List<MultipleAddressDetails> multipleAddressDetails) {
        this.multipleAddressDetails = multipleAddressDetails;
    }

    public int getAmqpPort() {
        return amqpPort;
    }

    public void setAmqpPort(int amqpPort) {
        this.amqpPort = amqpPort;
    }

    public int getSslAmqpPort() {
        return sslAmqpPort;
    }

    public void setSslAmqpPort(int sslAmqpPort) {
        this.sslAmqpPort = sslAmqpPort;
    }
}
