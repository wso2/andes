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

/**
 * This class responsible for handling multiple interface details and IP addresses.
 */

public class MultipleAddressDetails {

    //Network interface name
    private String interfaceDetail;

    //Ip address relevant to interface
    private String ipAddress;

    /**
     * This constructor set these parameters.
     * @param interfaceDetail network interface name.
     * @param ipAddress IP address related to interface.
     */
    public MultipleAddressDetails(String interfaceDetail, String ipAddress) {
        this.interfaceDetail = interfaceDetail;
        this.ipAddress = ipAddress;
    }

    /**
     * Get Network Interface details.
     * @return Interface name.
     */
    public String getInterfaceDetail() {
        return interfaceDetail;
    }

    /**
     * Set Network interface details.
     * @param interfaceDetail Interface name.
     */
    public void setInterfaceDetail(String interfaceDetail) {
        this.interfaceDetail = interfaceDetail;
    }

    /**
     * Get IP address relevant to network interface.
     * @return
     */
    public String getIpAddress() {
        return ipAddress;
    }

    /**
     * Set IP address.
     * @param ipAddress IP Address.
     */
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }
}
