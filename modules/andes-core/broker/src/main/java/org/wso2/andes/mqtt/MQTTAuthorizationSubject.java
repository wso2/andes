/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.andes.mqtt;

/**
 * Stores authorization details for MQTT clients.
 */
public class MQTTAuthorizationSubject {

    /**
     * The MQTT Client ID.
     */
    private String clientID;

    /**
     * Carbon username of MQTT client.
     */
    private String username;

    /**
     * User flag (true/false) of the connecting client.
     */
    private boolean userFlag;

    /**
     * The tenant domain client has connected to.
     */
    private String tenantDomain;

    /**
     * Initialize Authorization Subject with the clientID and userFlag which is required.
     *
     * @param clientID The MQTT ClientID
     * @param userFlag Is the user flag set in the MQTT Client
     */
    public MQTTAuthorizationSubject(String clientID, boolean userFlag) {
        this.clientID = clientID;
        this.userFlag = userFlag;
    }

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public boolean isUserFlag() {
        return userFlag;
    }

    public String getTenantDomain() {
        return tenantDomain;
    }

    public void setTenantDomain(String tenantDomain) {
        this.tenantDomain = tenantDomain;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
