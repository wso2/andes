/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.andes.configuration.models.transport.mqtt;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for mqtt transport security.
 */
@Configuration(description = "Broker mqtt messaging transport based security config")
public class MQTTSecurityConfiguration {

    @Element(description = "Instructs the MQTT server whether clients should always send credentials"
            + "when establishing a connection.Possible values: \n"
            + "OPTIONAL: This is the default value. MQTT clients may or may not send credentials. If a client sends \n"
            + "credentials server will validate it.If client doesn't send credentials then server will not authenticate"
            + ",but allows client to establish the connection.This behavior adheres to MQTT 3.1 specification.\n"
            + "REQUIRED: Clients should always provide credentials when connecting.\n"
            + " If client doesn't send credentials or they are invalid server rejects the connection.")
    private String authentication = "OPTIONAL";

    @Element(description = "Class name of the authenticator to use. class should \n"
            + "inherit from org.dna.mqtt.moquette.server.IAuthenticator \n"
            + "Note: default implementation authenticates against carbon user store \n"
            + " based on supplied username/password")
    private String authenticator = "org.wso2.carbon.andes.authentication.andes.CarbonBasedMQTTAuthenticator";

    @Element(description = "Authorizer to be used for mqtt")
    private MQTTAuthorizerConfiguration authorizer = new MQTTAuthorizerConfiguration();

    public String getAuthentication() {
        return authentication;
    }

    public String getAuthenticator() {
        return authenticator;
    }

    public MQTTAuthorizerConfiguration getAuthorizer() {
        return authorizer;
    }
}
