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
 * Configuration model for mqtt transport authorization
 */
@Configuration(description = "Broker authorization config related to mqtt transport")
public class MQTTAuthorizerConfiguration {

    @Element(description = "Class name of the authorizer to use. class should\n"
            + "                        inherit from org.dna.mqtt.moquette.server.IAutherizer")
    private String className = "org.wso2.carbon.andes.authorization.andes.CarbonPermissionBasedMQTTAuthorizer";

    @Element(description = "connectionPermission is required for a user to connect to broker")
    private String connectionPermission = "/permission/admin/mqtt/connect";

    public String getClassName() {
        return className;
    }

    public String getConnectionPermission() {
        return connectionPermission;
    }

}
