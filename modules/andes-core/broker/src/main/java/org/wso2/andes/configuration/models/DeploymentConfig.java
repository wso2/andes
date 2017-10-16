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
package org.wso2.andes.configuration.models;

import org.wso2.carbon.config.annotation.Configuration;

/**
 * Configuration model for deployment mode config.
 */
@Configuration(description = "Specifies the deployment mode for the broker node (and cluster). Possible values {standalone, clustered}.\n"
        + "        standalone - This is the simplest mode a broker can be started. The node will assume that message store is not\n"
        + "                     shared with another node. Therefore it will not try to coordinate with other nodes (possibly\n"
        + "                     non-existent) to provide HA.\n"
        + "        clustered - Broker node will run in HA mode (active/passive).")
public class DeploymentConfig {

    /**
     * Specifying the clustering mode to be used.
     */
    private String mode = "standalone";

    public String getMode() {
        return mode;
    }
}