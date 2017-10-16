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
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for RDBMS based cluster event synchronization.
 */
@Configuration(description = "RDBMS based cluster event synchronization configs")
public class RDBMSClusterEventSynchronizationConfig {

    @Element(description = "Enabling this will make the cluster notifications such as Queue changes(additions and deletions),\n"
            + "Subscription changes, etc. sent within the cluster be synchronized using RDBMS. If set to false, Hazelcast\n"
            + "will be used for this purpose.")
    private boolean enabled = true;

    @Element(description = "Specifies the interval at which, the cluster events will be read from the database. Needs to be\n"
            + "declared in milliseconds. Setting this to a very low value could downgrade the performance where as\n"
            + "setting this to a large value could increase the time taken for a cluster event to be synchronized in\n"
            + "all the nodes in a cluster.")
    private int eventSyncInterval = 1000;

    public boolean isEnabled() {
        return enabled;
    }

    public int getEventSyncInterval() {
        return eventSyncInterval;
    }

}