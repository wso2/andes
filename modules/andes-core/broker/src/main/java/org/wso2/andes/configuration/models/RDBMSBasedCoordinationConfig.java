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
 * Configuration model for RDBMS based coordination algorithm.
 */
@Configuration(description = "Configurations related to RDBMS based coordination algorithm.")
public class RDBMSBasedCoordinationConfig {

    @Element(description = "Enable RDBMS based coordination algorithm.")
    private boolean enabled = true;

    @Element(description = "Heartbeat interval used in the RDBMS base coordination algorithm in milliseconds.")
    private int heartbeatInterval = 5000;

    @Element(description = "Time to wait before informing others about coordinator change in milliseconds. This value should be\n"
            + "larger than a database read time including network latency and should be less than heartbeatInterval")
    private int coordinatorEntryCreationWaitTime = 3000;

    @Element(description = "Time interval used to poll database for membership related events in milliseconds.")
    private int eventPollingInterval = 4000;

    public boolean isEnabled() {
        return enabled;
    }

    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public int getCoordinatorEntryCreationWaitTime() {
        return coordinatorEntryCreationWaitTime;
    }

    public int getEventPollingInterval() {
        return eventPollingInterval;
    }
}