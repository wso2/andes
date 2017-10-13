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

@Configuration(description = "Message broker keeps track of all messages it has received as groups. These groups are termed\n"
        + "    'Slots' (To know more information about Slots and message broker install please refer to online wiki).\n"
        + "    Size of a slot is loosely determined by the configuration <windowSize> (and the number of\n"
        + "    parallel publishers for specific topic/queue). Message broker cluster (or in single node) keeps\n"
        + "    track of slots which constitutes for a large part of operating state before the cluster went down.\n"
        + "        When first message broker node of the cluster starts up, it will read the database to recreate\n"
        + "    the internal state to previous state.")
public class RecoveryConfig {

    @Element(description = " There could be multiple storage queues worked before entire cluster (or single node) went down.\n"
            + "        We need to recover all remaining messages of each storage queue when first node startup and we can\n"
            + "        read remaining message concurrently of each storage queue. Default value to set here to 5. You can\n"
            + "        increase this value based on number of storage queues exist. Please use optimal value based on\n"
            + "        number of storage queues to speed up warm startup.")
    private int concurrentStorageQueueReads = 5;

    @Element(description = "Virtual host sync interval seconds in for the Virtual host syncing Task which will\n"
            + "            sync the Virtual host details across the cluster")
    private int vHostSyncTaskInterval = 900;

    @Element(description = "Enables network partition detection ( and surrounding functionality, such\n"
            + "         as disconnecting subscriptions, enabling error based flow control if the\n"
            + "         minimal node count becomes less than configured value.")
    private NetworkPartitionsDetectionConfig networkPartitionsDetection = new NetworkPartitionsDetectionConfig();

    public int getConcurrentStorageQueueReads() {
        return concurrentStorageQueueReads;
    }

    public int getvHostSyncTaskInterval() {
        return vHostSyncTaskInterval;
    }

    public NetworkPartitionsDetectionConfig getNetworkPartitionsDetection() {
        return networkPartitionsDetection;
    }
}