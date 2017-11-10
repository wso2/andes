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
package org.wso2.andes.configuration.models.recovery;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for recovery related configs.
 */
@Configuration(description = "Message recovery related configs")
public class RecoveryConfiguration {

    @Element(description = "There could be multiple storage queues worked before entire cluster (or single node) went"
            + " down.We need to recover all remaining messages of each storage queue when first node startup and we can"
            + "read remaining message concurrently of each storage queue. Default value to set here to 5. You can\n"
            + " increase this value based on number of storage queues exist. Please use optimal value based on\n"
            + "number of storage queues to speed up warm startup.")
    private int concurrentStorageQueueReads = 5;

    @Element(description = "Virtual host sync interval seconds in for the Virtual host syncing Task which will\n"
            + "sync the Virtual host details across the cluster")
    private int vHostSyncTaskInterval = 900;

    @Element(description = "Enables network partition detection ( and surrounding functionality, such\n"
            + "as disconnecting subscriptions, enabling error based flow control if the minimal node count becomes"
            + " less than configured value.")
    private NetworkPartitionConfiguration networkPartitionsDetection = new NetworkPartitionConfiguration();

    public int getConcurrentStorageQueueReads() {
        return concurrentStorageQueueReads;
    }

    public int getvHostSyncTaskInterval() {
        return vHostSyncTaskInterval;
    }

    public NetworkPartitionConfiguration getNetworkPartitionsDetection() {
        return networkPartitionsDetection;
    }
}
