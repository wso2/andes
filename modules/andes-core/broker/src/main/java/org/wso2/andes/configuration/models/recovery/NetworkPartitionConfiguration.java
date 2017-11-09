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
 * Configuration model for network partition detection.
 */
@Configuration(description = "Enables network partition detection ( and surrounding functionality, such"
        + "as disconnecting subscriptions, enabling error based flow control if the minimal node count becomes less "
        + "than configured value.")
public class NetworkPartitionConfiguration {

    @Element(description = "Enables network partition detection ( and surrounding functionality, such as disconnecting"
            + " subscriptions, enabling error based flow control if the minimal node count becomes less than"
            + " configured value.")
    private boolean enabled = false;

    @Element(description = "The minimum node count the cluster should maintain for this node to operate. if cluster "
            + "size becomes less that configured value this node will not accept any incoming traffic (and disconnect"
            + "subscriptions) etc.")
    private int minimumClusterSize = 1;

    public boolean isEnabled() {
        return enabled;
    }

    public int getMinimumClusterSize() {
        return minimumClusterSize;
    }
}
