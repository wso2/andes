/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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

package org.wso2.andes.server.cluster;

/**
 * The type used define the coordination algorithm used to identify the coordinator in the cluster.
 */
public interface CoordinationStrategy {

    /**
     * Meant to be invoked when coordination algorithm should start working.
     * This is typically during the server start up.
     *
     * @param hazelcastClusterAgent Hazelcast cluster agent used to indicate cluster change events
     * @param nodeId
     */
    void start(HazelcastClusterAgent hazelcastClusterAgent, String nodeId);

    /**
     * Used to query about current node's coordinator status
     *
     * @return True if current node is the coordinator, False otherwise
     */
    boolean isCoordinator();

    /**
     * Meant to be invoked when coordination algorithm should stop working.
     * This is typically during the server shutdown.
     */
    void stop();
}
