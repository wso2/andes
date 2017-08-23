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
 * Hold clustering related information related to a node.
 */
public class NodeDetail {
    /**
     * Node ID of the belonging node
     */
    private final String nodeId;

    /**
     * Indicate if the node is the coordinator node
     */
    private final boolean isCoordinator;

    public NodeDetail(String nodeId, boolean isCoordinator) {
        this.nodeId = nodeId;
        this.isCoordinator = isCoordinator;
    }

    /**
     * Getter method for Node ID
     *
     * @return node ID
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Getter method for isNewNode
     *
     * @return true if this is a new node
     */
    public boolean isCoordinator() {
        return isCoordinator;
    }
}
