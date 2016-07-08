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
 * Hold information related to a node heartbeat entry. This can be used to pass information related to node heartbeat
 * to/from persistence layer.
 */
public class NodeHeartBeatData {
    /**
     * Node ID of the belonging node
     */
    private final String nodeId;

    /**
     * The last updated heartbeat value
     */
    private final long lastHeartbeat;

    /**
     * Indicate if the node addition is already identified by the coordinator
     */
    private final boolean isNewNode;

    /**
     * NodeHeartBeatData constructor
     *
     * @param nodeId        Node ID
     * @param lastHeartbeat Last heartbeat received from the node
     * @param isNewNode     True if new node
     */
    public NodeHeartBeatData(String nodeId, long lastHeartbeat, boolean isNewNode) {
        this.nodeId = nodeId;
        this.lastHeartbeat = lastHeartbeat;
        this.isNewNode = isNewNode;
    }

    /**
     * Getter method for Node ID
     *
     * @return Node ID
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Getter method for last heartbeat
     *
     * @return Last heartbeat received form the node
     */
    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    /**
     * Getter method for isNewNode
     *
     * @return True if this is a new node
     */
    public boolean isNewNode() {
        return isNewNode;
    }
}
