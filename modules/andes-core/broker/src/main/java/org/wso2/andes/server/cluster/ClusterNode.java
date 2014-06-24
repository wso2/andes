/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.andes.server.cluster;


import java.util.ArrayList;
import java.util.List;

/**
 * Class <code>ClusterNode</code> contains the node information
 * that is needed by the cluster manager to handle cluster management tasks.
 */
public class ClusterNode {

    private int nodeId;
    private List<String> globalQueueWokers;
    public ClusterNode(int nodeId) {

        this.nodeId = nodeId;
        this.globalQueueWokers = new ArrayList<String>();
    }


    public List<String> getGlobalQueueWokers() {
        return globalQueueWokers;
    }


    public void addGlobalQueueWorker(String queueName) {
        this.globalQueueWokers.add(queueName);
    }

    public int getNumberOfQueueWorkers() {
        return this.globalQueueWokers.size();
    }


    public void removeGlobalQueueWorker(String queueName) {
        this.globalQueueWokers.remove(queueName);
    }
}
