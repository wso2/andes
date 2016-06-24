/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import org.apache.log4j.Logger;
import java.util.Map;

/**
 * Task that tries to elect master periodically while being a slave.
 */
public class LockingMasterElectionTask extends MasterSlaveTask {

    public LockingMasterElectionTask() throws AndesException {
        super();
        log = Logger.getLogger(LockingMasterElectionTask.class);

        //Check if there's an entry in the store indicating that a node is present. If not, enter record.
        //In the case where all nodes start at the same time, there could be an issue when 2 nodes read that the
        // table as empty and one has locked the table when the other is trying to insert a record.
        Map<String, Integer> nodes = andesContextStore.readHeartBeat();
        if (nodes.isEmpty()) {
            andesContextStore.insertHeartBeat(myNodeId, 0);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {

        //If this node is not a master, try to acquire the database lock to become the master.
        if (!MasterSlaveStateManager.isMaster()) {
            log.info("Running master election task in slave, trying to acquire database lock.");
            if (andesContextStore.acquireLock()) {
                log.info("Database lock acquired, this node is elected as the master");
                //Notify all the listeners
                MasterSlaveStateManager.masterElected();
            } else {
                log.info("Could not acquire database lock, this node is will remain a slave.");
            }
        }
        else{
            if (!andesContextStore.isOperational("testString", System.currentTimeMillis())){
                MasterSlaveStateManager.slaveElected();
            }
        }

    }
}
