/*
 * Copyright (c)2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;

/**
 * Abstract class for the master slave task.
 */
public abstract class MasterSlaveTask implements Runnable, StoreHealthListener{


    protected final int TASK_PERIOD;
    protected AndesContextStore andesContextStore;
    protected Logger log;
    protected String myNodeId;

    public MasterSlaveTask() {
        andesContextStore = AndesContext.getInstance().getAndesContextStore();
        TASK_PERIOD = AndesConfigurationManager.readValue(AndesConfiguration.MASTER_ELECTION_TASK_PERIOD);
        myNodeId = AndesConfigurationManager.readValue(AndesConfiguration.MASTER_SLAVE_NODE_ID);
        FailureObservingStoreManager.registerStoreHealthListener(this);
    }

    public void waitForMaster() {
        try {
            Thread.sleep(TASK_PERIOD * 2);
        } catch (InterruptedException e) {
            log.error("Error while waiting for master. Retrying... ", e);
            waitForMaster();
        }
    }

    @Override
    public void storeOperational(HealthAwareStore store) {
        //Do nothing, the scheduled task will run
    }

    @Override
    public void storeNonOperational(HealthAwareStore store, Exception ex) {
        if (MasterSlaveStateManager.isMaster()) {
            log.info("Store non-operational. Hence, this node is elected as a slave");
            MasterSlaveStateManager.slaveElected();
        }
    }
}
