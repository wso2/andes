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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The class for managing all tasks and information with regard to Master/Slave deployment.
 */
public class MasterSlaveStateManager {

    private static MasterSlaveTask masterSlavetask;
    private static final String DEFAULT = "DEFAULT";
    private static final String LOCK = "LOCK";
    private static final Logger log = Logger.getLogger(MasterSlaveStateManager.class);
    private static MasterSlaveInfo masterSlaveInfo = MasterSlaveInfo.getMasterSlaveInfo();

    /**
     * Scheduler for periodically executing the master election task
     */
    private static final ScheduledExecutorService masterSlaveTaskScheduler = Executors.newScheduledThreadPool(1);

    /**
     * All the {@link MasterSlaveStateChangeListener} implementations registered to receive
     * callbacks on the state change from either master -> slave ot slave -> master.
     */
    private static Collection<MasterSlaveStateChangeListener> masterSlaveListeners =
            Collections.synchronizedCollection(new ArrayList<MasterSlaveStateChangeListener>());

    /**
     * Create master election task based on the method configured in the broker.xml
     *
     * @throws AndesException
     */
    public static void createTask() throws AndesException {

        String masterElectionMechanism = AndesConfigurationManager.readValue(AndesConfiguration
                .MASTER_ELECTION_MECHANISM);
        switch (masterElectionMechanism) {
            case DEFAULT:
                masterSlavetask = new DefaultMasterElectionTask();
                break;
            case LOCK:
                masterSlavetask = new LockingMasterElectionTask();
                break;
            default:
                log.warn("Unknown election method: " + masterElectionMechanism + ". Defaulting to method " + LOCK);
                masterSlavetask = new LockingMasterElectionTask();
                break;
        }

        // Notify all the listeners for the state that the node was appointed as a slave.
        // TODO: Remove this once Andes Kernel Boot is modified to boot as a slave
        slaveElected();

        //Schedule the master election task
        scheduleTask();
    }

    /**
     * Method to schedule the task to run periodically with the time interval given in the broker.xml.
     */
    public static void scheduleTask() {
        int masterSalveTaskInterval = AndesConfigurationManager.readValue(AndesConfiguration
                .MASTER_ELECTION_TASK_PERIOD);
        masterSlaveTaskScheduler.scheduleAtFixedRate(masterSlavetask, 0, masterSalveTaskInterval, TimeUnit.SECONDS);
    }

    /**
     * Method to shut down the master election task.
     */
    public static void shutDownTask() {
        masterSlaveTaskScheduler.shutdown();
        try {
            masterSlaveTaskScheduler.awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            masterSlaveTaskScheduler.shutdownNow();
            log.warn("Master/Slave task scheduler is forcefully shutdown.");
        }
    }

    /**
     * Add a listener to the state change.
     *
     * @param listener
     */
    public static void addMasterSlaveStateListener(MasterSlaveStateChangeListener listener) {
        masterSlaveListeners.add(listener);
    }

    /**
     * Method to notify the listeners that this node is elected as the master.
     */
    public static void masterElected() {
        masterSlaveInfo.setMaster(true);
        for (MasterSlaveStateChangeListener listener : masterSlaveListeners) {
            listener.electedAsMaster();
        }
    }

    /**
     * Method to notify the listeners that the node is downgraded to be a slave
     */
    public static void slaveElected() {
        masterSlaveInfo.setMaster(false);
        for (MasterSlaveStateChangeListener listener : masterSlaveListeners) {
            listener.electedAsSlave();
        }
    }

    public static boolean isMaster() {
        return masterSlaveInfo.isMaster();
    }

}
