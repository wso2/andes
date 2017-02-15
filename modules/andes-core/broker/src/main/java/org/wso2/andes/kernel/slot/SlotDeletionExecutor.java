/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel.slot;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.server.cluster.error.detection.NetworkPartitionListener;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * This class is responsible for deleting slots and scheduling slot deletions.
 */
public class SlotDeletionExecutor implements NetworkPartitionListener {

    private static Log log = LogFactory.getLog(SlotDeletionExecutor.class);

    /**
     * Slot deletion executor service
     */
    private ExecutorService slotDeletionExecutorService;

    /**
     * SlotDeletionScheduler instance
     */
    private static SlotDeletionExecutor instance;

    /**
     * Number of threads executing slot deleting task in parallel
     */
    private int parallelTaskCount;

    /**
     * Total number of slots submitted to delete
     */
    private int numOfScheduledSlots;

    /**
     * List of SlotDeletingTask executing slot delete in parallel
     */
    private List<SlotDeletingTask> slotDeletingTasks;

    /**
     * SlotDeletionExecutor constructor
     */
    private SlotDeletionExecutor() {

    }

    /**
     * Create slot deletion scheduler
     */
    public void init(int parallelTaskCount, int maxNumberOfPendingSlots) {

        this.numOfScheduledSlots = 0;
        this.parallelTaskCount = parallelTaskCount;
        this.slotDeletingTasks = new ArrayList<>(parallelTaskCount);

        // network partition detection works only when clustered.
        if (AndesContext.getInstance().isClusteringEnabled()) {
            AndesContext.getInstance().getClusterAgent().addNetworkPartitionListener(40, this);
        }

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat
                ("SlotDeletionExecutor-%d").build();

        this.slotDeletionExecutorService = Executors.newFixedThreadPool(parallelTaskCount, namedThreadFactory);

        for(int taskCount = 0; taskCount < parallelTaskCount; taskCount++) {
            SlotDeletingTask slotDeletingTask = new SlotDeletingTask(maxNumberOfPendingSlots);
            this.slotDeletingTasks.add(slotDeletingTask);
            this.slotDeletionExecutorService.submit(slotDeletingTask);
        }
    }

    /**
     * Schedule a slot for deletion. This will continuously try to delete the slot
     * until slot manager informs that the slot is successfully removed
     *
     * @param slot slot to be removed from cluster
     */
    public void scheduleToDelete(Slot slot) {
        numOfScheduledSlots = numOfScheduledSlots + 1;
        int taskIndex = numOfScheduledSlots % parallelTaskCount;
        slotDeletingTasks.get(taskIndex).scheduleToDelete(slot);
    }

    /**
     * Shutdown slot deletion executor service
     */
    public void stopSlotDeletionExecutor() {
        for (SlotDeletingTask slotDeletingTask : slotDeletingTasks) {
            slotDeletingTask.stop();
        }
        slotDeletionExecutorService.shutdown();
    }

    /**
     * Return slot deletion scheduler object
     *
     * @return SlotDeletionScheduler object
     */
    public static SlotDeletionExecutor getInstance() {
        if (instance == null) {
            instance = new SlotDeletionExecutor();
        }
        return instance;
    }

    @Override
    public void minimumNodeCountNotFulfilled(int currentNodeCount) {
        // Ignored at the moment
    }

    @Override
    public void minimumNodeCountFulfilled(int currentNodeCount) {
        // Ignored at the moment
    }

    @Override
    public void clusteringOutage() {
        log.info("cluster outage detected, stopping slot deletion executor");
        stopSlotDeletionExecutor();
    }

}
