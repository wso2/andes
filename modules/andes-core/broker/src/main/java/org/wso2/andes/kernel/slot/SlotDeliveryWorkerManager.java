/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * This class is responsible of allocating SloDeliveryWorker threads to each queue
 */
public class SlotDeliveryWorkerManager {

    private Map<Integer, SlotDeliveryWorker> slotDeliveryWorkerMap = new
            ConcurrentHashMap<Integer, SlotDeliveryWorker>();

    private ExecutorService slotDeliveryWorkerExecutor;

    private static Log log = LogFactory.getLog(SlotDeliveryWorkerManager.class);

    /**
     * Number of slot delivery worker threads running inn one MB node
     */
    private static ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat
            ("SlotDeliveryWorkerExecutor-%d").build();


    /**
    Number of slot delivery worker threads running in one MB node
     */
    private Integer numberOfThreads;

    /**
     * SlotDeliveryWorker instance
     */
    private static SlotDeliveryWorkerManager slotDeliveryWorkerManagerManager = new SlotDeliveryWorkerManager();


    private SlotDeliveryWorkerManager() {
        numberOfThreads = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SLOTS_WORKER_THREAD_COUNT);
        this.slotDeliveryWorkerExecutor = Executors.newFixedThreadPool(numberOfThreads, namedThreadFactory);
    }

    /**
     * @return SlotDeliveryWorkerManager instance
     */
    public static SlotDeliveryWorkerManager getInstance() {
        return slotDeliveryWorkerManagerManager;
    }

    /**
     * When a subscription is added this method will be called. This method will decide which
     * SlotDeliveryWorker thread is assigned to which queue. If a worker is already running on
     * the queue, it will not start a new one.
     *
     * @param storageQueueName  name of the queue to start slot delivery worker for
     */
    public synchronized void startSlotDeliveryWorker(String storageQueueName, String destinaton) {
        int slotDeliveryWorkerId = getIdForSlotDeliveryWorker(storageQueueName);
        if (getSlotDeliveryWorkerMap().containsKey(slotDeliveryWorkerId)) {
            //if this queue is not already in the queue
            if (getSlotDeliveryWorkerMap().get(slotDeliveryWorkerId).getStorageQueueNameToDestinationMap()
                    .get(storageQueueName) == null) {
                SlotDeliveryWorker slotDeliveryWorker = getSlotDeliveryWorkerMap()
                        .get(slotDeliveryWorkerId);
                slotDeliveryWorker.addQueueToThread(storageQueueName, destinaton);
                if(log.isDebugEnabled()) {
                    log.debug("Assigned Already Running Slot Delivery Worker. Reading messages storageQ= " + storageQueueName + " MsgDest= " + destinaton);
                }
            }
        } else {
            SlotDeliveryWorker slotDeliveryWorker = new SlotDeliveryWorker();
            if(log.isDebugEnabled()) {
                log.debug("Slot Delivery Worker Started. Reading messages storageQ= " + storageQueueName + " MsgDest= " + destinaton);
            }
            slotDeliveryWorker.addQueueToThread(storageQueueName, destinaton);
            getSlotDeliveryWorkerMap().put(slotDeliveryWorkerId, slotDeliveryWorker);
            slotDeliveryWorkerExecutor.execute(slotDeliveryWorker);
        }
    }

    /**
     * This method is to decide slotDeliveryWorkerId for the queue
     *
     * @param queueName name of the newly created queue
     * @return slot delivery worker ID
     */
    public int getIdForSlotDeliveryWorker(String queueName) {
        // Get the absolute value since String.hashCode() can give both positive and negative values.
        return Math.abs(queueName.hashCode() % numberOfThreads);
    }


    /**
     * Stop all stop delivery workers in the thread pool
     */
    public void stopSlotDeliveryWorkers() {
        for (Map.Entry<Integer, SlotDeliveryWorker> slotDeliveryWorkerEntry :
                getSlotDeliveryWorkerMap()
                        .entrySet()) {
            slotDeliveryWorkerEntry.getValue().setRunning(false);
        }
    }

    /**
     * @return SlotDeliveryWorkerMap  a map which stores slot delivery worker ID against
     * SlotDelivery
     * Worker
     * object references
     */
    private Map<Integer, SlotDeliveryWorker> getSlotDeliveryWorkerMap() {
        return slotDeliveryWorkerMap;
    }

    /**
     * Start all the SlotDeliveryWorkers if not already in running state.
     */
    public void startAllSlotDeliveryWorkers() {
        for (Map.Entry<Integer, SlotDeliveryWorker> entry :
                slotDeliveryWorkerMap.entrySet()) {
            SlotDeliveryWorker slotDeliveryWorker = entry.getValue();
            if (!slotDeliveryWorker.isRunning()) {
                slotDeliveryWorkerExecutor.execute(slotDeliveryWorker);
            }
        }
    }

    /**
     * Returns SlotDeliveryWorker mapped to a given queue
     *
     * @param queueName name of the queue
     * @return SlotDeliveryWorker instance
     */
    public SlotDeliveryWorker getSlotWorker(String queueName) {
        return slotDeliveryWorkerMap.get(getIdForSlotDeliveryWorker(queueName));
    }
}
