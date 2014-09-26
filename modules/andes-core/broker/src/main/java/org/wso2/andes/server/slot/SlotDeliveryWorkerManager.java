/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.andes.server.slot;

import org.apache.thrift.transport.TTransportException;
import org.wso2.andes.server.slot.thrift.MBThriftUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SlotDeliveryWorkerManager {

    private Map<Integer, SlotDeliveryWorker> slotDeliveryWorkerMap = new
            ConcurrentHashMap<Integer, SlotDeliveryWorker>();

    private ExecutorService slotDeliveryWorkerExecutor;
    private int numberOfThreads;
    private boolean reconnectingStarted = false;
    private static SlotDeliveryWorkerManager slotDeliveryWorkerManagerManager =
            new SlotDeliveryWorkerManager();


    private SlotDeliveryWorkerManager() {
        numberOfThreads = SlotCoordinationConstants.NUMBER_OF_SLOT_DELIVERY_WORKER_THREADS;
        this.slotDeliveryWorkerExecutor = Executors.newFixedThreadPool(numberOfThreads);
    }

    /**
     * @return SlotDeliveryWorkerManager instance
     */
    public static SlotDeliveryWorkerManager getInstance() {
        return slotDeliveryWorkerManagerManager;
    }

    /**
     * When a subscription is added this method will be called. This method will decide which
     * SlotDeliveryWorker thread is assigned to which queue
     *
     * @param queueName
     */
    public void startSlotDeliveryWorker(String queueName) {
        int slotDeliveryWorkerId = getIdForSlotDeliveryWorker(queueName);
        if (getSlotDeliveryWorkerMap().containsKey(slotDeliveryWorkerId)) {
            //if this queue is not already in the queue
            if (!getSlotDeliveryWorkerMap().get(slotDeliveryWorkerId).getQueueList()
                    .contains(queueName)) {
                SlotDeliveryWorker slotDeliveryWorker = getSlotDeliveryWorkerMap()
                        .get(slotDeliveryWorkerId);
                slotDeliveryWorker.addQueueToThread(queueName);
            }
        } else {
            SlotDeliveryWorker slotDeliveryWorker = new SlotDeliveryWorker();
            slotDeliveryWorker.addQueueToThread(queueName);
            getSlotDeliveryWorkerMap().put(slotDeliveryWorkerId, slotDeliveryWorker);
            slotDeliveryWorkerExecutor.execute(slotDeliveryWorker);
        }
    }

    /**
     * This method is to decide slotDeliveryWorkerId for the queue
     *
     * @param queueName
     * @return slot delivery worker ID
     */
    private int getIdForSlotDeliveryWorker(String queueName) {
        return queueName.hashCode() % numberOfThreads;
    }


    /**
     * stop all stop delivery workers in the thread pool
     */
    public void stopSlotDeliveryWorkers() {
        for (Map.Entry<Integer, SlotDeliveryWorker> slotDeliveryWorkerEntry :
                getSlotDeliveryWorkerMap()
                .entrySet()) {
            slotDeliveryWorkerEntry.getValue().setRunning(false);
        }
    }

    /**
     *
     * @return slotDeliveryWorkerMap
     */
    private Map<Integer, SlotDeliveryWorker> getSlotDeliveryWorkerMap() {
        return slotDeliveryWorkerMap;
    }

    /**
     * Start workers if not running
     */
    public void StartAllSlotDeliveryWorkers(){
        for (Map.Entry<Integer, SlotDeliveryWorker> entry :
                slotDeliveryWorkerMap.entrySet()) {
            SlotDeliveryWorker slotDeliveryWorker = entry.getValue();
            if (!slotDeliveryWorker.isRunning()) {
                slotDeliveryWorker.setRunning(true);
            }
        }
    }

}
