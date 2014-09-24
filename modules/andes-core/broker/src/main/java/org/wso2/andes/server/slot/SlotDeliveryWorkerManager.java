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

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.slot.thrift.MBThriftClient;
import org.wso2.andes.server.slot.thrift.MBThriftUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SlotDeliveryWorkerManager {

    private Map<Integer, SlotDeliveryWorker> slotDeliveryWorkerMap = new ConcurrentHashMap<Integer, SlotDeliveryWorker>();

    private ExecutorService slotDeliveryWorkerExecutor;
    private int numberOfThreads;
    private boolean reconnectingStarted = false;
    private static SlotDeliveryWorkerManager slotDeliveryWorkerManagerManager =
            new SlotDeliveryWorkerManager();


    private SlotDeliveryWorkerManager() {
        numberOfThreads = SlotCoordinationConstants.NUMBER_OF_SLOT_DELIVERY_WORKER_THREADS;
        this.slotDeliveryWorkerExecutor = Executors.newFixedThreadPool(numberOfThreads);
        startReconnectingToServerThread();
    }

    /**
     * @return SlotDeliveryWorkerManager instance
     */
    public static SlotDeliveryWorkerManager getInstance() {
        return slotDeliveryWorkerManagerManager;
    }

    /**
     * When a subscription is added this method will be called.
     * This method will decide which SlotDeliveryWorker thread is assigned to which queue
     *
     * @param queueName
     */
    //todo use schedule tasks
    public void startSlotDeliveryWorkerForQueue(String queueName) {
        int slotDeliveryWorkerId = getIdForSlotDeliveryWorker(queueName);
        if (slotDeliveryWorkerMap.containsKey(slotDeliveryWorkerId)) {
            //if this queue is not already in the queue
            if (!slotDeliveryWorkerMap.get(slotDeliveryWorkerId).getQueueList().contains(queueName)) {
                SlotDeliveryWorker slotDeliveryWorker = slotDeliveryWorkerMap.get(slotDeliveryWorkerId);
                slotDeliveryWorker.addQueueToThread(queueName);
            }
        } else {
            SlotDeliveryWorker slotDeliveryWorker = new SlotDeliveryWorker();
            slotDeliveryWorker.addQueueToThread(queueName);
            slotDeliveryWorkerMap.put(slotDeliveryWorkerId, slotDeliveryWorker);
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
        for (Map.Entry<Integer, SlotDeliveryWorker> slotDeliveryWorkerEntry : slotDeliveryWorkerMap.entrySet()) {
            slotDeliveryWorkerEntry.getValue().setRunning(false);
        }
    }

    /**
     * This thread is responsible of reconnecting to the thrift server
     * of the coordinator until it gets succeeded
     */
    public void startReconnectingToServerThread() {
        new Thread() {
            public void run() {
                /**
                 * this thread will try to connect to thrift server while reconnectingStarted flag is true
                 * After successfully connecting to the server this flag will be set to true
                 */
                while (reconnectingStarted) {

                    try {
                        MBThriftUtils.reConnectToServer();
                        //If re connect to server is successful, following code segment will be excuted
                        setReconnectingFlag(false);
                        for (Map.Entry<Integer, SlotDeliveryWorker> entry : slotDeliveryWorkerMap.entrySet()) {
                            SlotDeliveryWorker slotDeliveryWorker = entry.getValue();
                            if (!slotDeliveryWorker.isRunning()) {
                                slotDeliveryWorker.setRunning(true);
                            }
                        }
                    } catch (TTransportException e) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ignored) {
                           //silently ignore
                        }
                    }


                }
            }
        }.start();
    }

    /**
     * A flag to specify whether the reconnecting to thrift server is happening or not
     *
     * @return whether the reconnecting to thrift server is happening or not
     */
    public boolean isReconnectingToServerStarted() {
        return reconnectingStarted;
    }

    /**
     * Set reconnecting flag
     *
     * @param reconnectingFlag
     */
    public void setReconnectingFlag(boolean reconnectingFlag) {
        reconnectingStarted = reconnectingFlag;
    }

}
