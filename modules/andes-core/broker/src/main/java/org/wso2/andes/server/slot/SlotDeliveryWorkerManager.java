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
package org.wso2.andes.server.slot;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SlotDeliveryWorkerManager {

    private Map<Integer,SlotDeliveryWorker> queueWorkerMap =
            new ConcurrentHashMap<Integer,SlotDeliveryWorker>();

    private ExecutorService slotDeliveryWorkerExecutor;
    private int numberOfThreads;
    private static SlotDeliveryWorkerManager slotDeliveryWorkerManagerManager;


    private SlotDeliveryWorkerManager(){
        numberOfThreads = 5;
        this.slotDeliveryWorkerExecutor = Executors.newFixedThreadPool(numberOfThreads);
    }

    public static SlotDeliveryWorkerManager getInstance() {
        if (slotDeliveryWorkerManagerManager == null) {

            synchronized (SlotDeliveryWorkerManager.class) {
                if (slotDeliveryWorkerManagerManager == null) {
                    slotDeliveryWorkerManagerManager = new SlotDeliveryWorkerManager();
                }
            }
        }
        return slotDeliveryWorkerManagerManager;
    }

    public void startSlotDeliveryWorkerForQueue(String queueName){
       int slotDeliveryWorkerId = getIdForSlotDeliveryWorker(queueName);
       if(queueWorkerMap.containsKey(slotDeliveryWorkerId)){
                //if this queue is not already in the queue
                if(!queueWorkerMap.get(slotDeliveryWorkerId).getQueueList().contains(queueName)){
                    SlotDeliveryWorker slotDeliveryWorker = queueWorkerMap.get(slotDeliveryWorkerId);
                    slotDeliveryWorker.addQueueToThread(queueName);
                }
       } else{
           SlotDeliveryWorker slotDeliveryWorker = new SlotDeliveryWorker();
           slotDeliveryWorker.addQueueToThread(queueName);
           queueWorkerMap.put(slotDeliveryWorkerId, slotDeliveryWorker);
           slotDeliveryWorkerExecutor.execute(slotDeliveryWorker);
       }
    }

    private int getIdForSlotDeliveryWorker(String queueName){

       int sum=0;
        char ch[] = queueName.toCharArray();
        for (int i=0; i < queueName.length(); i++){
            sum += ch[i];
        }
        return sum % numberOfThreads;
    }
}
