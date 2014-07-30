package org.wso2.andes.server.cassandra;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * User: sajini
 * Date: 7/29/14
 * Time: 2:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class SlotDeliveryWorkerManager {

    private Map<Integer,SlotDeliveryWorker> queueWorkerMap =
            new ConcurrentHashMap<Integer,SlotDeliveryWorker>();

    private ExecutorService slotDeliveryWorkerExecutor;
    private int numberOfThreads;

    public SlotDeliveryWorkerManager(){
        numberOfThreads = 5;
        this.slotDeliveryWorkerExecutor = Executors.newFixedThreadPool(numberOfThreads);
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
           SlotDeliveryWorker slotDeliveryWorker = new SlotDeliveryWorker(slotDeliveryWorkerId);
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
