package org.wso2.andes.server.cassandra;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: sajini
 * Date: 7/28/14
 * Time: 2:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class SlotDeliveryWorker extends Thread {

    private List<String> queueList;
    private int slotDeliveryWorkerId;

    public SlotDeliveryWorker(int slotDeliveryWorkerId) {
        this.queueList = new ArrayList<String>();
        this.slotDeliveryWorkerId = slotDeliveryWorkerId;
    }


    @Override
    public void run() {

        while (true) {
             for(String queue:queueList){

             }
        }

    }

    public void addQueueToThread(String queueName){
        getQueueList().add(queueName);
    }

    public List<String> getQueueList() {
        return queueList;
    }
}

