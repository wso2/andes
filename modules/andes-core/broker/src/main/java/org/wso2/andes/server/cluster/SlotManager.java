package org.wso2.andes.server.cluster;

import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.server.cassandra.Slot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: sajini
 * Date: 7/28/14
 * Time: 2:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class SlotManager {

    private static SlotManager slotManager;
    private ConcurrentHashMap<String,Slot> slotAssignmentMap;
    private ConcurrentHashMap<String,ArrayList<Slot>> freeSlotsMap;

    public SlotManager() {
       this.slotAssignmentMap = new ConcurrentHashMap<String, Slot>();
        this.freeSlotsMap = new ConcurrentHashMap<String, ArrayList<Slot>>();
    }

    public static SlotManager getInstance() {
        if (slotManager == null) {

            synchronized (SlotManager.class) {
                if (slotManager == null) {
                    slotManager = new SlotManager();
                }
            }
        }
        return slotManager;
    }

   public String[] getSlotRangeForQueue(String queueName){
       if(slotAssignmentMap.get(queueName)==null){
            if(freeSlotsMap.get(queueName)!=null){
                freeSlotsMap.get(queueName).
       }

   }



}
