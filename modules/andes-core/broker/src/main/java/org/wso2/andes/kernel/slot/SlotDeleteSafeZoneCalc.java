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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class SlotDeleteSafeZoneCalc implements Runnable{

    private static Log log = LogFactory.getLog(SlotDeleteSafeZoneCalc.class);

    private long slotDeleteSafeZone;

    private boolean running;

    private boolean isLive;

    private int seekInterval;


    public SlotDeleteSafeZoneCalc(int seekInterval) {
        this.seekInterval = seekInterval;
        this.running = true;
        this.isLive = true;
        this.slotDeleteSafeZone = Long.MAX_VALUE;
    }
    @Override
    public void run() {
         while (running) {
             if(isLive) {
                 Set<String> nodesWithPublishedMessages = SlotManager.getInstance().getMessagePublishedNodes();
                 Map<String, Long> nodeInformedSafeZones =
                         SlotManager.getInstance().getNodeInformedSlotDeletionSafeZones();

                 //calculate safe zone (minimum value of starting messageIDs of assigned slots of every node in cluster)
                 long safeZoneValue = Long.MAX_VALUE;
                 //node-wise iterate
                 for (String nodeID : nodesWithPublishedMessages) {
                     Long safeZoneByPublishedMessages = SlotManager.getInstance().getLastPublishedIDByNode(nodeID);

                     if(null != safeZoneByPublishedMessages) {
                         safeZoneValue = safeZoneByPublishedMessages;
                     }

                     long nodeInformedSafeZone = nodeInformedSafeZones.get(nodeID);

                     /**
                      * if no new messages are published and no new slot assignment happened
                      * node informed value can be bigger. We need to accept that to keep the
                      * safe zone moving
                      */
                     if(Long.MAX_VALUE != safeZoneValue) {
                         if(safeZoneValue < nodeInformedSafeZone) {
                             safeZoneValue = nodeInformedSafeZone;
                         }
                     } else {
                         safeZoneValue = nodeInformedSafeZone;
                     }
                 }

                 slotDeleteSafeZone = safeZoneValue;

                 log.info("FIXX : " + "Safe Zone Calculated : " + safeZoneValue);

                 try {
                     Thread.sleep(seekInterval);
                 } catch (InterruptedException e) {
                     //silently ignore
                 }
             }  else {
                 try {
                     Thread.sleep(15*1000);
                 } catch (InterruptedException e) {
                     //silently ignore
                 }
             }
         }
    }


    public long getSlotDeleteSafeZone() {
        return slotDeleteSafeZone;
    }

    public void setSlotDeleteSafeZone(long slotDeleteSafeZone) {
        this.slotDeleteSafeZone = slotDeleteSafeZone;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isLive() {
        return isLive;
    }

    public void setLive(boolean isLive){
        this.isLive = isLive;
    }

    public int getSeekInterval() {
        return seekInterval;
    }

    public void setSeekInterval(int seekInterval) {
        this.seekInterval = seekInterval;
    }
}
