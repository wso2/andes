/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sasikala on 6/9/16.
 */
public class DefaultMasterElectionTask extends MasterSlaveTask implements Runnable {
//    private MasterSlaveInfo masterSlaveInfo;
//    private int timeInterval;
//    private AndesContextStore andesContextStore;
    private Map<String, Integer> previousheartBeatValues;
    private HashMap<String, Integer> priorities;

    public DefaultMasterElectionTask(){
        super();
        System.out.println("Starting default master slave tase!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        priorities = new HashMap<>();
        andesContextStore = AndesContext.getInstance().getAndesContextStore();
        try {
            Map<String, Integer> currentHearBeats;
            currentHearBeats = readHeartBeat();

            //This is the startup of the deployment
            if (currentHearBeats.isEmpty()) {
                System.out.println("This is a fresh start no updates");
                insertHeartBeat();

            } else {
                System.out.println("There has been a master");
                if (!currentHearBeats.containsKey(myNodeId)) {
                    System.out.println("I have never updated.");
                    insertHeartBeat();
                    currentHearBeats.put(myNodeId, 1);
                }
                previousheartBeatValues = currentHearBeats;
            }
            previousheartBeatValues = readHeartBeat();
        }
        catch (AndesException e){
            e.printStackTrace();
            MasterSlaveStateManager.slaveElected();
        }

    }

    @Override
    public void run() {
        while(true) {
            System.out.println("Running master election task......");
            try {
                Map<String, Integer> currentHeartBeatValues = readHeartBeat();
                if (MasterSlaveStateManager.isMaster()) {
                    runMaster(currentHeartBeatValues);

                } else {
                    runSlave(currentHeartBeatValues);
                }
                previousheartBeatValues = currentHeartBeatValues;
            } catch (AndesException e) {
                e.printStackTrace();
                MasterSlaveStateManager.slaveElected();
            }
            try {
                System.out.println("Waiting for 60 seconds to run again");
                Thread.sleep(TASK_PERIOD);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void runMaster(Map<String, Integer> currentHeartBeatValues){
        System.out.println("Running master task.......");
        //Nobody has updated after me
        if (!haveIUpdated(currentHeartBeatValues)){
            System.out.println("I haven't updated");
            MasterSlaveStateManager.slaveElected();
        }
        else{
            if (!hasAnyoneElseUpdated(currentHeartBeatValues)){
                //Do nothing remain master
                System.out.println("Others haven't updated");
                try {
                    updateHearBeat(currentHeartBeatValues);
                } catch (AndesException e) {
                    e.printStackTrace();
                    MasterSlaveStateManager.slaveElected();
                }
            }
            else{
                System.out.println("Somebody has updated");
                //Somebody else has updated
                //Be slave
                //Once the master sees that someone else has updated, he become the slave
                //This can lead to a situation where master slave switch is frequent
                MasterSlaveStateManager.slaveElected();
            }
        }
    }

    public void runSlave(Map<String, Integer> currentHeartBeatValues){
        System.out.println("Running slave task.....");
        boolean masterAlive = false;
        for (Map.Entry<String, Integer> entry: currentHeartBeatValues.entrySet()) {
            String nodeId = entry.getKey();
            if (entry.getValue() != previousheartBeatValues.get(nodeId).intValue()) {
                masterAlive = true;
                System.out.println("Master alive");
                break;
            }
        }
        if (masterAlive){
            //Do nothing, remain slave
            //update the previous heart beat values
            previousheartBeatValues = currentHeartBeatValues;
        }
        else{
            try {
                System.out.println("Master is not alive, waiting for master");

                    waitForMaster();
                previousheartBeatValues = currentHeartBeatValues;
                currentHeartBeatValues = readHeartBeat();
                if (!hasAnyoneElseUpdated(currentHeartBeatValues)) {
                    System.out.println("Master not alive, updating");
                    previousheartBeatValues = currentHeartBeatValues;
                    currentHeartBeatValues = updateHearBeat(currentHeartBeatValues);

                        waitForMaster();

                    previousheartBeatValues = currentHeartBeatValues;
                    currentHeartBeatValues = readHeartBeat();
                    masterAlive = hasNodeWithHighPriorityUpdated(currentHeartBeatValues);
                    if (masterAlive) {
                        //Do nothing remain slave
                        System.out.println("Node with higher priority has updated");
                    } else {
                        System.out.println("Only I have updated");
                        MasterSlaveStateManager.masterElected();
                        updateHearBeat(currentHeartBeatValues);
                    }
                }
            }
            catch (AndesException e){
                e.printStackTrace();
                //Do nothing since already a slave
            }
        }
    }

    private void insertHeartBeat() {
        try {
            andesContextStore.insertHeartBeat(myNodeId, 1);
            System.out.println("Inserting heartbeat node id: " + myNodeId);
        } catch (AndesException e) {
            e.printStackTrace();
        }
    }

    public boolean haveIUpdated(Map<String, Integer> currentHearBeats){
        int previousHeatBeat = previousheartBeatValues.get(myNodeId);
        return (currentHearBeats.get(myNodeId) != previousHeatBeat);
    }

    public boolean hasAnyoneElseUpdated(Map<String, Integer> currentHearBeats){
        for (Map.Entry<String, Integer> entry: previousheartBeatValues.entrySet()){
            String nodeId = entry.getKey();
            if ((entry.getValue() != currentHearBeats.get(nodeId).intValue()) && (!myNodeId.equals(nodeId))){
                return true;
            }
        }
        return false;
    }

    private Map<String, Integer> updateHearBeat(Map<String, Integer> currentHeartBeats) throws AndesException {
        //return the updated heartbeat value
        int currentHeartBeat = currentHeartBeats.get(myNodeId) + 1;
        Map<String, Integer> currentValue = new HashMap<String, Integer>(currentHeartBeats);
        currentValue.put(myNodeId, currentHeartBeat);
        andesContextStore.updateHeartBeat(myNodeId, currentHeartBeat);
        System.out.println("Updated heart beat. Node id: " + myNodeId + ", heartbeat: " + currentHeartBeat);
        return currentValue;
    }

    private Map<String, Integer> readHeartBeat() throws AndesException {
        System.out.println("Reading heart beat");
        Map<String, Integer> map = andesContextStore.readHeartBeat();
        System.out.println(map);
        if (map.size() != priorities.size()){
            updateNodePriorities();
        }
        return map;
    }

    private void updateNodePriorities() throws AndesException {
        Map<String, Integer> map = andesContextStore.readNodes();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (!priorities.containsKey(entry.getKey())) {
                priorities.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public boolean hasNodeWithHighPriorityUpdated(Map<String, Integer> currentHearBeats){
        boolean updated = false;
        for (Map.Entry<String, Integer> entry: previousheartBeatValues.entrySet()){
            String nodeID = entry.getKey();
            if ((entry.getValue() != currentHearBeats.get(nodeID).intValue()) && (!nodeID.equals(myNodeId) && priorities.get(nodeID) > priorities.get(myNodeId))){
                MasterSlaveStateManager.slaveElected();
                updated = true;
                break;
            }
        }
        return updated;
    }

}

