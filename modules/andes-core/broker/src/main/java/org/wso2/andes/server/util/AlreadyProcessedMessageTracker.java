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

package org.wso2.andes.server.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.SortedMap;
import java.util.concurrent.*;

public class AlreadyProcessedMessageTracker {

    private static Log log = LogFactory.getLog(AlreadyProcessedMessageTracker.class);

    private ConcurrentHashMap<Long,Object> trackingMessageIDs = new ConcurrentHashMap<Long, Object>();
    private SortedMap<Long,Long> trackingMessagesRemovalTasks = new ConcurrentSkipListMap<Long, Long>();
    private static final ScheduledExecutorService alreadyProcessedMessageIDsRemovingScheduler = Executors.newSingleThreadScheduledExecutor();
    private long timeOutPerMessage = 15000000000L;
    private long trackingMessagesRemovalTaskIntervalInSec = 10;
    private String identifier = "";

    public AlreadyProcessedMessageTracker (String identifier, long trackingTimeOut, long trackingMessagesRemovalTaskIntervalInSec) {
        this.identifier = identifier;
        this.timeOutPerMessage = trackingTimeOut;
        this.trackingMessagesRemovalTaskIntervalInSec = trackingMessagesRemovalTaskIntervalInSec;
        startRemovingProcessedMessageIDS();
    }

    public void insertMessageForTracking(long messageID, Object value) {
        log.debug("inserting message id " + messageID + " for tracking. identifier=" + identifier);
        trackingMessageIDs.put(messageID , value);
    }

    public boolean checkIfAlreadyTracked(long messageID) {
       boolean result;
       if(trackingMessageIDs.get(messageID) != null) {
           result =  true;
       }  else {
           result = false;
       }
        log.debug("checking if already tracked message ID: " + messageID + " result = " + result  + "identifier=" + identifier);
        return result;
    }

    public Object removeMessageFromTracking(long messageID) {
         log.debug("removing message id " + messageID + " from tracking. identifier=" + identifier);
         return trackingMessageIDs.remove(messageID);
    }

    public void clearAllMessagesFromTracking() {
        log.debug("clearing all messages from tracking. identifier=" + identifier);
        trackingMessageIDs.clear();
    }

    public void addTaskToRemoveMessageFromTracking(long messageID) {
        trackingMessagesRemovalTasks.put(System.nanoTime(), messageID);
    }

    public void shutDownMessageTracker() {
        log.debug("shutting down message tracker. identifier=" + identifier);
        trackingMessageIDs.clear();
        alreadyProcessedMessageIDsRemovingScheduler.shutdown();
    }

    private void startRemovingProcessedMessageIDS() {
        alreadyProcessedMessageIDsRemovingScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                while (!trackingMessagesRemovalTasks.isEmpty()) {
                    long currentTime = System.nanoTime();
                    SortedMap<Long, Long> timedOutContentList = trackingMessagesRemovalTasks.headMap(currentTime - timeOutPerMessage);
                    for (Long key : timedOutContentList.keySet()) {
                        long msgid = trackingMessagesRemovalTasks.get(key);
                        trackingMessageIDs.remove(msgid);
                        log.debug("cleared message from tracking message id "+ msgid +"identifier=" + identifier);
                        trackingMessagesRemovalTasks.remove(key);
                    }
                }
            }
        }, 5, trackingMessagesRemovalTaskIntervalInSec , TimeUnit.SECONDS);
    }
}
