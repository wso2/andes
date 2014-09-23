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

package org.wso2.andes.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessageStore;

import java.util.ArrayList;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MessageContentRemoverTask implements Runnable {

    private int waitInterval = 5000;
    private volatile boolean running = true;
    private long timeOutPerMessage = 10000000000L; //10s
    private SortedMap<Long, Long> contentDeletionTasks;
    private MessageStore messageStore;
    private DurableStoreConnection connectionToMessageStore;
    private static Log log = LogFactory.getLog(MessageContentRemoverTask.class);

    public MessageContentRemoverTask(int waitInterval, ConcurrentSkipListMap<Long, Long> contentDeletionTasks, MessageStore messageStore, DurableStoreConnection connection) {
        this.waitInterval = waitInterval;
        this.contentDeletionTasks = contentDeletionTasks;
        this.messageStore = messageStore;
        this.connectionToMessageStore = connection;
    }

    public MessageContentRemoverTask(ConcurrentSkipListMap<Long, Long> contentDeletionTasks, MessageStore messageStore) {
        this.contentDeletionTasks = contentDeletionTasks;
        this.messageStore = messageStore;
    }

    public void run() {
            try {
                if (!contentDeletionTasks.isEmpty() && connectionToMessageStore.isLive()) {
                    long currentTime = System.nanoTime();

                    //remove content for timeout messages
                    SortedMap<Long, Long> timedOutContentList = contentDeletionTasks.headMap(currentTime - timeOutPerMessage);
                    try {
                        messageStore.deleteMessageParts(new ArrayList<Long>(timedOutContentList.values()));
                        for (Long key : timedOutContentList.keySet()) {
                            contentDeletionTasks.remove(key);
                        }
                    } catch (AndesException e) {
                        log.error("Error while deleting message contents", e);
                    }
                }
                try {
                    Thread.sleep(waitInterval);
                } catch (InterruptedException e) {
                    log.error(e);
                }

            } catch (Throwable e) {
                log.error("Erring in removing message content details ", e);
            }
    }

    @Deprecated
    public boolean isRunning() {
        return running;
    }

    @Deprecated
    public void start() {
        this.setRunning(true);
        Thread t = new Thread(this);
        t.setName(this.getClass().getSimpleName() + "-Thread");
        t.start();
    }

    @Deprecated
    public void setRunning(boolean running) {
        this.running = running;
    }
}
