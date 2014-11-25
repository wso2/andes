/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.store;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.server.ClusterResourceHolder;

import java.util.ArrayList;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * This class is used as a task to delete message content at scheduled period
 */
public class MessageContentRemoverTask implements Runnable {

    private static Log log = LogFactory.getLog(MessageContentRemoverTask.class);

    /**
     * To be deleted contents map
     */
    private SortedMap<Long, Long> contentDeletionTasksMap;

    /**
     * Reference to message store
     */
    private MessageStore messageStore;

    /**
     * Waiting time for message content removal of an acked message
     */
    private final long timeOutPerMessage;

    /**
     * Setup the content deletion task with the reference to MessageStore and
     * DurableStoreConnection to message store
     * @param messageStore MessageStore
     */
    public MessageContentRemoverTask(MessageStore messageStore) throws AndesException {
        this.contentDeletionTasksMap = new ConcurrentSkipListMap<Long, Long>();
        this.messageStore = messageStore;

        Integer contentRemovalTimeDifference = AndesConfigurationManager.getInstance()
                .readConfigurationValue(AndesConfiguration.PERFORMANCE_TUNING_DELETION_CONTENT_REMOVAL_TIME_DIFFERENCE);
        // Convert to nanoseconds, since contentRemovalTimeDifference is in milliseconds
        timeOutPerMessage = contentRemovalTimeDifference * 1000000L;
    }

    public void run() {
            try {
                if (!contentDeletionTasksMap.isEmpty()) {
                    long currentTime = System.nanoTime();

                    //remove content for timeout messages
                    SortedMap<Long, Long> timedOutContentList = contentDeletionTasksMap.headMap(currentTime - timeOutPerMessage);
                    try {
                        messageStore.deleteMessageParts(new ArrayList<Long>(timedOutContentList.values()));

                        // remove from the deletion task map
                        for (Long key : timedOutContentList.keySet()) {
                            contentDeletionTasksMap.remove(key);
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("Message content removed of " + timedOutContentList.size()
                                            + " messages.");
                        }
                    } catch (AndesException e) {
                        log.error("Error while deleting message contents", e);
                    }
                }
            } catch (Throwable e) {
                log.error("Error in removing message content details ", e);
            }
    }

    /**
     * Data is put into the concurrent skip list map for deletion
     * @param currentNanoTime current time in nano seconds
     * @param messageId message id of the content
     */
    public void put(Long currentNanoTime, Long messageId) {
        contentDeletionTasksMap.put(currentNanoTime, messageId);
    }
}
