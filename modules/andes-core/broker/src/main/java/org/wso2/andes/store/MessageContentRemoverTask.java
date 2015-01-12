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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessageStore;

import java.util.*;
import java.util.concurrent.*;

/**
 * This class is used as a task to delete message content at scheduled period
 */
public class MessageContentRemoverTask implements Runnable {

    private static Log log = LogFactory.getLog(MessageContentRemoverTask.class);

    /**
     * Reference to message store
     */
    private final MessageStore messageStore;

    /**
     * To be deleted content
     */
    private final BlockingDeque<Long> messageIdToDeleteQueue;

    /**
     * Setup the content deletion task with the reference to MessageStore and
     * DurableStoreConnection to message store
     * @param messageStore MessageStore
     */
    public MessageContentRemoverTask(MessageStore messageStore) {
        this.messageStore = messageStore;
        messageIdToDeleteQueue = new LinkedBlockingDeque<Long>();
    }

    public void run() {
        try {
            if (!messageIdToDeleteQueue.isEmpty()) {

                try {
                    int queueCount = messageIdToDeleteQueue.size();
                    List<Long> idList = new ArrayList<Long>(queueCount);
                    messageIdToDeleteQueue.drainTo(idList);
                    // Remove from the deletion task map
                    messageStore.deleteMessageParts(idList);
                    if (log.isDebugEnabled()) {
                        log.debug("Message content removed of " + idList.size() + " messages.");
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
     * @param messageId message id of the content
     */
    public void put(Long messageId) {
        messageIdToDeleteQueue.add(messageId);
    }
}
