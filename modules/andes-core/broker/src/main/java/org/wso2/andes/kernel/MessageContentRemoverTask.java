/*
 * Copyright (c) 2005-2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * 
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;

/**
 * This class is used as a task to delete message content at scheduled period
 */
public class MessageContentRemoverTask implements Runnable, StoreHealthListener {

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
     * Indicates that message store operational
     */
    private volatile AtomicBoolean storeOperational;

    /**
     * Setup the content deletion task with the reference to MessageStore and
     * DurableStoreConnection to message store
     * 
     * @param messageStore
     *            MessageStore
     */
    public MessageContentRemoverTask(MessageStore messageStore) {
        this.messageStore = messageStore;
        messageIdToDeleteQueue = new LinkedBlockingDeque<Long>();
        storeOperational = new AtomicBoolean(true);
        FailureObservingStoreManager.registerStoreHealthListener(this);
    }

    /**
     * {@inheritDoc}
     * If the message/context stores are 
     */
    public void run() {
        if (storeOperational.compareAndSet(true, true) && (!messageIdToDeleteQueue.isEmpty())) {

            int queueCount = messageIdToDeleteQueue.size();
            List<Long> idList = new ArrayList<Long>(queueCount);

            try {
                messageIdToDeleteQueue.drainTo(idList);
                // Remove from the deletion task map
                messageStore.deleteMessageParts(idList);
                if (log.isDebugEnabled()) {
                    log.debug("Message content removed of " + idList.size() + " messages.");
                }

            } catch (Throwable e) {
                // Reason: we want to ensure that this
                // scheduled task never get suppressed due any errors
                // (executor service will not run subsequent runs if current
                // throws errors)
                log.error("Error in removing message content details ", e);

                /*
                 * since we failed to delete some message
                 * parts we will try those in next turn.
                 */
                messageIdToDeleteQueue.addAll(idList);
            }

        }

    }

    /**
     * Data is put into the concurrent skip list map for deletion
     * 
     * @param messageId
     *            message id of the content
     */
    public void put(Long messageId) {
        messageIdToDeleteQueue.add(messageId);
    }

    /**
     * {@inheritDoc}
     * <p>
     * A atomic boolean flag is set to false. indicating that message stores
     * went offline.
     * 
     */
    @Override
    public void storeNonOperational(HealthAwareStore store, Exception ex) {
        log.info(String.format("Message store became inoperational. Number of message-contents to be deleted: %d",
                               messageIdToDeleteQueue.size()));
        storeOperational.set(false);
    }

    /**
     * {@inheritDoc}
     * <p>
     * A atomic boolean flag is set to true, indicating that message stores
     * became online, allowing this periodic task to delete message contents.
     */
    @Override
    public void storeOperational(HealthAwareStore store) {
        log.info(String.format("Message store became operational. Number of message-contents to be deleted: %d",
                               messageIdToDeleteQueue.size()));
        storeOperational.set(true);
    }
}
