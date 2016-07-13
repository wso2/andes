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
package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.slot.SlotManagerClusterMode;
import org.wso2.andes.kernel.slot.SlotManagerStandalone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * This thread will keep looking for expired messages within the broker and remove them.
 */
public class MessageExpirationWorker extends Thread {

    private static Log log = LogFactory.getLog(MessageExpirationWorker.class);
    private volatile boolean working = false;
    /**
     * hold the expired messages detected in the message flusher delivery rule check for batch delete
     */
    public static Set<DeliverableAndesMetadata> expiredMessageSet = new HashSet<>();

    /**
     * Used to perform database operations on the context store.
     */
    private AndesContextStore andesContextStore = AndesContext.getInstance().getAndesContextStore();

    //configurations
    private final Integer workerWaitInterval;
    private final Integer safetySlotCount;
    private final Integer messageBatchSize;
    private final Boolean saveExpiredToDLC;

    public MessageExpirationWorker() {

        workerWaitInterval = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_MESSAGE_EXPIRATION_CHECK_INTERVAL);
        messageBatchSize = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_MESSAGE_EXPIRATION_BATCH_SIZE);
        saveExpiredToDLC = AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_AMQP_SEND_EXPIRED_MESSAGES_TO_DLC);
        safetySlotCount = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SAFE_DELETE_REGION_SLOT_COUNT);

        this.start();
    }

    @Override
    public void run() {

        //int failureCount = 0;

        // The purpose of the "while true" loop here is to ensure that once the worker is started, it will verify the "working" volatile variable by itself
        // and be able to wake up if the working state is changed to "false" and then "true".
        // If we remove the "while (true)" part, we will need to re-initialize the MessageExpirationChecker from various methods outside
        // once we set the "working" variable to false (since the thread will close).
        while (true) {
            if (working) {
                try {

                    //delete the accumulated messages in the list that are filtered out from the message flusher expiration rule
                    //should be run in all the nodes
                    if (!expiredMessageSet.isEmpty()) {
                        MessagingEngine.getInstance().deleteMessages(new ArrayList<AndesMessageMetadata>(expiredMessageSet));
                    }

                    boolean isClusterEnabled = AndesContext.getInstance().isClusteringEnabled();

                    if ( isClusterEnabled && AndesContext.getInstance().getClusterAgent().isCoordinator()) {
                        //only run in coordinator node
                        Set<String> queues = andesContextStore.getAllQueues();

                        for (String queueName : queues) {
                            //get the upper bound messageID for each unassigned slots as a set for the specific queue
                            TreeSet<Long> messageIDSet = andesContextStore.getMessageIds(queueName);

                            /**
                             * deletion task run only if there are more messages than the safety slot count upper bound
                             * otherwise deletion task is not run
                             * minimum safetySlotCount is 1
                             */
                            if (safetySlotCount >= 1 && messageIDSet.size() >= safetySlotCount) {

                                //set the lower bound Id for safety delete region as the safety slot count interval upper bound id + 1
                                long currentDeletionRangeLowerBoundId = messageIDSet.
                                        toArray(new Long[messageIDSet.size()])[safetySlotCount - 1] + 1;

                                //get the expired messages for that queue in the range of message ID starting form the lower bound ID
                                List<AndesMessageMetadata> expiredMessages = MessagingEngine.getInstance()
                                        .getExpiredMessages(currentDeletionRangeLowerBoundId, queueName);

                                if (null != expiredMessages && !expiredMessages.isEmpty()) {

                                    //in order to stop the slot delivery worker to this queue in the deletion range
                                    SlotManagerClusterMode.setCurrentDeletionQueue(queueName);
                                    SlotManagerClusterMode.setCurrentDeletionRangelowerboundID(currentDeletionRangeLowerBoundId);
                                    SlotManagerStandalone.setCurrentDeletionQueue(queueName);
                                    SlotManagerStandalone.setCurrentDeletionRangelowerboundID(currentDeletionRangeLowerBoundId);

                                    //delete message metadata, content from the db
                                    MessagingEngine.getInstance().deleteMessages(expiredMessages);

                                    //since the deletion task is finished that queue no need to hold the slot delivery worker for that queue
                                    SlotManagerClusterMode.setCurrentDeletionQueue("");
                                    SlotManagerClusterMode.setCurrentDeletionRangelowerboundID(0L);
                                    SlotManagerStandalone.setCurrentDeletionQueue("");
                                    SlotManagerStandalone.setCurrentDeletionRangelowerboundID(0L);

                                    if (log.isDebugEnabled()) {
                                        log.debug("Expired message count for queue : " + queueName + "is" + expiredMessages.size());
                                    }

                                }

                            }

                        }

                    }
                    //sleep the message expiration worker for specified amount of time
                    sleepForWaitInterval(workerWaitInterval);

                    /*
                    //Get Expired message IDs from the database with the massageBatchSize as the limit
                    // we cannot delegate a cascaded delete to cassandra since it doesn't maintain associations between column families.
                    List<AndesMessageMetadata> expiredMessages = MessagingEngine.getInstance().getExpiredMessages(messageBatchSize);

                    if (expiredMessages == null || expiredMessages.size() == 0 )  {
                        sleepForWaitInterval(workerWaitInterval);
                    } else {

                        if (log.isDebugEnabled()) {
                            log.debug("Expired message count : " + expiredMessages.size());
                        }

                        if (log.isTraceEnabled()) {

                            String messagesQueuedForExpiry = "";

                            for (AndesMessageMetadata arm : expiredMessages) {
                                messagesQueuedForExpiry += arm.getMessageID() + ",";
                            }
                            log.trace("Expired messages queued for deletion : " + messagesQueuedForExpiry);
                        }

                        Andes.getInstance().deleteMessages(expiredMessages, saveExpiredToDLC);
                        sleepForWaitInterval(workerWaitInterval);

                        // Note : We had a different alternative to employ cassandra column level TTLs to automatically handle
                        // deletion of expired message references. But since we need to abstract database specific logic to
                        // support different data models (like RDBMC) in future, the above approach is followed.
                    } */

                } catch (AndesException e) {
                    log.error("Error running Message Expiration Checker " + e.getMessage(), e);
                    // The wait time here is designed to increase per failure to avoid unnecessary attempts to wake up the thread.
                    // However, given that the most probable error here could be a timeout during the database call, it could recover in the next few attempts.
                    // Therefore, no need to keep on delaying the worker.
                    // So the maximum interval between the startup attempt will be 5 * regular wait time.
                    /*
                    long waitTime = workerWaitInterval;
                    failureCount++;
                    long faultWaitTime = Math.max(waitTime * 5, failureCount * waitTime);
                    try {
                        Thread.sleep(faultWaitTime);
                    } catch (InterruptedException ignore) {
                        //silently ignore
                    } */

                }
            } else {
                sleepForWaitInterval(workerWaitInterval);
            }
        }
    }

    /**
     * get if Message Expiration Worker is active
     *
     * @return isWorking
     */
    public boolean isWorking() {
        return working;
    }

    /**
     * set Message Expiration Worker active
     */
    public void startWorking() {
        if (log.isDebugEnabled()) {
            log.debug("Starting message expiration checker.");
        }
        working = true;
    }

    /**
     * Stop Message expiration Worker
     */
    public void stopWorking() {
        if (log.isDebugEnabled()) {
            log.debug("Shutting down message expiration checker.");
        }
        working = false;
    }

    /**
     * To sleep the the worker for specified interval of time
     * @param sleepInterval
     */
    private void sleepForWaitInterval(int sleepInterval) {
        try {
            Thread.sleep(sleepInterval);
        } catch (InterruptedException ignore) {
            //ignored
        }
    }

    /*
    public static boolean isExpired(Long msgExpiration) {
        if (msgExpiration > 0) {
            return (System.currentTimeMillis() > msgExpiration);
        } else {
            return false;
        }
    } */
}
