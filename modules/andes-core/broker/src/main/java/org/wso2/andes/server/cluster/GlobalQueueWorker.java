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
package org.wso2.andes.server.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.*;

/**
 * <code>GlobalQueueWorker</code> is responsible for polling global queues and
 * distribute messages to the subscriber userQueues.
 */
public class GlobalQueueWorker implements Runnable {

    private static Log log = LogFactory.getLog(GlobalQueueWorker.class);

    private String globalQueueName;
    private boolean running;
    private int messageCountToReadFromCasssandra;
    private long lastProcessedMessageID;
    private MessageStore messageStore;


    public GlobalQueueWorker(String queueName, MessageStore messageStore,
                             int messageCountToReadFromCasssandra) {
        this.messageStore = messageStore;
        this.globalQueueName = queueName;
        this.messageCountToReadFromCasssandra = messageCountToReadFromCasssandra;
        this.lastProcessedMessageID = 0;
    }

    public void run() {
        SubscriptionStore subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        int queueWorkerWaitTime = ClusterResourceHolder.getInstance().getClusterConfiguration()
                .getQueueWorkerInterval();
        int repeatedSleepingCounter = 0;

        while (true) {
            if (running) {
                try {
                    /**
                     * Steps
                     *
                     * 1)Poll Global queue and get chunk of messages 2) Put messages
                     * one by one to node queues and delete them
                     */
                    QueueAddress sourceQueueAddress = new QueueAddress(QueueAddress.QueueType.GLOBAL_QUEUE, globalQueueName);

                    List<AndesMessageMetadata> messageList = messageStore.getNextNMessageMetadataFromQueue(sourceQueueAddress, lastProcessedMessageID, messageCountToReadFromCasssandra);
                    while (messageList.size() != 0) {
                        Iterator<AndesMessageMetadata> metadataIterator = messageList.iterator();
                        while (metadataIterator.hasNext()) {
                            AndesMessageMetadata metadata = metadataIterator.next();

                            /**
                             * check if the cluster has some subscriptions for that message and distribute to relevant node queues
                             */
                            String destinationQueue = metadata.getDestination();
                            Random random = new Random();
                            //TODO remove this list to set conversion
                            List<String> nodeQueuesHavingSubscriptionsForQueue = new ArrayList<String>(subscriptionStore.getNodeQueuesHavingSubscriptionsForQueue(destinationQueue));
                            if (nodeQueuesHavingSubscriptionsForQueue.size() > 0) {
                                int index = random.nextInt(nodeQueuesHavingSubscriptionsForQueue.size());
                                String nodeQueue = nodeQueuesHavingSubscriptionsForQueue.get(index);
                                metadata.queueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE, nodeQueue);
                                //if (log.isDebugEnabled()) {
                                String msgID = (String) metadata.getMessageHeader("msgID");
                                log.debug("TRACING>> GQW " + globalQueueName + ">> copying message-" + (msgID == null ? "" : msgID ) +
                                        " to " + nodeQueue + " message ID: " + metadata.getMessageID());
                                // }
                            } else {
                                //if there is no node queue to move message we skip
                                metadataIterator.remove();
                            }
                            lastProcessedMessageID = metadata.getMessageID();

                        }
                        messageStore.moveMessageMetaData(sourceQueueAddress, null, messageList);
                        PerformanceCounter.recordGlobalQueueMsgMove(messageList.size());
                        messageList = messageStore.getNextNMessageMetadataFromQueue(sourceQueueAddress, lastProcessedMessageID, messageCountToReadFromCasssandra);
                    }

                    try {
                        Thread.sleep(queueWorkerWaitTime);
                        repeatedSleepingCounter++;
                        if (repeatedSleepingCounter > 1) {
                            resetMessageReading();
                        }
                    } catch (InterruptedException e) {
                        // ignore
                    }
                } catch (Exception e) {
                    log.error("Error in moving messages from global queue to node queue", e);
                }
            } else {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    //silently ignore
                }
            }

        }

    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public void resetMessageReading() {
        this.lastProcessedMessageID = 0;
        if (log.isDebugEnabled()) {
            log.debug("Worker for Global Queue " + globalQueueName + " is reset");
        }
    }
}
