/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.server.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.configuration.BrokerConfiguration;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;


/**
 * <code>QueueDeliveryWorker</code> Handles the task of polling the user queues and flushing the
 * messages to subscribers There will be one Flusher per Queue Per Node
 */
public class QueueDeliveryWorker {
    private final String nodeQueue;
    private boolean running = true;
    private static Log log = LogFactory.getLog(QueueDeliveryWorker.class);

    private int maxNumberOfUnAckedMessages = 100000;

    //per queue
    private int maxNumberOfReadButUndeliveredMessages = 5000;

    private SequentialThreadPoolExecutor executor;

    private final int queueWorkerWaitInterval;

    private Map<String, QueueDeliveryInfo> subscriptionCursar4QueueMap = new HashMap<String,
            QueueDeliveryInfo>();

    private SubscriptionStore subscriptionStore;

    private static QueueDeliveryWorker queueDeliveryWorker = new QueueDeliveryWorker(1000);

    public QueueDeliveryWorker(final int queueWorkerWaitInterval) {
        this.nodeQueue = MessagingEngine.getMyNodeQueueName();
        this.executor = new SequentialThreadPoolExecutor(
                (ClusterResourceHolder.getInstance().getClusterConfiguration().
                        getPublisherPoolSize()), "QueueMessagePublishingExecutor");
        this.queueWorkerWaitInterval = queueWorkerWaitInterval;

        BrokerConfiguration clusterConfiguration = ClusterResourceHolder.getInstance()
                                                                         .getClusterConfiguration();
        this.maxNumberOfUnAckedMessages = clusterConfiguration.getMaxNumberOfUnackedMessages();
        this.maxNumberOfReadButUndeliveredMessages = clusterConfiguration
                .getMaxNumberOfReadButUndeliveredMessages();
        this.subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
    }

    public boolean isWorking() {
        return running;
    }

    /**
     * Class to keep track of message delivery information queue wise
     */
    public class QueueDeliveryInfo {
        String queueName;

        Iterator<LocalSubscription> iterator;
        //in-memory message list scheduled to be delivered
        Set<AndesMessageMetadata> readButUndeliveredMessages = new
                ConcurrentSkipListSet<AndesMessageMetadata>();

        /**
         * Returns boolean variable saying whether this queue has room or not
         *
         * @return whether this queue has room or not
         */
        public boolean isMessageBufferFull() {
            boolean hasRoom = true;
            if (readButUndeliveredMessages.size() >= maxNumberOfReadButUndeliveredMessages) {
                hasRoom = false;
            }
            return hasRoom;
        }
    }

    /**
     * Get the next subscription for the given queue. If at end of the subscriptions, it circles
     * around to the first one
     *
     * @param queueName
     *         name of queue
     * @param subscriptions4Queue
     *         subscriptions registered for the queue
     * @return subscription to deliver
     * @throws AndesException
     */
    private LocalSubscription findNextSubscriptionToSent(String queueName,
                                                         Collection<LocalSubscription>
                                                                 subscriptions4Queue)
            throws AndesException {
        if (subscriptions4Queue == null || subscriptions4Queue.size() == 0) {
            subscriptionCursar4QueueMap.remove(queueName);
            return null;
        }

        QueueDeliveryInfo queueDeliveryInfo = getQueueDeliveryInfo(queueName);
        Iterator<LocalSubscription> it = queueDeliveryInfo.iterator;
        if (it.hasNext()) {
            return it.next();
        } else {
            it = subscriptions4Queue.iterator();
            queueDeliveryInfo.iterator = it;
            if (it.hasNext()) {
                return it.next();
            } else {
                return null;
            }
        }
    }


    public QueueDeliveryInfo getQueueDeliveryInfo(String queueName) throws AndesException {
        QueueDeliveryInfo queueDeliveryInfo = subscriptionCursar4QueueMap.get(queueName);
        if (queueDeliveryInfo == null) {
            queueDeliveryInfo = new QueueDeliveryInfo();
            queueDeliveryInfo.queueName = queueName;
            Collection<LocalSubscription> localSubscribersForQueue = subscriptionStore
                    .getActiveLocalSubscribers(queueName, false);
            queueDeliveryInfo.iterator = localSubscribersForQueue.iterator();
            subscriptionCursar4QueueMap.put(queueName, queueDeliveryInfo);
        }
        return queueDeliveryInfo;
    }


    public boolean isMessageBufferEmpty(String queueName) {
        return subscriptionCursar4QueueMap.get(queueName).readButUndeliveredMessages.isEmpty();
    }

    /**
     * send the messages to deliver
     *
     * @param messagesReadByLeadingThread
     *         AndesMetadata list
     * @param slot
     *         these messages are belonged to
     */
    public void sendMessageToFlusher(List<AndesMessageMetadata> messagesReadByLeadingThread,
                                     Slot slot) {

        int pendingJobsToSendToTransport;
        long failureCount = 0;
        try {
            /**
             *    Following check is to avoid the worker queue been full with too many pending tasks
             *    to send messages. Better stop buffering until we have some breathing room
             */
            pendingJobsToSendToTransport = executor.getSize();

            if (pendingJobsToSendToTransport > 1000) {
                if (pendingJobsToSendToTransport > 5000) {
                    log.error(
                            "Flusher queue is growing, and this should not happen. Please check " +
                            "cassandra Flusher");
                }
                log.warn("Flusher queue has " + pendingJobsToSendToTransport + " tasks");
                // TODO: we need to handle this. Notify slot delivery worker to sleep more
            }
            for (AndesMessageMetadata message : messagesReadByLeadingThread) {

                String queueName = message.getDestination();
                message.setSlot(slot);
                QueueDeliveryInfo queueDeliveryInfo = getQueueDeliveryInfo(queueName);
                //check and buffer message
                //stamp this message as buffered
                boolean isOKToBuffer = OnflightMessageTracker.getInstance()
                                                             .addMessageToBufferingTracker(slot,
                                                                                           message);
                log.info("buffering message" + message.getMessageID() + " queue= " + queueName);
                if (isOKToBuffer) {
                    queueDeliveryInfo.readButUndeliveredMessages.add(message);
                    //increment the message count in the slot
                    OnflightMessageTracker.getInstance().incrementMessageCountInSlot(slot);
                }
            }
            /**
             * Now messages are read to the memory. Send the read messages to subscriptions
             */
            sendMessagesInBuffer(slot.getQueueName());
            failureCount = 0;
        } catch (Throwable e) {
            /**
             * When there is a error, we will wait to avoid looping.
             */
            long waitTime = queueWorkerWaitInterval;
            failureCount++;
            long faultWaitTime = Math.max(waitTime * 5, failureCount * waitTime);
            try {
                Thread.sleep(faultWaitTime);
            } catch (InterruptedException e1) {
                //silently ignore
            }

            log.fatal("Error running Cassandra Message Flusher" + e.getMessage(), e);
        }
    }

    /**
     * Read messages from the buffer and send messages to subscribers
     */
    public void sendMessagesInBuffer(String queueName) throws AndesException {
        QueueDeliveryInfo queueDeliveryInfo = subscriptionCursar4QueueMap.get(queueName);
        if (log.isDebugEnabled()) {
            for (String queue : subscriptionCursar4QueueMap.keySet()) {
                log.debug("Queue Size of queue " + queue + " is :"
                          + subscriptionCursar4QueueMap.get(queue).readButUndeliveredMessages
                        .size());
            }

        }
        try {
            log.debug(
                    "Sending messages from buffer num of msg = " + queueDeliveryInfo
                            .readButUndeliveredMessages
                            .size());
            sendMessagesToSubscriptions(queueDeliveryInfo.queueName,
                                        queueDeliveryInfo.readButUndeliveredMessages);
        } catch (Exception e) {
            throw new AndesException("Error occurred while sending messages to subscribers " +
                                     "from message buffer" + e);
        }
    }


    private void sleep4waitInterval(long sleepInterval) {
        try {
            Thread.sleep(sleepInterval);
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * does that queue has too many messages pending
     *
     * @param localSubscription
     *         local subscription
     * @return is subscription ready to accept messages
     */
    private boolean isThisSubscriptionHasRoom(LocalSubscription localSubscription) {
        //
        int notAckedMsgCount = localSubscription.getnotAckedMsgCount();


        //Here we ignore messages that has been scheduled but not executed,
        // so it might send few messages than maxNumberOfUnAckedMessages
        if (notAckedMsgCount < maxNumberOfUnAckedMessages) {
            return true;
        } else {

            if (log.isDebugEnabled()) {
                log.debug(
                        "Not selected, channel =" + localSubscription + " pending count =" +
                        (notAckedMsgCount + executor
                                .getSize()));
            }
            return false;
        }
    }


    public int sendMessagesToSubscriptions(String targetQueue, Set<AndesMessageMetadata> messages)
            throws Exception {

        /**
         * check if this queue has any subscription
         */

        //todo return the slot
        if (subscriptionStore.getActiveClusterSubscribersForDestination(targetQueue, false)
                             .size() == 0) {
            return 0;
        }

        /**
         * deliver messages to subscriptions
         */
        int sentMessageCount = 0;
        Iterator<AndesMessageMetadata> iterator = messages.iterator();
        while (iterator.hasNext()) {
            AndesMessageMetadata message = iterator.next();
            if (MessageExpirationWorker.isExpired(message.getExpirationTime())) {
                continue;
            }
            boolean messageSent = false;
            Collection<LocalSubscription> subscriptions4Queue = subscriptionStore
                    .getActiveLocalSubscribers(targetQueue, false);
                /*
                 * we do this in a for loop to avoid iterating for a subscriptions for ever. We
                 * only iterate as
                 * once for each subscription
                 */
            for (int j = 0; j < subscriptions4Queue.size(); j++) {
                LocalSubscription localSubscription = findNextSubscriptionToSent(targetQueue,
                                                                                 subscriptions4Queue);
                if (isThisSubscriptionHasRoom(localSubscription)) {
                    iterator.remove();
                    log.debug("Scheduled to send id = " + message.getMessageID());
                    deliverAsynchronously(localSubscription, message);
                    sentMessageCount++;
                    messageSent = true;
                    break;
                }
            }
            if (!messageSent) {
                log.debug(
                        "All subscriptions for queue " + targetQueue + " have max Unacked " +
                        "messages " + message
                                .getDestination());
            }
        }
        return sentMessageCount;
    }


    private void deliverAsynchronously(final LocalSubscription subscription,
                                       final AndesMessageMetadata message) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    if (subscription.isActive()) {
                        if (MessageExpirationWorker.isExpired(message.getExpirationTime())) {
                            return;
                        }
                        (subscription).sendMessageToSubscriber(message);
                    } else {
                        reQueueUndeliveredMessagesDueToInactiveSubscriptions(message);
                    }
                } catch (Throwable e) {
                    log.error("Error while delivering message. Moving to Dead Letter Queue ", e);
                    //todo - hasitha - here we have already tried three times to deliver.
                }
            }
        };
        executor.submit(r, (subscription.getTargetQueue() + subscription.getSubscriptionID())
                .hashCode());
    }

    public void reQueueUndeliveredMessagesDueToInactiveSubscriptions(AndesMessageMetadata message) {
        String queueName = message.getDestination();
        subscriptionCursar4QueueMap.get(queueName).readButUndeliveredMessages.add(message);
    }

    public void stopFlusher() {
        running = false;
        log.debug("Shutting down the queue message flusher for the queue " + nodeQueue);
    }

    public void clearMessagesAccumilatedDueToInactiveSubscriptionsForQueue(
            String destinationQueueName) throws AndesException {
        getQueueDeliveryInfo(destinationQueueName).readButUndeliveredMessages.clear();
    }

    public static QueueDeliveryWorker getInstance() {
        return queueDeliveryWorker;
    }


}