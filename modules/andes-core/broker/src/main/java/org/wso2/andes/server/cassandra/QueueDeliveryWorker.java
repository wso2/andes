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

package org.wso2.andes.server.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.configuration.ClusterConfiguration;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * QueueDeliveryWorker Handles the task of polling the user queues and flushing
 * the messages to subscribers
 * There will be one Flusher per Queue Per Node
 */
public class QueueDeliveryWorker {
    private final String nodeQueue;
    private static Log log = LogFactory.getLog(QueueDeliveryWorker.class);

    private int maxNumberOfUnAckedMessages = 100000;
    //per queue
    private int maxNumberOfReadButUndeliveredMessages = 5000;

    private static final int THREADPOOL_RECOVERY_INTERVAL = 2000;
    private static final int SAFE_THREAD_COUNT = 300;

    // private long lastProcessedId = 0;

    // private long totMsgSent = 0;
    //  private long totMsgRead = 0;

    private static final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private SequentialThreadPoolExecutor executor;
    private Map<String, QueueDeliveryInfo> subscriptionCursar4QueueMap = new HashMap<String, QueueDeliveryInfo>();
    private SubscriptionStore subscriptionStore;

    private OnflightMessageTracker onflightMessageTracker;


    private static QueueDeliveryWorker queueDeliveryWorker = new QueueDeliveryWorker();

    public QueueDeliveryWorker() {
        this.nodeQueue = MessagingEngine.getMyNodeQueueName();
        this.executor = new SequentialThreadPoolExecutor((ClusterResourceHolder.getInstance().getClusterConfiguration().
                getPublisherPoolSize()), "QueueMessagePublishingExecutor");
        ClusterConfiguration clusterConfiguration = ClusterResourceHolder.getInstance().getClusterConfiguration();
        this.maxNumberOfUnAckedMessages = clusterConfiguration.getMaxNumberOfUnackedMessages();
        this.maxNumberOfReadButUndeliveredMessages = clusterConfiguration.getMaxNumberOfReadButUndeliveredMessages();
        this.subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        this.onflightMessageTracker = OnflightMessageTracker.getInstance();

        log.info("Queue worker started Listening for " + nodeQueue + " with on flight message checks");
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
     * Get the next subscription for the given queue. If at end of the subscriptions, it circles around to the first one
     *
     * @param queueName           name of queue
     * @param subscriptions4Queue subscriptions registered for the queue
     * @return subscription to deliver
     * @throws AndesException
     */
    private LocalSubscription findNextSubscriptionToSent(String queueName,
                                                         Collection<LocalSubscription>
                                                                 subscriptions4Queue) throws AndesException {
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
            Collection<LocalSubscription> localSubscribersForQueue = subscriptionStore.getActiveLocalSubscribers(queueName, false);
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
     * @param messagesReadByLeadingThread AndesMetadata list
     * @param slot                        these messages are belonged to
     */
    public void sendMessageToFlusher(List<AndesMessageMetadata> messagesReadByLeadingThread,
                                     Slot slot) {

        int workqueueSize;
        long lastProcessedId = 0;
        try {
            /**
             *    Following check is to avoid the worker queue been full with too many pending tasks.
             *    those pending tasks are best left in Cassandra until we have some breathing room
             */
            workqueueSize = executor.getSize();

            if (workqueueSize > 1000) {
                if (workqueueSize > 5000) {
                    log.error("Flusher queue is growing, and this should not happen. Please check cassandra Flusher");

                    // Must give some time for the threads to clean up. Thus, following conditional loop.
                    // Once the thread count is below 300, other tasks can resume. Until then this worker is held hostage with <THREADPOOL_RECOVERY_INTERVAL> millisecond sleeps.
                    while(executor.getSize() > SAFE_THREAD_COUNT) {
                        log.info("Current Threadpool size : " + executor.getSize());
                        Thread.sleep(THREADPOOL_RECOVERY_INTERVAL);
                        log.info("Current Threadpool size after sleep: " + executor.getSize());
                    }
                }
                log.warn("skipping content cassandra reading thread as flusher queue has " + workqueueSize + " tasks");
            }

            /**
             * Following reads from message store, it reads only if there are not enough messages loaded in memory
             */
            List<AndesMessageMetadata> messagesFromMessageStore;
            messagesFromMessageStore = new ArrayList<AndesMessageMetadata>();
            for (AndesMessageMetadata message : messagesReadByLeadingThread) {
                Long messageID = message.getMessageID();
                if (onflightMessageTracker.testMessage((message.getMessageID()))) {
                    messagesFromMessageStore.add(message);
                }
                if (log.isTraceEnabled()) {
                    log.trace("TRACING>> CMS>> read from message store " + message
                            .getMessageID());
                }
                lastProcessedId = messageID;
            }

            if (log.isDebugEnabled()) {
                log.debug("QDW >> Number of messages read from " + nodeQueue + " with last processed ID " + lastProcessedId + " is  = " + messagesFromMessageStore.size());
            }
            for (AndesMessageMetadata message : messagesFromMessageStore) {
                String queueName = message.getDestination();
                message.setSlot(slot);
                QueueDeliveryInfo queueDeliveryInfo = getQueueDeliveryInfo(queueName);
                queueDeliveryInfo.readButUndeliveredMessages.add(message);
                //increment the message count in the slot
                OnflightMessageTracker.getInstance().incrementMessageCountInSlot(slot);
            }
            //Send messages in QueueDeliveryInfo message list
            sendMessagesInBuffer(slot.getQueueName());

        } catch (Throwable e) {
            //todo handle
            log.fatal("Error running Cassandra Message Flusher" + e.getMessage(), e);
        }
    }

    /**
     * Read messages from the buffer and send messages to subscribers. THis buffer stays in
     * QueueDeliveryInfo objects for each queue.
     */
    public void sendMessagesInBuffer(String queueName) throws AndesException {

        QueueDeliveryInfo queueDeliveryInfo = subscriptionCursar4QueueMap.get(queueName);
        if (log.isDebugEnabled()) {
            for (String queue : subscriptionCursar4QueueMap.keySet()) {
                log.debug("Queue Size of queue " + queue + " is :"
                        + subscriptionCursar4QueueMap.get(queue).readButUndeliveredMessages.size());
            }
        }
        try {
            sendMessagesToSubscriptions(queueDeliveryInfo.queueName,
                    queueDeliveryInfo.readButUndeliveredMessages);
        } catch (Exception e) {
            throw new AndesException("Error occurred while sending messages to subscribers " +
                    "from message buffer" + e);
        }
    }

    /**
     * Does that queue has too many messages pending
     *
     * @param localSubscription local subscription
     * @return is subscription ready to accept messages
     */
    private boolean isThisSubscriptionHasRoom(LocalSubscription localSubscription) {

        int notAckedMsgCount = localSubscription.getnotAckedMsgCount();
        //Here we ignore messages that has been scheduled but not executed, so it might send few messages than maxNumberOfUnAckedMessages
        if (notAckedMsgCount < maxNumberOfUnAckedMessages) {
            return true;
        } else {

            if (log.isDebugEnabled()) {
                log.debug("Not selected, channel =" + localSubscription + " pending count =" + (notAckedMsgCount + executor.getSize()));
            }
            return false;
        }
    }

    /**
     * Check whether there are active subscribers and send
     *
     * @param targetQueue queue name
     * @param messages    metadata set
     * @return how many messages sent
     * @throws Exception
     */
    public int sendMessagesToSubscriptions(String targetQueue, Set<AndesMessageMetadata> messages)
            throws Exception {

        /**
         * Check if this queue has any subscription
         */
        if (subscriptionStore.getActiveClusterSubscribersForDestination(targetQueue, false).size() == 0) {
            return 0;
        }

        /**
         * Deliver messages to subscriptions
         */
        int sentMessageCount = 0;
        Iterator<AndesMessageMetadata> iterator = messages.iterator();
        while (iterator.hasNext()) {

            try {
                AndesMessageMetadata message = iterator.next();

                if (MessageExpirationWorker.isExpired(message.getExpirationTime())) {
                    continue;
                }

                boolean messageSent = false;
                Collection<LocalSubscription> subscriptions4Queue = subscriptionStore
                        .getActiveLocalSubscribers(targetQueue, false);
                    /*
                     * We do this in a for loop to avoid iterating for a subscriptions for ever. We
                     * only iterate as
                     * once for each subscription
                     */
                for (int j = 0; j < subscriptions4Queue.size(); j++) {
                    LocalSubscription localSubscription = findNextSubscriptionToSent(targetQueue, subscriptions4Queue);
                    if (isThisSubscriptionHasRoom(localSubscription)) {
                        iterator.remove();
                        deliverAsynchronously(localSubscription, message);
                        sentMessageCount++;
                        messageSent = true;
                        break;
                    }
                }
                if (!messageSent) {
                    log.debug("All subscriptions for queue " + targetQueue + " have max Unacked messages " + message.getDestination());
                }
            } catch (NoSuchElementException ex) {
                //This exception can occur because the iterator of ConcurrentSkipListSet loads the at-the-time snapshot.
                // Some records could be deleted by the time the iterator reaches them.
                // However, this can only happen at the tail of the collection, not in middle, and it would cause the loop
                // to blindly check for a batch of deleted records.
                // Given this situation, this loop should break so the sendFlusher can re-trigger it.
                //for tracing purposes can use this : log.warn("NoSuchElementException thrown",ex);
                break;
            }
        }
        return sentMessageCount;
    }

    /**
     * Submit the messages to a thread pool to deliver asynchronously
     *
     * @param subscription local subscription
     * @param message      metadata of the message
     */
    private void deliverAsynchronously(final LocalSubscription subscription, final AndesMessageMetadata message) {
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
        executor.submit(r, (subscription.getTargetQueue() + subscription.getSubscriptionID()).hashCode());
    }

    /**
     * Send the messages back to the buffer in QueueDeliveryInfo object due to inactive
     * subscribers.
     *
     * @param message metadata
     */
    public void reQueueUndeliveredMessagesDueToInactiveSubscriptions(AndesMessageMetadata message) {
        String queueName = message.getDestination();
        subscriptionCursar4QueueMap.get(queueName).readButUndeliveredMessages.add(message);
    }

    /**
     * This method will be called when message purging is initiated. Clear the messages in
     * QueueDeliveryInfo buffer
     *
     * @param destinationQueueName queue name
     * @throws AndesException
     */
    public void clearMessagesAccumilatedDueToInactiveSubscriptionsForQueue(String destinationQueueName) throws AndesException {
        getQueueDeliveryInfo(destinationQueueName).readButUndeliveredMessages.clear();
    }

    public static QueueDeliveryWorker getInstance() {
        return queueDeliveryWorker;
    }


}