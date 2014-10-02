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
import org.wso2.andes.server.configuration.ClusterConfiguration;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * <code>QueueDeliveryWorker</code> Handles the task of polling the user queues and flushing
 * the messages to subscribers
 * There will be one Flusher per Queue Per Node
 */
public class QueueDeliveryWorker {
    private final String nodeQueue;
    private boolean running = true;
    private static Log log = LogFactory.getLog(QueueDeliveryWorker.class);

    private int maxNumberOfUnAckedMessages = 20000;
    //per queue
    private int maxNumberOfReadButUndeliveredMessages = 5000;

    private long lastProcessedId = 0;

    private long totMsgSent = 0;
    private long totMsgRead = 0;
    private int totalReadButUndeliveredMessages = 0;

    private long lastRestTime = 0;

    private SequentialThreadPoolExecutor executor;

    private final int queueWorkerWaitInterval;

    private long iterations = 0;
    private int workqueueSize = 0;

    private Map<String, QueueDeliveryInfo> subscriptionCursar4QueueMap = new HashMap<String, QueueDeliveryInfo>();

    private long failureCount = 0;

    private SubscriptionStore subscriptionStore;

    private OnflightMessageTracker onflightMessageTracker;


    private static QueueDeliveryWorker queueDeliveryWorker = new QueueDeliveryWorker(1000, false);

    //messages read by Laggards thread
    ConcurrentLinkedQueue<AndesMessageMetadata> laggardsMessages = new ConcurrentLinkedQueue<AndesMessageMetadata>();

    //map to keep undelivered messages due to inactive subscribers
    private ConcurrentHashMap<String, ArrayList<AndesMessageMetadata>> undeliveredMessagesMap = new ConcurrentHashMap<String, ArrayList<AndesMessageMetadata>>();

    public QueueDeliveryWorker(final int queueWorkerWaitInterval, boolean isInMemoryMode) {
        this.nodeQueue = MessagingEngine.getMyNodeQueueName();
        this.executor = new SequentialThreadPoolExecutor((ClusterResourceHolder.getInstance().getClusterConfiguration().
                getPublisherPoolSize()), "QueueMessagePublishingExecutor");
        this.queueWorkerWaitInterval = queueWorkerWaitInterval;

        ClusterConfiguration clusterConfiguration = ClusterResourceHolder.getInstance().getClusterConfiguration();
        this.maxNumberOfUnAckedMessages = clusterConfiguration.getMaxNumberOfUnackedMessages();
        this.maxNumberOfReadButUndeliveredMessages = clusterConfiguration.getMaxNumberOfReadButUndeliveredMessages();
        this.subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        this.onflightMessageTracker = OnflightMessageTracker.getInstance();

        log.info("Queue worker started Listening for " + nodeQueue + " with on flight message checks");
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
        List<AndesMessageMetadata> readButUndeliveredMessages = new ArrayList<AndesMessageMetadata>();

        /**
         * Returns boolean variable saying whether this queue has room or not
         *
         * @return whether this queue has room or not
         */
        public boolean hasRoom() {
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
    private LocalSubscription findNextSubscriptionToSent(String queueName, Collection<LocalSubscription> subscriptions4Queue) throws AndesException {
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
     *  send the messages to deliver
     * @param messagesReadByLeadingThread  AndesMetadata list
     * @param slot these messages are belonged to
     */
    public void sendMessageToFlusher(List<AndesMessageMetadata> messagesReadByLeadingThread,
                                     Slot slot) {

        iterations = 0;
        workqueueSize = 0;
        lastRestTime = System.currentTimeMillis();
        failureCount = 0;
        try {
            /**
             *    Following check is to avoid the worker queue been full with too many pending tasks.
             *    those pending tasks are best left in Cassandra until we have some breathing room
             */
            workqueueSize = executor.getSize();

            if (workqueueSize > 1000) {
                if (workqueueSize > 5000) {
                    log.error("Flusher queue is growing, and this should not happen. Please check cassandra Flusher");
                }
                log.info("skipping content cassandra reading thread as flusher queue has " + workqueueSize + " tasks");
                sleep4waitInterval(queueWorkerWaitInterval);
            }

            /**
             * Following reads from message store, it reads only if there are not enough messages loaded in memory
             */
            int msgReadThisTime = 0;
            List<AndesMessageMetadata> messagesFromMessageStore;
            messagesFromMessageStore = new ArrayList<AndesMessageMetadata>();
            for (AndesMessageMetadata message : messagesReadByLeadingThread) {
                Long messageID = message.getMessageID();
                if (!onflightMessageTracker.checkIfAlreadyReadFromNodeQueue(message.getMessageID())) {
                    onflightMessageTracker.markMessageAsReadFromNodeQueue(messageID);
                    if (log.isDebugEnabled()) {
                        log.debug("TRACING>> QDW------Adding " + messageID + " From Leading Thread to Deliver");
                    }
                    messagesFromMessageStore.add(message);
                }
                lastProcessedId = messageID;
            }

            if (log.isDebugEnabled()) {
                log.debug("QDW >> Number of messages read from " + nodeQueue + " with last processed ID " + lastProcessedId + " is  = " + messagesFromMessageStore.size());
            }
            for (AndesMessageMetadata message : messagesFromMessageStore) {

                /**
                 * If this is a message that had sent already, just drop them.
                 */
                if (!onflightMessageTracker.testMessage(message.getMessageID())) {
                    continue;
                }

                String queueName = message.getDestination();
                message.setSlot(slot);
                QueueDeliveryInfo queueDeliveryInfo = getQueueDeliveryInfo(queueName);
                queueDeliveryInfo.readButUndeliveredMessages.add(message);
                //increment the message count in the slot
                OnflightMessageTracker.getInstance().incrementMessageCountInSlot(slot);
            }
            /**
             * If no messages to read sleep more
             */
            if (messagesFromMessageStore.size() == 0) {
                sleep4waitInterval(queueWorkerWaitInterval);
            }
            totMsgRead = totMsgRead + messagesFromMessageStore.size();
            msgReadThisTime = messagesFromMessageStore.size();

            /**
             * Now messages are read to the memory. Send the read messages to subscriptions
             */
            int sentMessageCount = 0;

            sendMessagesInBuffer();

            if (iterations % 20 == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("[Flusher" + this + "]readNow=" + msgReadThisTime + " totRead=" +
                            totMsgRead + " totprocessed= " + totMsgSent + ", totalReadButNotSent=" +
                            totalReadButUndeliveredMessages + ". workQueue= " + workqueueSize + " lastID=" + lastProcessedId);
                }
            }
            iterations++;

            //on every 10th, we sleep a bit to give cassandra a break, we do the same if we have not sent any messages
            if (sentMessageCount == 0 || iterations % 10 == 0) {
                sleep4waitInterval(queueWorkerWaitInterval);
            }
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
            log.error("Error running Cassandra Message Flusher" + e.getMessage(), e);
        }
    }

    /**
     *Read messages from the buffer and send messages to subscribers
     */
    public void sendMessagesInBuffer() throws AndesException {
        //todo stop iterate through all the queues
        for (QueueDeliveryInfo queueDeliveryInfo : subscriptionCursar4QueueMap.values()) {
            if (log.isDebugEnabled()) {
                log.debug("TRACING>> delivering read but undelivered message list with size: " +
                        queueDeliveryInfo.readButUndeliveredMessages.size());
            }
            try {
                sendMessagesToSubscriptions(queueDeliveryInfo.queueName,
                        queueDeliveryInfo.readButUndeliveredMessages);
            } catch (Exception e) {
                throw new AndesException("Error occurred while sending messages to subscribers " +
                        "from message buffer" + e);
            }
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
     * @param localSubscription local subscription
     * @return is subscription ready to accept messages
     */
    private boolean isThisSubscriptionHasRoom(LocalSubscription localSubscription) {
        //
        int notAckedMsgCount = localSubscription.getnotAckedMsgCount();

        //Here we ignore messages that has been scheduled but not executed, so it might send few messages than maxNumberOfUnAckedMessages
        if (notAckedMsgCount < maxNumberOfUnAckedMessages) {
            return true;
        } else {

            if (log.isDebugEnabled()) {
                log.debug("Not selected, channel =" + localSubscription + " pending count =" + (notAckedMsgCount + workqueueSize));
            }
            return false;
        }
    }


    public int sendMessagesToSubscriptions(String targetQueue, List<AndesMessageMetadata> messages) throws Exception {

        /**
         * check if this queue has any subscription
         */

        //todo return the slot
        if (subscriptionStore.getActiveClusterSubscribersForDestination(targetQueue, false).size() == 0) {
            return 0;
        }

        /**
         * see if there are some messages scheduled to deliver but failed earlier and add to the read message list
         * and sort all to the order
         */
        ArrayList<AndesMessageMetadata> previouslyUndeliveredMessages = getUndeliveredMessagesOfQueue(targetQueue);
        if (previouslyUndeliveredMessages != null && previouslyUndeliveredMessages.size() > 0) {
            log.debug("TRACING >> previously undelivered messages count: " + previouslyUndeliveredMessages.size());
            messages.addAll(previouslyUndeliveredMessages);
            Collections.sort(messages, new Comparator<AndesMessageMetadata>() {
                public int compare(AndesMessageMetadata m1, AndesMessageMetadata m2) {
                    return Long.toString(m1.getMessageID()).compareTo(Long.toString(m2.getMessageID()));
                }
            });
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
            if (subscriptions4Queue != null) {
                /*
                 * we do this in a for loop to avoid iterating for a subscriptions for ever. We only iterate as
                 * once for each subscription
                 */
                for (int j = 0; j < subscriptions4Queue.size(); j++) {
                    LocalSubscription localSubscription = findNextSubscriptionToSent(targetQueue, subscriptions4Queue);
                    if (isThisSubscriptionHasRoom(localSubscription)) {
                        if (log.isDebugEnabled()) {
                            log.debug("TRACING>> scheduled to deliver - messageID: " + message.getMessageID() + " for queue: " + message.getDestination());
                        }
                        deliverAsynchronously(localSubscription, message);
                        totMsgSent++;
                        sentMessageCount++;
                        totalReadButUndeliveredMessages--;
                        messageSent = true;
                        iterator.remove();
                        break;
                    }
                }
                if (!messageSent) {
                    log.debug("All subscriptions for queue " + targetQueue + " have max Unacked messages " + message.getDestination());
                }
            } else {
                // TODO : Malinga do something here. All subscriptions deleted for the queue, should we move messages back to global queue?
                //todo return the slot
            }
        }
        return sentMessageCount;
    }


    private void deliverAsynchronously(final LocalSubscription subscription, final AndesMessageMetadata message) {
        if (onflightMessageTracker.testMessage(message.getMessageID())) {
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
                            storeUndeliveredMessagesDueToInactiveSubscriptions(message);
                            if (log.isDebugEnabled()) {
                                log.debug("TRACING>> QDW- storing due to subscription vanish - message messageID:" + message.getMessageID() + " for subscription " + subscription.getSubscriptionID());
                            }
                        }
                    } catch (Throwable e) {
                        log.error("Error while delivering message. Moving to Dead Letter Queue ", e);
                        //todo - hasitha - here we have already tried three times to deliver.
                    }
                }
            };
            executor.submit(r, (subscription.getTargetQueue() + subscription.getSubscriptionID()).hashCode());
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Rejecting message: messageID " + message.getMessageID());
            }
        }
    }

    private void storeUndeliveredMessagesDueToInactiveSubscriptions(AndesMessageMetadata message) {
        String queueName = message.getDestination();
        ArrayList<AndesMessageMetadata> undeliveredMessages = undeliveredMessagesMap.get(queueName);
        if (undeliveredMessages == null) {
            undeliveredMessages = new ArrayList<AndesMessageMetadata>();
            undeliveredMessages.add(message);
            undeliveredMessagesMap.put(queueName, undeliveredMessages);
        } else {
            undeliveredMessages.add(message);
        }
    }

    private ArrayList<AndesMessageMetadata> getUndeliveredMessagesOfQueue(String queueName) {

        ArrayList<AndesMessageMetadata> processedButUndeliveredMessages = new ArrayList<AndesMessageMetadata>();

        /**
         * get sent but not acked messages
         */
        ArrayList<AndesMessageMetadata> undeliveredMessagesOfQueue = onflightMessageTracker.getSentButNotAckedMessagesOfQueue(queueName);
        if (undeliveredMessagesOfQueue != null && !undeliveredMessagesOfQueue.isEmpty()) {
            processedButUndeliveredMessages.addAll(undeliveredMessagesOfQueue);
        }

        /**
         * get messages undelivered due to sudden subscription close
         */
        ArrayList<AndesMessageMetadata> messagesUndeliveredDueToInactiveSubscriptions = undeliveredMessagesMap.remove(queueName);
        if (messagesUndeliveredDueToInactiveSubscriptions != null && !messagesUndeliveredDueToInactiveSubscriptions.isEmpty()) {
            processedButUndeliveredMessages.addAll(messagesUndeliveredDueToInactiveSubscriptions);
        }

        return processedButUndeliveredMessages;
    }


    public void stopFlusher() {
        running = false;
        log.debug("Shutting down the queue message flusher for the queue " + nodeQueue);
    }

    public void setWorking() {
        log.debug("staring queue message flusher for " + nodeQueue);
        running = true;
    }

//    private boolean resetOffsetAtCassadraQueueIfNeeded(boolean force) {
//        resetCounter++;
//        if (force || (resetCounter > maxRestCounter && (System.currentTimeMillis() - lastRestTime) > queueMsgDeliveryCurserResetTimeInterval)) {
//            resetCounter = 0;
//            lastRestTime = System.currentTimeMillis();
//            lastProcessedId = getStartingIndex();
//            if (log.isDebugEnabled()) {
//                log.debug("TRACING>> QDW-Reset offset called and Updated lastProcessedId is= " + lastProcessedId);
//            }
//            return true;
//        }
//        return false;
//    }

//    private long getStartingIndex() {
//        long startingIndex = lastProcessedId;
//        if (subscriptionCursar4QueueMap.values().size() == 0) {
//            startingIndex = 0;
//        }
//        for (QueueDeliveryInfo queueDeliveryInfo : subscriptionCursar4QueueMap.values()) {
//
//            if (queueDeliveryInfo.hasQueueFullAndMessagesIgnored) {
//                if (startingIndex > queueDeliveryInfo.ignoredFirstMessageId && queueDeliveryInfo.ignoredFirstMessageId != -1) {
//                    startingIndex = queueDeliveryInfo.ignoredFirstMessageId;
//                }
//                if (queueDeliveryInfo.readButUndeliveredMessages.size() < maxNumberOfReadButUndeliveredMessages / 2) {
//                    queueDeliveryInfo.hasQueueFullAndMessagesIgnored = false;
//                }
//            }
//            if (queueDeliveryInfo.needToReset) {
//                if (startingIndex > queueDeliveryInfo.ignoredFirstMessageId) {
//                    startingIndex = queueDeliveryInfo.ignoredFirstMessageId;
//                }
//                queueDeliveryInfo.setNeedToReset(false);
//            }
//        }
//        if (startingIndex > 0) {
//            startingIndex--;
//        }
//        return startingIndex;
//    }

    public void clearMessagesAccumilatedDueToInactiveSubscriptionsForQueue(String destinationQueueName) throws AndesException {
        undeliveredMessagesMap.remove(destinationQueueName);
        getQueueDeliveryInfo(destinationQueueName).readButUndeliveredMessages.clear();
        Iterator<AndesMessageMetadata> laggardsIterator = laggardsMessages.iterator();
        while (laggardsIterator.hasNext()) {
            AndesMessageMetadata message = laggardsIterator.next();
            String routingKey = message.getDestination();
            if (routingKey.equals(destinationQueueName)) {
                laggardsIterator.remove();
            }
        }
    }

    public static QueueDeliveryWorker getInstance() {
        return queueDeliveryWorker;
    }


}