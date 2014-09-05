package org.wso2.andes.server.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.configuration.ClusterConfiguration;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <code>QueueDeliveryWorker</code> Handles the task of polling the user queues and flushing
 * the messages to subscribers
 * There will be one Flusher per Queue Per Node
 */
public class QueueDeliveryWorker {
    private final String nodeQueue;
    private boolean running = true;
    private static Log log = LogFactory.getLog(QueueDeliveryWorker.class);

    private int messageCountToRead = 50;
    private int maxMessageCountToRead = 300;
    private int minMessageCountToRead = 20;

    private int maxNumberOfUnAckedMessages = 20000;
    //per queue
    private int maxNumberOfReadButUndeliveredMessages = 1000;

    private long lastProcessedId = 0;

    private int resetCounter;

    private int maxRestCounter = 50;

    private long totMsgSent = 0;
    private long totMsgRead = 0;
    private int totalReadButUndeliveredMessages = 0;

    private long lastRestTime = 0;

    private SequentialThreadPoolExecutor executor;

    private final int queueWorkerWaitInterval;
    private int queueMsgDeliveryCurserResetTimeInterval;

    private long iterations = 0;
    private int workqueueSize = 0;
    private long failureCount = 0;

    private MessageStore messageStore;
    private SubscriptionStore subscriptionStore;
    private OnflightMessageTracker onflightMessageTracker;

    private static QueueDeliveryWorker queueDeliveryWorker;


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
        this.messageCountToRead = clusterConfiguration.getMessageBatchSizeForSubscribers();
        this.maxMessageCountToRead = clusterConfiguration.getMaxMessageBatchSizeForSubscribers();
        this.minMessageCountToRead = clusterConfiguration.getMinMessageBatchSizeForSubscribers();
        this.maxNumberOfUnAckedMessages = clusterConfiguration.getMaxNumberOfUnackedMessages();
        this.maxNumberOfReadButUndeliveredMessages = clusterConfiguration.getMaxNumberOfReadButUndeliveredMessages();
        this.queueMsgDeliveryCurserResetTimeInterval = clusterConfiguration.getQueueMsgDeliveryCurserResetTimeInterval();
        this.subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        this.onflightMessageTracker = OnflightMessageTracker.getInstance();

        if (isInMemoryMode) {
            this.messageStore = MessagingEngine.getInstance().getInMemoryMessageStore();
        } else {
            this.messageStore = MessagingEngine.getInstance().getDurableMessageStore();
        }

       // this.start();
      //  this.setWorking();
     //   startLaggardsThread();
     //

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
        boolean messageIgnored = false;
        boolean hasQueueFullAndMessagesIgnored = false;
        long ignoredFirstMessageId = -1;
        boolean needToReset = false;

        public void setIgnoredFirstMessageId(long ignoredFirstMessageId) {
            this.ignoredFirstMessageId = ignoredFirstMessageId;
        }

        public void setNeedToReset(boolean needToReset) {
            this.needToReset = needToReset;
        }
    }

    private Map<String, QueueDeliveryInfo> subscriptionCursar4QueueMap = new HashMap<String, QueueDeliveryInfo>();

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


    public QueueDeliveryInfo getQueueDeliveryInfo(String queueName) {
        QueueDeliveryInfo queueDeliveryInfo = subscriptionCursar4QueueMap.get(queueName);
        if (queueDeliveryInfo == null) {
            queueDeliveryInfo = new QueueDeliveryInfo();
            queueDeliveryInfo.queueName = queueName;
            Collection<LocalSubscription> localSubscribersForQueue = subscriptionStore.getActiveLocalSubscribersForQueue(queueName);
            queueDeliveryInfo.iterator = localSubscribersForQueue.iterator();
            subscriptionCursar4QueueMap.put(queueName, queueDeliveryInfo);
        }
        return queueDeliveryInfo;
    }


    /**
     * Laggards Thread to deliver messages skipped by Primary Delivery Thread if the conditions met
     */
    private void startLaggardsThread() {
       new Thread() {
            public void run() {
                try {
                    long lastReadLaggardMessageID = 0;
                    int laggardQueueEntriesListSize = 0;
                    //we need to delay laggards thread as it should be started after a fair interval after the leading thread
                    sleep4waitInterval(60000);
                    while (true) {
                        if (running) {
                            try {
                                if (laggardQueueEntriesListSize == 0 || lastReadLaggardMessageID >= lastProcessedId) {
                                    lastReadLaggardMessageID = 0;
                                    sleep4waitInterval(queueWorkerWaitInterval * 5);
                                }

                                //List<AMQMessage> laggardQueueEntriesList = messageStore.getMessagesFromNodeQueue(nodeQueue, queue, messageCountToRead, lastReadLaggardMessageID);
                                QueueAddress queueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE, nodeQueue);
                                List<AndesMessageMetadata> laggardMessageMetaDataList = messageStore.getNextNMessageMetadataFromQueue(queueAddress, lastReadLaggardMessageID++, messageCountToRead);
                                //log.info("LAGGARDS>> Read " + messageCountToRead + " number of messages from id " + lastReadLaggardMessageID + ". Returned " + laggardMessageMetaDataList.size());
                                for (AndesMessageMetadata entry : laggardMessageMetaDataList) {
                                    String routingKey = entry.getDestination();
                                    if (subscriptionStore.getActiveClusterSubscribersForDestination(routingKey, false).size() > 0) {
                                        laggardsMessages.add(entry);
                                    }
                                }
                                laggardQueueEntriesListSize = laggardMessageMetaDataList.size();
                                if (laggardQueueEntriesListSize > 0) {
                                    lastReadLaggardMessageID = laggardMessageMetaDataList.get(laggardQueueEntriesListSize - 1).getMessageID();
                                }
                                sleep4waitInterval(20000);
                            } catch (Exception e) {
                                log.warn("Error in laggard message reading thread ", e);
                                sleep4waitInterval(queueWorkerWaitInterval * 2);
                            }
                        } else {
                            Thread.sleep(2000);
                        }

                    }
                } catch (Exception e) {
                    log.error("Error in laggard message reader thread, it will break the thread", e);
                }

            }
        }.start();
    }

    /**
     * Main Message Delivery Thread
     */

    public void run(List<AndesMessageMetadata> messagesReadByLeadingThread) {
        iterations = 0;
        workqueueSize = 0;
        lastRestTime = System.currentTimeMillis();
        failureCount = 0;

       // while (true) {
           // if (running) {
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
                            //continue;
                        }

                        resetOffsetAtCassadraQueueIfNeeded(false);

                        /**
                         * Following reads from message store, it reads only if there are not enough messages loaded in memory
                         */
                        int msgReadThisTime = 0;
                        List<AndesMessageMetadata> messagesFromMessageStore;
                        if (totalReadButUndeliveredMessages < 10000) {
                            /**
                             * Read messages from leading thread
                             */
                            messagesFromMessageStore = new ArrayList<AndesMessageMetadata>();
                          //  QueueAddress queueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE, nodeQueue);
//                            List<AndesMessageMetadata> messagesReadByLeadingThread =
//                                    messageStore.getNextNMessageMetadataFromQueue(queueAddress, lastProcessedId++, messageCountToRead);
                            //log.info(" LEADING >> Read " + messageCountToRead + " number of messages from id " + lastProcessedId + ". Returned " + messagesReadByLeadingThread.size());

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

                            /**
                             * Add messages read from Laggards Thread as well
                             */
                            Iterator<AndesMessageMetadata> laggardsIterator = laggardsMessages.iterator();
                            while (laggardsIterator.hasNext()) {
                                AndesMessageMetadata laggardsMessage = laggardsIterator.next();
                                if (!onflightMessageTracker.checkIfAlreadyReadFromNodeQueue(laggardsMessage.getMessageID())) {
                                    String routingKey = laggardsMessage.getDestination();
                                    if (subscriptionStore.getActiveClusterSubscribersForDestination(routingKey,false).size() > 0) {
                                        messagesFromMessageStore.add(laggardsMessage);
                                        onflightMessageTracker.markMessageAsReadFromNodeQueue(laggardsMessage.getMessageID());
                                        if (log.isDebugEnabled()) {
                                            log.debug("TRACING>> QDW------Adding " + laggardsMessage.getMessageID() + " From laggardsMessages to deliver");
                                        }
                                    }
                                }
                                laggardsIterator.remove();
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
                                QueueDeliveryInfo queueDeliveryInfo = getQueueDeliveryInfo(queueName);

                                /**
                                 * We keep a limited number of messages in-memory scheduled to deliver
                                 */
                                if (!queueDeliveryInfo.hasQueueFullAndMessagesIgnored) {
                                    if (queueDeliveryInfo.readButUndeliveredMessages.size() < maxNumberOfReadButUndeliveredMessages) {

                                        long currentMessageId = message.getMessageID();
                                        queueDeliveryInfo.readButUndeliveredMessages.add(message);
                                        totalReadButUndeliveredMessages++;
                                    } else {
                                        queueDeliveryInfo.hasQueueFullAndMessagesIgnored = true;
                                        queueDeliveryInfo.ignoredFirstMessageId = message.getMessageID();
                                        OnflightMessageTracker.getInstance().unMarkMessageAsAlreadyReadFromNodeQueueMessageInstantly(message.getMessageID());
                                    }
                                } else {
                                    /**
                                     * All subscription in this queue are full and we were forced to ignore messages
                                     * do not keep message in-memory. Mark it as not read from Node Queue
                                     */
                                    if (queueDeliveryInfo.hasQueueFullAndMessagesIgnored && queueDeliveryInfo.ignoredFirstMessageId == -1) {
                                        queueDeliveryInfo.ignoredFirstMessageId = message.getMessageID();
                                    }
                                    OnflightMessageTracker.getInstance().unMarkMessageAsAlreadyReadFromNodeQueueMessageInstantly(message.getMessageID());
                                }
                            }
                            /**
                             * If no messages to read sleep more
                             */
                            if (messagesFromMessageStore.size() == 0) {
                                sleep4waitInterval(queueWorkerWaitInterval);
                            }

                            //If we have read all messages we asked for, we increase the reading count. Else we reduce it.
                            //TODO we might want to take into account the size of the message while we change the batch size
                            if (messagesFromMessageStore.size() == messageCountToRead) {
                                messageCountToRead += 100;
                                if (messageCountToRead > maxMessageCountToRead) {
                                    messageCountToRead = maxMessageCountToRead;
                                }
                            } else {
                                messageCountToRead -= 50;
                                if (messageCountToRead < minMessageCountToRead) {
                                    messageCountToRead = minMessageCountToRead;
                                }
                            }
                            totMsgRead = totMsgRead + messagesFromMessageStore.size();
                            msgReadThisTime = messagesFromMessageStore.size();
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("QDW >> Total ReadButUndeliveredMessages count " + totalReadButUndeliveredMessages + " is over the accepted limit ");
                            }
                        }

                        /**
                         * Now messages are read to the memory. Send the read messages to subscriptions
                         */
                        int sentMessageCount = 0;
                        for (QueueDeliveryInfo queueDeliveryInfo : subscriptionCursar4QueueMap.values()) {
                            log.debug("TRACING>> delivering read but undelivered message list with size: " + queueDeliveryInfo.readButUndeliveredMessages.size());
                            sentMessageCount = sendMessagesToSubscriptions(queueDeliveryInfo.queueName, queueDeliveryInfo.readButUndeliveredMessages);
                            queueDeliveryInfo.messageIgnored = false;
                        }

                        if (iterations % 20 == 0) {
                            if (log.isDebugEnabled()) {
                                log.info("[Flusher" + this + "]readNow=" + msgReadThisTime + " totRead=" + totMsgRead + " totprocessed= " + totMsgSent + ", totalReadButNotSent=" +
                                        totalReadButUndeliveredMessages + ". workQueue= " + workqueueSize + " lastID=" + lastProcessedId);
                            }
                        }
                        iterations++;
                        //Message reading work is over in this iteration. If read message count in this iteration is 0 definitely
                        // we have to force reset the counter
                        if (msgReadThisTime == 0) {
                            boolean f = resetOffsetAtCassadraQueueIfNeeded(false);
                        }
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

//            } else {
//                try {
//                    Thread.sleep(2000);
//                } catch (InterruptedException e) {
//                    //silently ignore
//                }
//            }

      //  }
    }


    private void sleep4waitInterval(long sleepInterval) {
        try {
            Thread.sleep(queueWorkerWaitInterval);
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
            Collection<LocalSubscription> subscriptions4Queue = subscriptionStore.getActiveLocalSubscribersForQueue(targetQueue);
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
                        log.error("Error while delivering message ", e);
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

    private boolean resetOffsetAtCassadraQueueIfNeeded(boolean force) {
        resetCounter++;
        if (force || (resetCounter > maxRestCounter && (System.currentTimeMillis() - lastRestTime) > queueMsgDeliveryCurserResetTimeInterval)) {
            resetCounter = 0;
            lastRestTime = System.currentTimeMillis();
            lastProcessedId = getStartingIndex();
            if (log.isDebugEnabled()) {
                log.debug("TRACING>> QDW-Reset offset called and Updated lastProcessedId is= " + lastProcessedId);
            }
            return true;
        }
        return false;
    }

    private long getStartingIndex() {
        long startingIndex = lastProcessedId;
        if (subscriptionCursar4QueueMap.values().size() == 0) {
            startingIndex = 0;
        }
        for (QueueDeliveryInfo queueDeliveryInfo : subscriptionCursar4QueueMap.values()) {

            if (queueDeliveryInfo.hasQueueFullAndMessagesIgnored) {
                if (startingIndex > queueDeliveryInfo.ignoredFirstMessageId && queueDeliveryInfo.ignoredFirstMessageId != -1) {
                    startingIndex = queueDeliveryInfo.ignoredFirstMessageId;
                }
                if (queueDeliveryInfo.readButUndeliveredMessages.size() < maxNumberOfReadButUndeliveredMessages / 2) {
                    queueDeliveryInfo.hasQueueFullAndMessagesIgnored = false;
                }
            }
            if (queueDeliveryInfo.needToReset) {
                if (startingIndex > queueDeliveryInfo.ignoredFirstMessageId) {
                    startingIndex = queueDeliveryInfo.ignoredFirstMessageId;
                }
                queueDeliveryInfo.setNeedToReset(false);
            }
        }
        if (startingIndex > 0) {
            startingIndex--;
        }
        return startingIndex;
    }

    public void clearMessagesAccumilatedDueToInactiveSubscriptionsForQueue(String destinationQueueName) {
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
        if (queueDeliveryWorker == null) {

            synchronized (QueueDeliveryWorker.class) {
                if (queueDeliveryWorker == null) {
                    queueDeliveryWorker = new QueueDeliveryWorker(1000,false);
                }
            }
        }
        return queueDeliveryWorker;
    }

}