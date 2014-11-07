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

    //per destination
    private int maxNumberOfReadButUndeliveredMessages = 5000;

    private SequentialThreadPoolExecutor executor;

    private final int queueWorkerWaitInterval;

    /**
     * Subscribed destination wise information
     */
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
     * Class to keep track of message delivery information destination wise
     */
    public class QueueDeliveryInfo {
        String destination;

        Iterator<LocalSubscription> iterator;
        //in-memory message list scheduled to be delivered
        Set<AndesMessageMetadata> readButUndeliveredMessages = new
                ConcurrentSkipListSet<AndesMessageMetadata>();

        /**
         * Returns boolean variable saying whether this destination has room or not
         *
         * @return whether this destination has room or not
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
     * Get the next subscription for the given destination. If at end of the subscriptions, it circles
     * around to the first one
     *
     * @param destination
     *         name of destination
     * @param subscriptions4Queue
     *         subscriptions registered for the destination
     * @return subscription to deliver
     * @throws AndesException
     */
    private LocalSubscription findNextSubscriptionToSent(String destination,
                                                         Collection<LocalSubscription>
                                                                 subscriptions4Queue)
            throws AndesException {
        if (subscriptions4Queue == null || subscriptions4Queue.size() == 0) {
            subscriptionCursar4QueueMap.remove(destination);
            return null;
        }

        QueueDeliveryInfo queueDeliveryInfo = getQueueDeliveryInfo(destination);
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


    public QueueDeliveryInfo getQueueDeliveryInfo(String destination) throws AndesException {
        QueueDeliveryInfo queueDeliveryInfo = subscriptionCursar4QueueMap.get(destination);
        if (queueDeliveryInfo == null) {
            queueDeliveryInfo = new QueueDeliveryInfo();
            queueDeliveryInfo.destination = destination;
            Collection<LocalSubscription> localSubscribersForQueue = subscriptionStore
                    .getActiveLocalSubscribersForQueuesAndTopics(destination);
            queueDeliveryInfo.iterator = localSubscribersForQueue.iterator();
            subscriptionCursar4QueueMap.put(destination, queueDeliveryInfo);
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
             *    Following check is to avoid the worker destination been full with too many pending tasks
             *    to send messages. Better stop buffering until we have some breathing room
             */
            pendingJobsToSendToTransport = executor.getSize();

            if (pendingJobsToSendToTransport > 1000) {
                if (pendingJobsToSendToTransport > 5000) {
                    log.error(
                            "Flusher destination is growing, and this should not happen. Please check " +
                            "cassandra Flusher");
                }
                log.warn("Flusher destination has " + pendingJobsToSendToTransport + " tasks");
                // TODO: we need to handle this. Notify slot delivery worker to sleep more
            }
            for (AndesMessageMetadata message : messagesReadByLeadingThread) {

                /**
                 * Rather than destination of the message, we get the destination of
                 * the messages in the slot. In hierarchical topic case this will
                 * represent subscription bound destination NOT message destination
                 * (games.cricket.* Not games.cricket.SriLanka)
                 */
                String destination = slot.getDestinationOfMessagesInSlot();
                message.setSlot(slot);
                QueueDeliveryInfo queueDeliveryInfo = getQueueDeliveryInfo(destination);
                //check and buffer message
                //stamp this message as buffered
                boolean isOKToBuffer = OnflightMessageTracker.getInstance()
                                                             .addMessageToBufferingTracker(slot,
                                                                                           message);
                if (isOKToBuffer) {
                    queueDeliveryInfo.readButUndeliveredMessages.add(message);
                    //increment the message count in the slot
                    OnflightMessageTracker.getInstance().incrementMessageCountInSlot(slot);
                } else {
                    log.warn("Tracker rejected message id= " + message.getMessageID() + " from buffering " +
                             "to deliver. This is an already buffered message");
                    //todo: this message is previously buffered. Should be removed from slot
                }
            }
            /**
             * Now messages are read to the memory. Send the read messages to subscriptions
             */
            sendMessagesInBuffer(slot.getDestinationOfMessagesInSlot());
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
    public void sendMessagesInBuffer(String subDestination) throws AndesException {

        QueueDeliveryInfo queueDeliveryInfo = subscriptionCursar4QueueMap.get(subDestination);
        if (log.isDebugEnabled()) {
            for (String dest : subscriptionCursar4QueueMap.keySet()) {
                log.debug("Queue size of destination " + dest + " is :"
                          + subscriptionCursar4QueueMap.get(dest).readButUndeliveredMessages
                        .size());
            }

        }
        try {
            log.debug(
                    "Sending messages from buffer num of msg = " + queueDeliveryInfo
                            .readButUndeliveredMessages
                            .size());
            sendMessagesToSubscriptions(queueDeliveryInfo.destination,
                                        queueDeliveryInfo.readButUndeliveredMessages);
        } catch (Exception e) {
            log.error("Error occurred while sending messages to subscribers from buffer", e);
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
     * does that destination has too many messages pending
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


    public int sendMessagesToSubscriptions(String destination, Set<AndesMessageMetadata> messages)
            throws Exception {

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

            /**
             * get all relevant type of subscriptions. This call does NOT
             * return hierarchical subscriptions for the destination. There
             * are duplicated messages for each different subscribed destination.
             * For durable topic subscriptions this should return queue subscription
             * bound to unique queue based on subscription id
             */
            Collection<LocalSubscription> subscriptions4Queue =
                    subscriptionStore.getActiveLocalSubscribers(destination, message.isTopic());

            //check id destination has any subscription
            //todo return the slot
            if (subscriptions4Queue.size() == 0) {
                return 0;
            }

            int numOfCurrentMsgDeliverySchedules = 0;

            /**
             * if message is addressed to queues, only ONE subscriber should
             * get the message. Otherwise, loop for every subscriber
             */
            for (int j = 0; j < subscriptions4Queue.size(); j++) {
                LocalSubscription localSubscription = findNextSubscriptionToSent(destination,
                                                                                 subscriptions4Queue);
                if(!message.isTopic()) { //for queue messages
                    if (isThisSubscriptionHasRoom(localSubscription)) {
                        log.debug("Scheduled to send id = " + message.getMessageID());
                        deliverAsynchronously(localSubscription, message);
                        numOfCurrentMsgDeliverySchedules ++;
                        break;
                    }
                }  else { //for topic messages. We do not consider room
                    log.debug("Scheduled to send id = " + message.getMessageID());
                    deliverAsynchronously(localSubscription, message);
                    numOfCurrentMsgDeliverySchedules ++;
                }
            }
            //remove message after sending to all subscribers

            if(!message.isTopic()) { //queue messages
                if(numOfCurrentMsgDeliverySchedules == 1) {
                    iterator.remove();
                    sentMessageCount++;
                } else {
                    log.debug(
                            "All subscriptions for destination " + destination + " have max unacked " +
                            "messages " + message
                                    .getDestination());
                    //if we continue message order will break
                    break;
                }
            } else { //topic message
                if(numOfCurrentMsgDeliverySchedules == subscriptions4Queue.size()) {
                    iterator.remove();
                    sentMessageCount++;
                }  else {
                    log.warn("Could not schedule message delivery to all" +
                             " subscriptions. May cause message duplication. id= " + message.getMessageID());
                    //if we continue message order will break
                    break;
                }
            }
        }
        return sentMessageCount;
    }

    /**
     * Schedule to deliver message for the subscription
     * @param subscription subscription to send
     * @param message message to send
     */
    public void scheduleMessageForSubscription(LocalSubscription subscription,
                                               final AndesMessageMetadata message) {
        deliverAsynchronously(subscription, message);
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
                    OnflightMessageTracker.getInstance().decrementNumberOfScheduledDeliveries(message.getMessageID());
                } catch (Throwable e) {
                    log.error("Error while delivering message. Moving to Dead Letter Queue ", e);
                    OnflightMessageTracker.getInstance().decrementNumberOfScheduledDeliveries(message.getMessageID());
                    //todo - hasitha - here we have already tried three times to deliver.
                }
            }
        };
        executor.submit(r, (subscription.getTargetQueue() + subscription.getSubscriptionID())
                .hashCode());
        OnflightMessageTracker.getInstance().incrementNumberOfScheduledDeliveries(message.getMessageID());
    }

    //TODO: in multiple subscription case this can cause message duplication
    public void reQueueUndeliveredMessagesDueToInactiveSubscriptions(AndesMessageMetadata message) {
        String destination = message.getDestination();
        subscriptionCursar4QueueMap.get(destination).readButUndeliveredMessages.add(message);
    }

    public void stopFlusher() {
        running = false;
        log.debug("Shutting down the destination message flusher for the destination " + nodeQueue);
    }

    public void clearMessagesAccumilatedDueToInactiveSubscriptionsForQueue(
            String destinationQueueName) throws AndesException {
        getQueueDeliveryInfo(destinationQueueName).readButUndeliveredMessages.clear();
    }

    public static QueueDeliveryWorker getInstance() {
        return queueDeliveryWorker;
    }


}