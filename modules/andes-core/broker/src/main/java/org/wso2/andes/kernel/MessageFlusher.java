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

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.distruptor.delivery.DisruptorBasedFlusher;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.server.store.MessageMetaDataType;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;


/**
 * <code>MessageFlusher</code> Handles the task of polling the user queues and flushing the
 * messages to subscribers There will be one Flusher per Queue Per Node
 */
public class MessageFlusher {
    private static Log log = LogFactory.getLog(MessageFlusher.class);
    private final DisruptorBasedFlusher flusherExecutor;

    private Integer maxNumberOfUnAckedMessages = 100000;

    //per destination
    private Integer maxNumberOfReadButUndeliveredMessages = 5000;

    private final int queueWorkerWaitInterval = 1000;

    /**
     * Subscribed destination wise information
     * the key here is the original destination of message. NOT storage queue name.
     */
    private Map<String, MessageDeliveryInfo> subscriptionCursar4QueueMap = new HashMap<String,
            MessageDeliveryInfo>();

    private SubscriptionStore subscriptionStore;

    private static MessageFlusher messageFlusher = new MessageFlusher();

    public MessageFlusher() {

        this.maxNumberOfUnAckedMessages = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_ACK_HANDLING_MAX_UNACKED_MESSAGES);

        this.maxNumberOfReadButUndeliveredMessages = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_MAX_READ_BUT_UNDELIVERED_MESSAGES);

        this.subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        flusherExecutor = new DisruptorBasedFlusher();
    }

    /**
     * Class to keep track of message delivery information destination wise
     */
    public class MessageDeliveryInfo {
        String destination;

        Iterator<LocalSubscription> iterator;
        //in-memory message list scheduled to be delivered
        Set<AndesMessageMetadata> readButUndeliveredMessages = new
                ConcurrentSkipListSet<AndesMessageMetadata>();

        /***
         * In case of a purge, we must store the timestamp when the purge was called.
         * This way we can identify messages received before that timestamp that fail and ignore them.
         */
        private Long lastPurgedTimestamp;

        /***
         * Constructor
         * initialize lastPurgedTimestamp to 0.
         */
        public MessageDeliveryInfo() {
            lastPurgedTimestamp = 0l;
        }

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

        /***
         * Clear the read-but-undelivered collection of messages of the given queue from memory
         * @return Number of messages that was in the read-but-undelivered buffer
         */
        public int clearReadButUndeliveredMessages() {

            int messageCount = readButUndeliveredMessages.size();

            readButUndeliveredMessages.clear();

            return messageCount;
        }

        /***
         * @return Last purged timestamp of queue.
         */
        public Long getLastPurgedTimestamp() {
            return lastPurgedTimestamp;
        }

        /**
         * set last purged timestamp for queue.
         * @param lastPurgedTimestamp the time stamp of the message message which was purged most recently
         */
        public void setLastPurgedTimestamp(Long lastPurgedTimestamp) {
            this.lastPurgedTimestamp = lastPurgedTimestamp;
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
        LocalSubscription localSubscription = null;
        boolean isValidLocalSubscription = false;
        if (subscriptions4Queue == null || subscriptions4Queue.size() == 0) {
            subscriptionCursar4QueueMap.remove(destination);
            return null;
        }

        MessageDeliveryInfo messageDeliveryInfo = getMessageDeliveryInfo(destination);
        Iterator<LocalSubscription> it = messageDeliveryInfo.iterator;
        while (it.hasNext()) {
            localSubscription = it.next();
            if (subscriptions4Queue.contains(localSubscription)) {
                isValidLocalSubscription = true;
                break;
            }
        }
        if(isValidLocalSubscription){
             return localSubscription;
        }else {
            it = subscriptions4Queue.iterator();
            messageDeliveryInfo.iterator = it;
            if (it.hasNext()) {
                return it.next();
            } else {
                return null;
            }
        }
    }


    /**
     * Will allow retrival of information related to delivery of the message
     *
     * @param destination where the message should be delivered to
     * @return the information which holds of the message which should be delivered
     * @throws AndesException
     */
    public MessageDeliveryInfo getMessageDeliveryInfo(String destination) throws AndesException {
        MessageDeliveryInfo messageDeliveryInfo = subscriptionCursar4QueueMap.get(destination);
        if (messageDeliveryInfo == null) {
            messageDeliveryInfo = new MessageDeliveryInfo();
            messageDeliveryInfo.destination = destination;
            Collection<LocalSubscription> localSubscribersForQueue = subscriptionStore
                    .getActiveLocalSubscribersForQueuesAndTopics(destination);
            messageDeliveryInfo.iterator = localSubscribersForQueue.iterator();
            subscriptionCursar4QueueMap.put(destination, messageDeliveryInfo);
        }
        return messageDeliveryInfo;
    }


    /**
     * Validates if the the buffer is empty, the messages will be read through this buffer and will be delivered to the
     * relevant subscriptions
     * @param queueName the name of the queue which hold the messages
     * @return whether the buffer is empty
     */
    public boolean isMessageBufferEmpty(String queueName) {
        return subscriptionCursar4QueueMap.get(queueName).readButUndeliveredMessages.isEmpty();
    }

    /**
     * send the messages to deliver
     *
     * @param messagesRead
     *         AndesMetadata list
     * @param slot
     *         these messages are belonged to
     */
    public void sendMessageToBuffer(List<AndesMessageMetadata> messagesRead,
                                    Slot slot) {


        try {
            for (AndesMessageMetadata message : messagesRead) {

                /**
                 * Rather than destination of the message, we get the destination of
                 * the messages in the slot. In hierarchical topic case this will
                 * represent subscription bound destination NOT message destination
                 * (games.cricket.* Not games.cricket.SriLanka)
                 */
                String destination = slot.getDestinationOfMessagesInSlot();
                message.setSlot(slot);
                MessageDeliveryInfo messageDeliveryInfo = getMessageDeliveryInfo(destination);
                //check and buffer message
                //stamp this message as buffered
                boolean isOKToBuffer = OnflightMessageTracker.getInstance()
                                                             .addMessageToBufferingTracker(slot,
                                                                                           message);
                if (isOKToBuffer) {
                    messageDeliveryInfo.readButUndeliveredMessages.add(message);
                    //increment the message count in the slot
                    OnflightMessageTracker.getInstance().incrementMessageCountInSlot(slot);
                } else {
                    log.warn("Tracker rejected message id= " + message.getMessageID() + " from buffering " +
                             "to deliver. This is an already buffered message");
                    //todo: this message is previously buffered. Should be removed from slot
                }
            }
        } catch (Throwable e) {
            log.fatal("Error running Cassandra Message Flusher" + e.getMessage(), e);
        }
    }

    /**
     * Read messages from the buffer and send messages to subscribers
     */
    public void sendMessagesInBuffer(String subDestination) throws AndesException {
        /**
         * Now messages are read to the memory. Send the read messages to subscriptions
         */
        if (log.isDebugEnabled()) {
            log.debug("Sending messages in buffer destination= " + subDestination);
        }
        long failureCount = 0;
        MessageDeliveryInfo messageDeliveryInfo = subscriptionCursar4QueueMap.get(subDestination);
        if (log.isDebugEnabled()) {
            for (String dest : subscriptionCursar4QueueMap.keySet()) {
                log.debug("Queue size of destination " + dest + " is :"
                        + subscriptionCursar4QueueMap.get(dest).readButUndeliveredMessages
                        .size());
            }

        }
        try {
            if(log.isDebugEnabled()) {
                log.debug(
                        "Sending messages from buffer num of msg = " + messageDeliveryInfo
                                .readButUndeliveredMessages
                                .size());
            }
            sendMessagesToSubscriptions(messageDeliveryInfo.destination,
                    messageDeliveryInfo.readButUndeliveredMessages);
        } catch (Exception e) {
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
            log.error("Error occurred while sending messages to subscribers from buffer", e);
            throw new AndesException("Error occurred while sending messages to subscribers " +
                                     "from message buffer", e);
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

        int notAckedMsgCount = OnflightMessageTracker.getInstance().getNotAckedMessageCount(localSubscription.getChannelID());

        //Here we ignore messages that has been scheduled but not executed,
        // so it might send few messages than maxNumberOfUnAckedMessages
        if(notAckedMsgCount < 0){
            if(log.isDebugEnabled()){
                log.debug("Invalid local subscription selected to send messages");
            }
            return false;
        }
        if (notAckedMsgCount < maxNumberOfUnAckedMessages) {
            return true;
        } else {

            if (log.isDebugEnabled()) {
                log.debug(
                        "Not selected, channel =" + localSubscription + " pending count =" +
                        (notAckedMsgCount));
            }
            return false;
        }
    }

    /**
     * Check whether there are active subscribers and send
     *
     * @param destination queue name
     * @param messages    metadata set
     * @return how many messages sent
     * @throws Exception
     */
    public int sendMessagesToSubscriptions(String destination, Set<AndesMessageMetadata> messages)
            throws Exception {

        /**
         * deliver messages to subscriptions
         */
        int sentMessageCount = 0;
        boolean orphanedSlot = false;
        Iterator<AndesMessageMetadata> iterator = messages.iterator();
        List<AndesRemovableMetadata> droppedTopicMessageList = new ArrayList<AndesRemovableMetadata>();

        while (iterator.hasNext()) {

            try {
                AndesMessageMetadata message = iterator.next();

                /**
                 * get all relevant type of subscriptions. This call does NOT
                 * return hierarchical subscriptions for the destination. There
                 * are duplicated messages for each different subscribed destination.
                 * For durable topic subscriptions this should return queue subscription
                 * bound to unique queue based on subscription id
                 */
                Collection<LocalSubscription> subscriptions4Queue =
                        subscriptionStore.getActiveLocalSubscribers(destination, message.isTopic());

                //If this is a topic message, we remove all durable topic subscriptions here.
                //Because durable topic subscriptions will get messages via queue path.
                if (message.isTopic()) {
                    Iterator<LocalSubscription> subscriptionIterator = subscriptions4Queue.iterator();
                    while (subscriptionIterator.hasNext()) {
                        LocalSubscription subscription = subscriptionIterator.next();
                        /**
                         * Here we need to consider the arrival time of the message. Only topic
                         * subscribers who appeared before publishing this message should receive it
                         */
                        if (subscription.isDurable() || (subscription.getSubscribeTime() > message.getArrivalTime())) {
                            subscriptionIterator.remove();
                        }

                        // Avoid sending if the subscriber is MQTT and message is not MQTT
                        if (AndesSubscription.SubscriptionType.MQTT == subscription.getSubscriptionType()
                                && MessageMetaDataType.META_DATA_MQTT != message.getMetaDataType()) {
                            subscriptionIterator.remove();
                        // Avoid sending if the subscriber is AMQP and message is MQTT
                        } else if (AndesSubscription.SubscriptionType.AMQP == subscription.getSubscriptionType()
                                && MessageMetaDataType.META_DATA_MQTT == message.getMetaDataType()) {
                            subscriptionIterator.remove();
                        }
                    }

                    if (subscriptions4Queue.size() == 0) {
                        iterator.remove(); // remove buffer
                        OnflightMessageTracker.getInstance().decrementMessageCountInSlotAndCheckToResend(message.getSlot());
                        AndesRemovableMetadata removableMetadata = new AndesRemovableMetadata(message.getMessageID(),
                                message.getDestination(), message.getStorageQueueName());
                        droppedTopicMessageList.add(removableMetadata);

                        continue; // skip this iteration if no subscriptions for the message
                    }

                } else { // Queue
                    if (subscriptions4Queue.size() == 0) {
                        // We don't have subscribers for this message
                        // Handle orphaned slot created with this no subscription scenario for queue
                        orphanedSlot = true;
                        break; // break the loop
                    }
                }


                //check id destination has any subscription for the current message

                int numOfCurrentMsgDeliverySchedules = 0;

                /**
                 * if message is addressed to queues, only ONE subscriber should
                 * get the message. Otherwise, loop for every subscriber
                 */
                for (int j = 0; j < subscriptions4Queue.size(); j++) {
                    LocalSubscription localSubscription = findNextSubscriptionToSent(destination,
                            subscriptions4Queue);
                    if (!message.isTopic()) { //for queue messages and durable topic messages (as they are now queue messages)
                        if (isThisSubscriptionHasRoom(localSubscription)) {
                            if (log.isDebugEnabled()) {
                                log.debug("Scheduled to send id = " + message.getMessageID());
                            }

                            // In a re-queue for delivery scenario we need the correct destination. Hence setting
                            // it back correctly in AndesMetadata for durable subscription for topics
                            if (localSubscription.isBoundToTopic()) {
                                message.setDestination(localSubscription.getSubscribedDestination());
                            }
                            deliverMessageAsynchronously(localSubscription, message);
                            numOfCurrentMsgDeliverySchedules++;
                            break;
                        }
                    } else { //for normal (non-durable) topic messages. We do not consider room
                        if (log.isDebugEnabled()) {
                            log.debug("Scheduled to send id = " + message.getMessageID());
                        }
                        deliverMessageAsynchronously(localSubscription, message);
                        numOfCurrentMsgDeliverySchedules++;
                    }
                }

                //remove message after sending to all subscribers

                if (!message.isTopic()) { //queue messages (and durable topic messages)
                    if (numOfCurrentMsgDeliverySchedules == 1) {
                        iterator.remove();
                        if (log.isDebugEnabled()) {
                            log.debug("Removing Scheduled to send message from buffer. MsgId= " + message.getMessageID());
                        }
                        sentMessageCount++;
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("All subscriptions for destination " + destination + " have max unacked " +
                                            "messages " + message.getDestination());
                        }
                        //if we continue message order will break
                        break;
                    }
                } else { //normal topic message
                    if (numOfCurrentMsgDeliverySchedules == subscriptions4Queue.size()) {
                        iterator.remove();
                        if (log.isDebugEnabled()) {
                            log.debug("Removing Scheduled to send message from buffer. MsgId= " + message.getMessageID());
                        }
                        sentMessageCount++;
                    } else {
                        log.warn("Could not schedule message delivery to all" +
                                " subscriptions. May cause message duplication. id= " + message.getMessageID());
                        //if we continue message order will break
                        break;
                    }
                }
            } catch (NoSuchElementException ex) {
                // This exception can occur because the iterator of ConcurrentSkipListSet loads the at-the-time snapshot.
                // Some records could be deleted by the time the iterator reaches them.
                // However, this can only happen at the tail of the collection, not in middle, and it would cause the loop
                // to blindly check for a batch of deleted records.
                // Given this situation, this loop should break so the sendFlusher can re-trigger it.
                // for tracing purposes can use this : log.warn("NoSuchElementException thrown",ex);
                log.warn("NoSuchElementException thrown. ",ex);
                break;
            }
        }
        // clear all tracking when orphan slot situation
        if (orphanedSlot) {
            for (AndesMessageMetadata message : messages) {
                OnflightMessageTracker.getInstance().clearAllTrackingWhenSlotOrphaned(message.getSlot());
            }
            messages.clear();
        }
        // delete topic messages that were dropped due to no subscriptions for the message
        Andes.getInstance().deleteMessages(droppedTopicMessageList, false);
        return sentMessageCount;
    }

    /**
     * Schedule to deliver message for the subscription
     * @param subscription subscription to send
     * @param message message to send
     */
    public void scheduleMessageForSubscription(LocalSubscription subscription,
                                               final AndesMessageMetadata message) {
        deliverMessageAsynchronously(subscription, message);
    }

    /**
     * Submit the messages to a thread pool to deliver asynchronously
     *
     * @param subscription local subscription
     * @param message      metadata of the message
     */
    private void deliverMessageAsynchronously(LocalSubscription subscription, AndesMessageMetadata message) {
        if(log.isDebugEnabled()) {
            log.debug("Scheduled message id= " + message.getMessageID() + " to be sent to subscription= " + subscription);
        }
        OnflightMessageTracker.getInstance().incrementNumberOfScheduledDeliveries(message.getMessageID());
        flusherExecutor.submit(subscription, message);
    }

    //TODO: in multiple subscription case this can cause message duplication
    /**
     * Will be responsible in placing the message back at the queue if delivery fails
     * @param message the message which was scheduled for delivery to its subscribers
     */
    public void reQueueUndeliveredMessagesDueToInactiveSubscriptions(AndesMessageMetadata message) {
        String destination = message.getDestination();
        subscriptionCursar4QueueMap.get(destination).readButUndeliveredMessages.add(message);
    }

    public static MessageFlusher getInstance() {
        return messageFlusher;
    }

    public DisruptorBasedFlusher getFlusherExecutor() {
        return flusherExecutor;
    }
}
