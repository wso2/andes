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
import org.wso2.andes.configuration.util.TopicMessageDeliveryStrategy;
import org.wso2.andes.kernel.disruptor.delivery.DisruptorBasedFlusher;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.subscription.LocalSubscription;
import org.wso2.andes.subscription.SubscriptionStore;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;


/**
 * <code>MessageFlusher</code> Handles the task of polling the user queues and flushing the
 * messages to subscribers There will be one Flusher per Queue Per Node
 */
public class MessageFlusher {

    private static Log log = LogFactory.getLog(MessageFlusher.class);

    private final DisruptorBasedFlusher flusherExecutor;

    //per destination
    private Integer maxNumberOfReadButUndeliveredMessages = 5000;

    private final int queueWorkerWaitInterval = 1000;

    /**
     * Subscribed destination wise information
     * the key here is the original destination of message. NOT storage queue name.
     */
    private Map<String, MessageDeliveryInfo> subscriptionCursar4QueueMap = new HashMap<>();

    private SubscriptionStore subscriptionStore;

    /**
     * Message flusher for queue message delivery. Depending on the behaviour of the strategy
     * conditions to push messages to subscribers vary.
     */
    private MessageDeliveryStrategy queueMessageFlusher;

    /**
     * Message flusher for topic message delivery. Depending on the behaviour of the strategy
     * conditions to push messages to subscribers vary.
     */
    private MessageDeliveryStrategy topicMessageFlusher;


    /**
     * List of delivery rules to evaluate. Before scheduling message to protocol for
     * delivery we evaluate these and if failed we take necessary actions
     */
    private List<CommonDeliveryRule> DeliveryRulesList = new ArrayList<>();



    private static MessageFlusher messageFlusher = new MessageFlusher();

    public MessageFlusher() {

        this.subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        flusherExecutor = new DisruptorBasedFlusher();

        this.maxNumberOfReadButUndeliveredMessages = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_MAX_READ_BUT_UNDELIVERED_MESSAGES);

        //set queue message flusher
        this.queueMessageFlusher = new FlowControlledQueueMessageDeliveryImpl(subscriptionStore);

        //set topic message flusher
        TopicMessageDeliveryStrategy topicMessageDeliveryStrategy = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_TOPIC_MESSAGE_DELIVERY_STRATEGY);
        if(topicMessageDeliveryStrategy.equals(TopicMessageDeliveryStrategy.DISCARD_ALLOWED)
                || topicMessageDeliveryStrategy.equals(TopicMessageDeliveryStrategy.DISCARD_NONE)) {
            this.topicMessageFlusher = new NoLossBurstTopicMessageDeliveryImpl(subscriptionStore);
        } else if(topicMessageDeliveryStrategy.equals(TopicMessageDeliveryStrategy.SLOWEST_SUB_RATE)) {
            this.topicMessageFlusher = new SlowestSubscriberTopicMessageDeliveryImpl(subscriptionStore);
        }

        initializeDeliveryRules();

    }


    /**
     * Initialize common delivery rules. These delivery rules apply
     * irrespective of the protocol
     */
    private void initializeDeliveryRules() {

/*        // NOTE: Feature Message Expiration moved to a future release
        //checking message expiration deliver rule
        deliveryRulesList.add(new MessageExpiredRule());*/

        //checking message purged delivery rule
        DeliveryRulesList.add(new MessagePurgeRule());
    }

    /**
     * Evaluating Delivery rules before sending the messages
     *
     * @param message AMQ Message
     * @return IsOKToDelivery
     * @throws AndesException
     */
    private boolean evaluateDeliveryRules(DeliverableAndesMetadata message) throws AndesException {
        boolean isOKToDelivery = true;

        for (CommonDeliveryRule rule : DeliveryRulesList) {
            if (!rule.evaluate(message)) {
                isOKToDelivery = false;
                break;
            }
        }
        return isOKToDelivery;
    }

    /**
     * Class to keep track of message delivery information destination wise
     */
    public class MessageDeliveryInfo {

        /**
         * Destination of the messages (not storage queue name). For durable topics
         * this is the internal queue name with subscription id.
         */
        private String destination;

        /**
         * Subscription iterator
         */
        Iterator<LocalSubscription> iterator;

        /**
         * in-memory message list scheduled to be delivered. These messages will be flushed
         * to subscriber
         */
        private Set<DeliverableAndesMetadata> readButUndeliveredMessages = new
                ConcurrentSkipListSet<>();

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
         * Buffer messages to be delivered
         *
         * @param message message metadata to buffer
         */
        public void bufferMessage(DeliverableAndesMetadata message) {
            readButUndeliveredMessages.add(message);
            message.markAsBuffered();
            //Tracing message
            MessageTracer.trace(message, MessageTracer.METADATA_BUFFERED_FOR_DELIVERY);

        }

        /**
         * Check if message buffer for destination is empty
         *
         * @return true if empty
         */
        public boolean isMessageBufferEmpty() {
            return readButUndeliveredMessages.isEmpty();
        }

        /**
         * Get the number of messages buffered for the destination to be delivered
         *
         * @return number of messages buffered
         */
        public int getSizeOfMessageBuffer() {
            return readButUndeliveredMessages.size();
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
    public LocalSubscription findNextSubscriptionToSent(String destination,
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

                // We have to iterate through the collection to find the matching the local subscription since
                // the Collection does not have a get method
                for (LocalSubscription subscription : subscriptions4Queue) {
                    // Assign the matching object reference from subscriptions4Queue collection
                    // to local subscription variable
                    if (subscription.equals(localSubscription)) {
                        localSubscription = subscription;
                        break;
                    }
                }
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
     * Will allow retrieval of information related to delivery of the message
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
     * Initialize message flusher for delivering messages for the given destination. This is the destination consumers
     * subscribe.
     *
     * @param destination
     *         Delivery Destination
     * @throws AndesException
     */
    public void prepareForDelivery(String destination) throws AndesException {
        MessageDeliveryInfo messageDeliveryInfo = new MessageDeliveryInfo();
        messageDeliveryInfo.destination = destination;
        Collection<LocalSubscription> localSubscribersForQueue = subscriptionStore
                .getActiveLocalSubscribersForQueuesAndTopics(destination);

        messageDeliveryInfo.iterator = localSubscribersForQueue.iterator();
        subscriptionCursar4QueueMap.put(destination, messageDeliveryInfo);
    }

    /**
     * Validates if the the buffer is empty, the messages will be read through this buffer and will be delivered to the
     * relevant subscriptions
     * @param destination the name of the queue which hold the messages
     * @return whether the buffer is empty
     */
    public boolean isMessageBufferEmpty(String destination) {
        return subscriptionCursar4QueueMap.get(destination).isMessageBufferEmpty();
    }

    /**
     * send the messages to deliver
     *
     * @param messagesRead
     *         AndesMetadata list
     * @param slot
     *         these messages are belonged to
     */
    public void sendMessageToBuffer(List<DeliverableAndesMetadata> messagesRead, Slot slot) {
        try {
            slot.incrementPendingMessageCount(messagesRead.size());
            for (DeliverableAndesMetadata message : messagesRead) {

                /**
                 * Rather than destination of the message, we get the destination of
                 * the messages in the slot. In hierarchical topic case this will
                 * represent subscription bound destination NOT message destination
                 * (games.cricket.* Not games.cricket.SriLanka)
                 */
                String destination = slot.getDestinationOfMessagesInSlot();
                MessageDeliveryInfo messageDeliveryInfo = getMessageDeliveryInfo(destination);
                messageDeliveryInfo.bufferMessage(message);
            }
        } catch (Throwable e) {
            log.fatal("Error scheduling messages for delivery", e);
        }
    }

    /**
     * Send the messages to deliver
     *
     * @param destination
     *         message destination
     * @param messages
     *         message to add
     */
    public void addAlreadyTrackedMessagesToBuffer(String destination, List<DeliverableAndesMetadata> messages) {
        try {
            MessageDeliveryInfo messageDeliveryInfo = getMessageDeliveryInfo(destination);
            for (DeliverableAndesMetadata metadata : messages) {
                messageDeliveryInfo.bufferMessage(metadata);
            }
        } catch (AndesException e) {
            log.fatal("Error scheduling messages for delivery", e);
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
                        + subscriptionCursar4QueueMap.get(dest).getSizeOfMessageBuffer());
            }
        }
        try {
            if(log.isDebugEnabled()) {
                log.debug("Sending messages from buffer num of msg = "
                        + messageDeliveryInfo.getSizeOfMessageBuffer());
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
     * Check whether there are active subscribers and send
     *
     * @param destination queue name
     * @param messages    metadata set
     * @return how many messages sent
     * @throws Exception
     */
    public int sendMessagesToSubscriptions(String destination, Set<DeliverableAndesMetadata> messages)
            throws Exception {

        if(messages.iterator().hasNext()) {
            //identify if this messages address queues or topics. There CANNOT be a mix
            DeliverableAndesMetadata firstMessage = messages.iterator().next();
            boolean isTopic = firstMessage.isTopic();

            if (isTopic) {
                return topicMessageFlusher.deliverMessageToSubscriptions(destination, messages);
            } else {
                return queueMessageFlusher.deliverMessageToSubscriptions(destination, messages);
            }
        } else {
            if(log.isDebugEnabled()) {
                // This exception can occur because the iterator of ConcurrentSkipListSet loads the
                // at-the-time snapshot.
                // Some records could be deleted by the time the iterator reaches them.
                // However, this can only happen at the tail of the collection, not in middle, and it
                // would cause the loop to blindly check for a batch of deleted records.
                // Given this situation, this loop should break so the sendFlusher can re-trigger it.
                // for tracing purposes can use this : log.warn("NoSuchElementException thrown",ex);
                log.debug("No elements in andes metadata message set.");
            }

            // Since no messages were effected returning 0.
            return 0;
        }
    }

    /**
     * Clear up all the buffered messages for delivery
     * @param destination destination of messages to delete
     */
    public void clearUpAllBufferedMessagesForDelivery(String destination) {
        subscriptionCursar4QueueMap.get(destination).clearReadButUndeliveredMessages();
    }

    /**
     * Schedule to deliver message for the subscription
     *
     * @param subscription subscription to send
     * @param message message to send
     */
    public void scheduleMessageForSubscription(LocalSubscription subscription,
                                               final DeliverableAndesMetadata message) throws AndesException {
        deliverMessageAsynchronously(subscription, message);
    }

    /**
     * Submit the messages to a thread pool to deliver asynchronously
     *
     * @param subscription local subscription
     * @param message      metadata of the message
     */
    public void deliverMessageAsynchronously(LocalSubscription subscription, DeliverableAndesMetadata message)
            throws AndesException {

        if(evaluateDeliveryRules(message)) {
            if(log.isDebugEnabled()) {
                log.debug("Scheduled message id= " + message.getMessageID() + " to be sent to subscription= " + subscription);
            }
            //mark message as came into the subscription for deliver

            message.markAsDispatchedToDeliver(subscription.getChannelID());

            ProtocolMessage protocolMessage = message.generateProtocolDeliverableMessage(subscription.getChannelID());
            flusherExecutor.submit(subscription, protocolMessage);
        }
    }

    /**
     * Re-queue message to andes core. This message will be delivered to
     * any eligible subscriber to receive later. in multiple subscription case this
     * can cause message duplication.
     *
     * @param message message to reschedule
     */
    public void reQueueMessage(DeliverableAndesMetadata message) {
        String destination = message.getDestination();
        subscriptionCursar4QueueMap.get(destination).bufferMessage(message);
    }

    public static MessageFlusher getInstance() {
        return messageFlusher;
    }

    public DisruptorBasedFlusher getFlusherExecutor() {
        return flusherExecutor;
    }
}
