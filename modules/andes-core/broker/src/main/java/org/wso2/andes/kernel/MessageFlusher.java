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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.configuration.BrokerConfigurationService;
import org.wso2.andes.configuration.util.TopicMessageDeliveryStrategy;
import org.wso2.andes.kernel.disruptor.delivery.DisruptorBasedFlusher;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.StorageQueue;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


/**
 * <code>MessageFlusher</code> Handles the task of polling the user queues and flushing the
 * messages to subscribers There will be one Flusher per Queue Per Node
 */
public class MessageFlusher {

    private static Log log = LogFactory.getLog(MessageFlusher.class);

    private final DisruptorBasedFlusher flusherExecutor;

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
     * Head of the delivery message responsibility chain
     */
    private DeliveryResponsibility deliveryResponsibilityHead;


    private static MessageFlusher messageFlusher = new MessageFlusher();

    /**
     * Get Singleton object
     * @return MessageFlusher of the broker
     */
    public static MessageFlusher getInstance() {
        return messageFlusher;
    }

    private MessageFlusher() {

        flusherExecutor = new DisruptorBasedFlusher();

        //set queue message flusher
        this.queueMessageFlusher = new FlowControlledQueueMessageDeliveryImpl();

        //set topic message flusher
        TopicMessageDeliveryStrategy topicMessageDeliveryStrategy = BrokerConfigurationService.getInstance()
                .getBrokerConfiguration().getPerformanceTuning().getDelivery().getTopicMessageDeliveryStrategy().getStrategyName();

        if(topicMessageDeliveryStrategy.equals(TopicMessageDeliveryStrategy.DISCARD_ALLOWED)
                || topicMessageDeliveryStrategy.equals(TopicMessageDeliveryStrategy.DISCARD_NONE)) {

            this.topicMessageFlusher =
                    new NoLossBurstTopicMessageDeliveryImpl();

        } else if(topicMessageDeliveryStrategy.equals(TopicMessageDeliveryStrategy.SLOWEST_SUB_RATE)) {

            this.topicMessageFlusher =
                    new SlowestSubscriberTopicMessageDeliveryImpl();
        }

        initializeDeliveryResponsibilityComponents();

    }

    /**
     * Initialize the delivery filter chain
     */
    private void initializeDeliveryResponsibilityComponents() {
        //assign the head of the handler chain
        deliveryResponsibilityHead = new PurgedMessageHandler();
        ExpiredMessageHandler expiredMessageHandler = new ExpiredMessageHandler();
        //link the second handler to the head
        deliveryResponsibilityHead.setNextDeliveryFilter(expiredMessageHandler);
        //link the third handler
        expiredMessageHandler.setNextDeliveryFilter(new DeliveryMessageHandler());

        int preDeliveryDeletionTaskScheduledPeriod = BrokerConfigurationService.getInstance().getBrokerConfiguration()
                .getPerformanceTuning().getMessageExpiration().getPreDeliveryExpiryDeletionInterval();
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("ExpiryMessageDeletionTask-%d")
                .build();
        //executor service for pre delivery deletion task
        ScheduledExecutorService expiryMessageDeletionTaskScheduler =
                Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
        //pre-delivery deletion task initialization
        PreDeliveryExpiryMessageDeletionTask preDeliveryExpiryMessageDeletionTask =
                new PreDeliveryExpiryMessageDeletionTask();
        //Set the expiry message deletion task to the expired message handler
        expiredMessageHandler.setExpiryMessageDeletionTask(preDeliveryExpiryMessageDeletionTask);
        //schedule the task at the specified intervals
        expiryMessageDeletionTaskScheduler.scheduleAtFixedRate(preDeliveryExpiryMessageDeletionTask,
                preDeliveryDeletionTaskScheduledPeriod, preDeliveryDeletionTaskScheduledPeriod, TimeUnit.SECONDS);

    }

    /**
     * Check whether there are active subscribers and send
     *
     * @param storageQueue storage queue of messages
     * @return how many messages sent
     * @throws AndesException
     */
    public int sendMessagesToSubscriptions(StorageQueue storageQueue) throws AndesException {
        int noOfSentMessages;
        if ((!storageQueue.isDurable()) &&
                (storageQueue.getMessageRouter().getName().equals(AMQPUtils.TOPIC_EXCHANGE_NAME))) {

            noOfSentMessages = topicMessageFlusher.
                    deliverMessageToSubscriptions(storageQueue);

        } else {
            noOfSentMessages = queueMessageFlusher.
                    deliverMessageToSubscriptions(storageQueue);
        }
        return noOfSentMessages;
    }

    /**
     * Schedule to deliver message for the subscription
     *
     * @param subscription subscription to send
     * @param message message to send
     */
    public void scheduleMessageForSubscription(AndesSubscription subscription,
                         final DeliverableAndesMetadata message) throws AndesException {
        deliverMessageAsynchronously(subscription, message);
    }

    /**
     * Submit the messages to a thread pool to deliver asynchronously
     *
     * @param subscription local subscription
     * @param message      metadata of the message
     */
    public void deliverMessageAsynchronously(AndesSubscription subscription, DeliverableAndesMetadata message)
            throws AndesException {

        deliveryResponsibilityHead.handleDeliveryMessage(subscription, message);
    }

    /**
     * Stop disruptor based message delivery. This will process all taken in events
     * and then shutdown. Make sure to stop all incoming events to outbound disruptor
     * before calling this method.
     */
    public void stopMessageFlusher() {
        flusherExecutor.stop();
    }

    public DisruptorBasedFlusher getFlusherExecutor() {
        return flusherExecutor;
    }



}
