/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.kernel.subscription;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextInformationManager;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.MessageHandler;
import org.wso2.andes.kernel.SubscriptionAlreadyExistsException;
import org.wso2.andes.kernel.router.AndesMessageRouter;
import org.wso2.andes.kernel.slot.Slot;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Storage queue represents the bounded queue for subscription. A subscription
 * can only bound to a single storage queue. Given storage queue can bare multiple
 * subscribers (i.e topic scenario)
 */
public class StorageQueue {

    private static Log log = LogFactory.getLog(StorageQueue.class);

    /**
     * Name of the storage queue
     */
    private String name;

    /**
     * This indicates if the messages in the queue should be removed
     * when bounded subscription is closed
     */
    private boolean isDurable;

    /**
     * Indicates if messages should be shared by subscribers bound
     */
    private boolean isShared;

    /**
     * Owner (virtualhost) of the queue. This is used by internal Qpid
     */
    private String queueOwner;

    /**
     * If the queue is exclusive. This property is used by internal Qpid
     */
    private boolean isExclusive;

    /**
     * Added to infer the state of the queue during concurrent message delivery.
     * Initial value before the first purge within this server session should be 0.
     */
    private Long lastPurgedTimestamp;

    private AndesMessageRouter messageRouter;

    private String messageRouterBindingKey;

    private List<AndesSubscription> boundedSubscriptions;

    /**
     * Handler for messages which handles buffering, persisting and reading messages for queue
     */
    private MessageHandler messageHandler;


    /**
     * Create a storage queue instance. This instance MUST be registered at StorageQueueRegistry.
     *
     * @param name        name of the storage queue
     * @param isDurable   indicate if queue should be preserved
     *                    on subscription close
     * @param isShared    Indicates if messages should be shared by subscribers bound. If true messages will be
     *                    distributed round-robin between bound subscribers
     * @param queueOwner  owner of the queue (virtualhost). This is needed for internal Qpid.
     * @param isExclusive is the queue exclusive. This is needed for internal Qpid.
     */
    public StorageQueue(String name, boolean isDurable, boolean isShared, String queueOwner, boolean isExclusive) {
        this.name = name;
        this.isDurable = isDurable;
        this.isShared = isShared;
        this.queueOwner = queueOwner;
        this.isExclusive = isExclusive;
        this.lastPurgedTimestamp = 0L;
        this.boundedSubscriptions = new ArrayList<>(1);
        this.messageHandler = new MessageHandler(name);
    }

    /**
     * create an instance of andes queue
     *
     * @param queueAsStr queue information as encoded string
     */
    public StorageQueue(String queueAsStr) {
        String[] propertyToken = queueAsStr.split(",");
        for (String pt : propertyToken) {
            String[] tokens = pt.split("=");
            switch (tokens[0]) {
                case "queueName":
                    this.name = tokens[1];
                    break;
                case "queueOwner":
                    this.queueOwner = tokens[1].equals("null") ? null : tokens[1];
                    break;
                case "isExclusive":
                    this.isExclusive = Boolean.parseBoolean(tokens[1]);
                    break;
                case "isDurable":
                    this.isDurable = Boolean.parseBoolean(tokens[1]);
                    break;
                case "isShared":
                    this.isShared = Boolean.parseBoolean(tokens[1]);
                    break;
                case "lastPurgedTimestamp":
                    this.lastPurgedTimestamp = Long.parseLong(tokens[1]);
                    break;
            }
        }
        this.boundedSubscriptions = new ArrayList<>(1);
    }

    public String encodeAsString() {
        return "queueName=" + name +
                ",queueOwner=" + queueOwner +
                ",isExclusive=" + isExclusive +
                ",isDurable=" + isDurable +
                ",isShared=" + isShared +
                ",lastPurgedTimestamp=" + lastPurgedTimestamp;
    }

    public String toString() {
        return encodeAsString();
    }

    /**
     * Get name of the queue
     *
     * @return name of the queue
     */
    public String getName() {
        return name;
    }

    /**
     * Get if queue is durable. If true
     * queue is not removed when all subscriptions bound is detached.
     *
     * @return true if queue is durable
     */
    public boolean isDurable() {
        return isDurable;
    }

    /**
     * Get if queue is shared between subscribers bound to it. If shared,
     * messages should be distributed in round-robin manner to subscriptions
     *
     * @return true if shared
     */
    public boolean isShared() {
        return isShared;
    }

    /**
     * Get owner of the queue.
     *
     * @return queue owner
     */
    public String getQueueOwner() {
        return queueOwner;
    }

    /**
     * Get if queue is exclusive
     *
     * @return true if queue is exclusive
     */
    public boolean isExclusive() {
        return isExclusive;
    }

    /**
     * Get last purged timestamp of the queue
     *
     * @return time last purge is performed
     */
    public Long getLastPurgedTimestamp() {
        return lastPurgedTimestamp;
    }

    /**
     * Add a binding for queue. Bind it to given router by given binding key
     *
     * @param bindingKey binding key
     * @param router     message router to bind the queue
     * @throws AndesException
     */
    public void bindQueueToMessageRouter(String bindingKey, AndesMessageRouter router) throws AndesException {
        this.messageRouterBindingKey = bindingKey;
        this.messageRouter = router;
        router.addMapping(bindingKey, this);
    }

    /**
     * Unbind queue from bound message router
     *
     * @throws AndesException
     */
    public void unbindQueueFromMessageRouter() throws AndesException {
        if (null != messageRouterBindingKey) {
            messageRouter.removeMapping(messageRouterBindingKey, this);
            this.messageRouterBindingKey = null;
            this.messageRouter = null;
        }
    }

    /**
     * Get subscriptions attached to the queue
     *
     * @return list of subscriptions
     */
    public List<AndesSubscription> getBoundSubscriptions() {
        return boundedSubscriptions;
    }

    /**
     * Add a subscription for the queue. Binding key of the subscriber
     * should match with the binding key by which queue is bound to message router
     *
     * @param subscription           subscription to add
     * @param routingKeyOfSubscriber binding key of the subscriber.
     * @throws AndesException
     */
    public void bindSubscription(AndesSubscription subscription, String routingKeyOfSubscriber) throws
            AndesException {

        if ((AMQPUtils.TOPIC_EXCHANGE_NAME.equals(messageRouter.getName())) && isDurable) {

            if (!messageRouterBindingKey.equals(routingKeyOfSubscriber)) {
                throw new SubscriptionAlreadyExistsException("An subscription already exists with same "
                        + "subscription ID " + name + " bound to a different topic "
                        + messageRouterBindingKey);
            }

            boolean allowSharedSubscribers = AndesConfigurationManager
                    .readValue(AndesConfiguration.ALLOW_SHARED_SHARED_SUBSCRIBERS);

            if (!allowSharedSubscribers) {

                //check cluster-wide if an active subscription available
                Iterable<AndesSubscription> existingSubscribers = AndesContext.getInstance()
                        .getAndesSubscriptionManager()
                        .getAllSubscriptionsByQueue(subscription.getProtocolType(), name);

                Iterator<AndesSubscription> subscriptionIterator = existingSubscribers.iterator();

                if (subscriptionIterator.hasNext()) {
                    //An active subscription already exists. Creating another is not permitted
                    AndesSubscription existingSubscriber = subscriptionIterator.next();
                    throw new SubscriptionAlreadyExistsException("An active subscription already exists with same "
                            + "subscription id " + existingSubscriber.getSubscriptionId()
                            + " bound to queue " + name + " by routing key " + messageRouterBindingKey);
                }
            }
        }
        boundedSubscriptions.add(subscription);
        messageHandler.startMessageDelivery(this);
    }

    /**
     * Unbind subscription from queue. This will purge the messages of the queue
     * if queue is not durable and no subscribers are further bound to the queue.
     * Also slots will be released back to coordinator if this is the last bound
     * subscriber. Only locally connected subscribers can be unbound. If subscription
     * is not already bound, nothing will be changed.
     *
     * @param subscription AndesSubscription to unbind
     * @throws AndesException
     */
    public void unbindSubscription(AndesSubscription subscription) throws AndesException {
        boundedSubscriptions.remove(subscription);
        if (boundedSubscriptions.isEmpty()) {
            if (isDurable) {
                //return slots back to coordinator
                messageHandler.releaseAllSlots();
                messageHandler.clearReadButUndeliveredMessages();
            } else {
                AndesContextInformationManager contextInformationManager = AndesContext.getInstance()
                                                                                       .getAndesContextInformationManager();
                contextInformationManager.deleteQueue(this);
            }
            messageHandler.stopMessageDelivery(this);
        } else {
            subscription.rebufferUnackedMessages();
        }
    }

    /**
     * Get message router bound to queue
     *
     * @return get AndesMessageRouter queue is bound. Return null if not bound to any
     */
    public AndesMessageRouter getMessageRouter() {
        return messageRouter;
    }

    /**
     * Get binding key by which queue is bound to the router
     *
     * @return binding key
     */
    public String getMessageRouterBindingKey() {
        return messageRouterBindingKey;
    }

    /**
     * Read messages from persistent storage for delivery
     *
     * @param messageSlot message slot whose messages should be read
     */
    public void loadMessagesForDelivery(Slot messageSlot) throws AndesException {
        messageHandler.bufferMessages(messageSlot);
    }

    /**
     * Buffer some external message to storage queue. This message
     * will be delivered to subscriptions eventually
     *
     * @param message message to be buffered for delivery
     */
    public void bufferMessageForDelivery(DeliverableAndesMetadata message) {
        messageHandler.bufferMessage(message);
    }

    /**
     * Get messages for delivery. This call returns the messages that are buffered
     * for delivery (read by slots - loadMessagesForDelivery())
     *
     * @return Collection of messages
     */
    public Collection<DeliverableAndesMetadata> getMessagesForDelivery() {
        return messageHandler.getReadButUndeliveredMessages();
    }

    //TODO: is there a workaround to remove this call?

    /**
     * Delete a specific message slot that is read by the queue
     *
     * @param slotToDelete slot to delete
     */
    public void deleteSlot(Slot slotToDelete) {
        messageHandler.deleteSlot(slotToDelete);
    }

    /**
     * Check if storage queue read message buffer is not reached
     * the limit. This should be checked before calling
     * loadMessagesForDelivery(Slot messageSlot)
     *
     * @return true if buffer has space
     */
    public boolean checkForReadMessageBufferLimit() {
        return messageHandler.messageBufferHasRoom();
    }


    /**
     * Clear all messages that read to buffer for delivery. This call
     * will clear up all read slots so far as well.
     *
     * @return number of messages removed from memory
     */
    public int clearMessagesReadToBufferForDelivery() {
        return messageHandler.clearReadButUndeliveredMessages();
    }

    /**
     * Purge the messages in the queue. This will remove all the messages from read buffer
     * delete the slots and delete all messages from persistent storage
     *
     * @return how many messages were removed from persistent storage
     * @throws AndesException
     */
    public int purgeMessages() throws AndesException {
        lastPurgedTimestamp = System.currentTimeMillis();
        log.info("Purging messages of queue " + name);
        return messageHandler.purgeMessagesOfQueue();
    }

    /**
     * Dump all message status of the slots read to this storage queue
     *
     * @param fileToWrite File to write information
     * @throws AndesException
     */
    public void dumpAllSlotInformationToFile(File fileToWrite) throws AndesException {
        messageHandler.dumpAllSlotInformationToFile(fileToWrite, name);
    }

    /**
     * Get message count for queue
     *
     * @return message count of the queue
     * @throws AndesException
     */
    public long getMessageCount() throws AndesException {
        return messageHandler.getMessageCountForQueue();
    }


    public boolean equals(Object o) {
        if (o instanceof StorageQueue) {
            StorageQueue c = (StorageQueue) o;
            if (this.name.equals(c.name)) {
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return new HashCodeBuilder(17, 31).
                append(name).
                toHashCode();
    }

}
