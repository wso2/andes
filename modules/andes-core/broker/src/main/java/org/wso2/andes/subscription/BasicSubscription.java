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

package org.wso2.andes.subscription;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.AndesUtils;

import java.util.UUID;

/**
 * This represents Basic Andes Subscription. Any type of subscription
 * (AMQP,MQTT) is inherited from this template
 */
public class BasicSubscription implements AndesSubscription {
    // The id of the subscriber cluster wide this will be unique - MANDOTORY
    protected String subscriptionID;
    // The target queue or topic name - MANDOTORY
    protected String destination;
    // Whether the subscription is bound to topic- MANDOTORY
    protected boolean isBoundToTopic;
    //If the internal queue for the subscription have other subscriptions bound to it other than its own - OPTIONAL
    protected boolean isExclusive;
    // Durability of the subscription - MANDOTORY
    protected boolean isDurable;
    // The name of the node in the cluster where the subscription is bound - MANDOTORY
    protected String subscribedNode;
    //Time of subscription creation/disconnection/deletion - MANDATORY
    protected long subscribeTime;
    // If the binding is non durable (topic) then the name would be prfix+destination+nodeID - INTERNALLY CONSTRUCTED
    protected String storageQueueName;
    //non durable topics this value will be null. In other cases ex queues and durable topics we need to have this - OPTIONAL
    protected String targetQueue;
    //This will be used for security purposes defines the creator of the queue - OPTIONAL
    protected String targetQueueOwner;
    // This will be AMQP specific - OPTIONAL
    protected String targetQueueBoundExchange;
    // This will be AMQP specific - OPTIONAL
    protected String targetQueueBoundExchangeType;
    // This will be AMQP specific - OPTIONAL
    protected Short isTargetQueueBoundExchangeAutoDeletable;
    //whether the subscription is online or offline - MANDOTORY
    protected boolean hasExternalSubscriptions;

    // The subscription type the basic subscription belongs to, basically this represents the protocol - OPTIONAL
    private SubscriptionType subscriptionType;


    /**
     * Create a basic subscription instance from encoded info
     *
     * @param subscriptionAsStr encoded info as string
     */
    public BasicSubscription(String subscriptionAsStr) {
        String[] propertyToken = subscriptionAsStr.split(",");
        for (String pt : propertyToken) {
            String[] tokens = pt.split("=");
            if (tokens[0].equals("subscriptionID")) {
                this.subscriptionID = tokens[1];
            } else if (tokens[0].equals("destination")) {
                this.destination = tokens[1];
            } else if (tokens[0].equals("isBoundToTopic")) {
                this.isBoundToTopic = Boolean.parseBoolean(tokens[1]);
            } else if (tokens[0].equals("isExclusive")) {
                this.isExclusive = Boolean.parseBoolean(tokens[1]);
            } else if (tokens[0].equals("isDurable")) {
                this.isDurable = Boolean.parseBoolean(tokens[1]);
            } else if (tokens[0].equals("subscribedNode")) {
                this.subscribedNode = tokens[1];
            } else if (tokens[0].equals("subscribedTime")) {
                this.subscribeTime = Long.parseLong(tokens[1]);
            } else if (tokens[0].equals("targetQueue")) {
                this.targetQueue = tokens[1];
            } else if (tokens[0].equals("targetQueueOwner")) {
                this.targetQueueOwner = tokens[1].equals("null") ? null : tokens[1];
            } else if (tokens[0].equals("targetQueueBoundExchange")) {
                this.targetQueueBoundExchange = tokens[1].equals("null") ? null : tokens[1];
            } else if (tokens[0].equals("targetQueueBoundExchangeType")) {
                this.targetQueueBoundExchangeType = tokens[1].equals("null") ? null : tokens[1];
            } else if (tokens[0].equals("isTargetQueueBoundExchangeAutoDeletable")) {
                this.isTargetQueueBoundExchangeAutoDeletable = tokens[1].equals("null") ? null : Short.parseShort(tokens[1]);
            } else if (tokens[0].equals("hasExternalSubscriptions")) {
                this.hasExternalSubscriptions = Boolean.parseBoolean(tokens[1]);
            } else if (tokens[0].equals("subscriptionType")) {
                this.subscriptionType = SubscriptionType.valueOf(tokens[1]);
                // Will automatically throw an IllegalArgumentException if the value does not match to any
                // SubscriptionType
            } else {
                if (tokens[0].trim().length() > 0) {
                    throw new UnsupportedOperationException("Unexpected token " + tokens[0]);
                }
            }
        }

        setStorageQueueName();
    }

    /**
     * create an instance of Basic subscription
     *
     * @param subscriptionID                          id of the subscription
     * @param destination                             subscribed destination (queue/topic name)
     * @param isBoundToTopic                          is subscription for topic
     * @param isExclusive                             is this an exclusive subscription
     * @param isDurable                               is this an durable subscription
     * @param subscribedNode                          node information where actual subscription lies
     * @param subscribeTime                           Timestamp in milliseconds subscription is created
     * @param targetQueue                             to which queue subscription is bound
     * @param targetQueueOwner                        owner of the subscribed queue
     * @param targetQueueBoundExchange                name of one of exchanges to which queue of the subscriber is bound
     * @param targetQueueBoundExchangeType            type of one of exchanges to which queue of the subscriber is bound
     * @param isTargetQueueBoundExchangeAutoDeletable type of one of exchanges to which queue of the subscriber is bound
     * @param hasExternalSubscriptions                is this subscription entry is active (having a live TCP connection)
     */
    public BasicSubscription(String subscriptionID, String destination,
                             boolean isBoundToTopic, boolean isExclusive, boolean isDurable,
                             String subscribedNode, long subscribeTime, String targetQueue, String targetQueueOwner, String targetQueueBoundExchange,
                             String targetQueueBoundExchangeType, Short isTargetQueueBoundExchangeAutoDeletable, boolean hasExternalSubscriptions) {

        super();
        this.subscriptionID = subscriptionID;

        //TODO this is random, need to get this id from one place
        if (subscriptionID == null) {
            this.subscriptionID = UUID.randomUUID().toString();
        }
        this.destination = destination;
        this.isBoundToTopic = isBoundToTopic;
        this.isExclusive = isExclusive;
        this.isDurable = isDurable;
        this.subscribedNode = subscribedNode;
        this.subscribeTime = subscribeTime;
        this.targetQueue = targetQueue;
        this.targetQueueOwner = targetQueueOwner;
        this.targetQueueBoundExchange = targetQueueBoundExchange;
        this.targetQueueBoundExchangeType = targetQueueBoundExchangeType;
        this.isTargetQueueBoundExchangeAutoDeletable = isTargetQueueBoundExchangeAutoDeletable;
        this.hasExternalSubscriptions = hasExternalSubscriptions;
        setStorageQueueName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSubscriptionType(SubscriptionType subscriptionType) {
        this.subscriptionType = subscriptionType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSubscriptionID() {
        return subscriptionID;
    }

    @Override
    public String getSubscribedDestination() {
        return destination;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isBoundToTopic() {
        return isBoundToTopic;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDurable() {
        return isDurable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSubscribedNode() {
        return subscribedNode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getSubscribeTime() {
        return subscribeTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isExclusive() {
        return isExclusive;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setExclusive(boolean isExclusive) {
        this.isExclusive = isExclusive;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTargetQueue() {
        return targetQueue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getStorageQueueName() {
        return storageQueueName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTargetQueueOwner() {
        return targetQueueOwner;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTargetQueueBoundExchangeName() {
        return targetQueueBoundExchange;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTargetQueueBoundExchangeType() {
        return targetQueueBoundExchangeType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Short ifTargetQueueBoundExchangeAutoDeletable() {
        return isTargetQueueBoundExchangeAutoDeletable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasExternalSubscriptions() {
        return hasExternalSubscriptions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setHasExternalSubscriptions(boolean hasExternalSubscription) {
        this.hasExternalSubscriptions = hasExternalSubscription;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" + destination +
               "]ID=" + subscriptionID +
               "@" + subscribedNode +
               "/T=" + subscribeTime +
               "/D=" + isDurable +
               "/X=" + isExclusive +
               "/O=" + targetQueueOwner
               + "/E=" + targetQueueBoundExchange +
               "/ET=" + targetQueueBoundExchangeType +
               "/EUD=" + isTargetQueueBoundExchangeAutoDeletable +
               "/S=" + hasExternalSubscriptions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String encodeAsStr() {
        StringBuilder builder = new StringBuilder();
        builder.append("subscriptionID=").append(subscriptionID)
                .append(",destination=").append(destination)
                .append(",isBoundToTopic=").append(isBoundToTopic)
                .append(",isExclusive=").append(isExclusive)
                .append(",isDurable=").append(isDurable)
                .append(",targetQueue=").append(targetQueue)
                .append(",targetQueueOwner=").append(targetQueueOwner)
                .append(",targetQueueBoundExchange=").append(targetQueueBoundExchange)
                .append(",targetQueueBoundExchangeType=").append(targetQueueBoundExchangeType)
                .append(",isTargetQueueBoundExchangeAutoDeletable=").append(isTargetQueueBoundExchangeAutoDeletable)
                .append(",subscribedNode=").append(subscribedNode)
                .append(",subscribedTime=").append(subscribeTime)
                .append(",hasExternalSubscriptions=").append(hasExternalSubscriptions);

        if (subscriptionType != null) {
            builder.append(",subscriptionType=").append(subscriptionType);
        }

        return builder.toString();
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object o) {
        if (o instanceof BasicSubscription) {
            BasicSubscription c = (BasicSubscription) o;
            if (this.subscriptionID.equals(c.subscriptionID) &&
                    this.getSubscribedNode().equals(c.getSubscribedNode()) &&
                    this.targetQueue.equals(c.targetQueue) &&
                    this.targetQueueBoundExchange.equals(c.targetQueueBoundExchange)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode() {
        return new HashCodeBuilder(17, 31).
                append(subscriptionID).
                append(getSubscribedNode()).
                append(targetQueue).
                append(targetQueueBoundExchange).
                toHashCode();
    }

    /**
     * Set storage queue name. Slot delivery worker will refer this name
     */
    private void setStorageQueueName() {
        if (isBoundToTopic && !isDurable) {  // for normal topic subscriptions
            storageQueueName = AndesUtils.getStorageQueueForDestination(destination, subscribedNode, true);
        } else if (isBoundToTopic) {  //for durable topic subscriptions
            storageQueueName = AndesUtils.getStorageQueueForDestination(targetQueue, subscribedNode, false);
        } else { //For queue subscriptions. This is a must. Otherwise queue will not be shared among nodes
            storageQueueName = AndesUtils.getStorageQueueForDestination(targetQueue, subscribedNode, false);
        }
    }
}
