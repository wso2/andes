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
import org.wso2.andes.server.util.AndesUtils;

import java.util.UUID;

/**
 * This represents Basic Andes Subscription. Any type of subscription
 * (AMQP,MQTT) is inherited from this template
 */
public class BasicSubscription implements AndesSubscription {
    protected String subscriptionID;
    protected String destination;
    protected boolean isBoundToTopic;
    protected boolean isExclusive;
    protected boolean isDurable;
    protected String subscribedNode;
    protected String storageQueueName;
    protected String targetQueue;
    protected String targetQueueOwner;
    protected String targetQueueBoundExchange;
    protected String targetQueueBoundExchangeType;
    protected Short isTargetQueueBoundExchangeAutoDeletable;
    protected boolean hasExternalSubscriptions;


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
     * @param subscribedNode                          node queue information where actual subscription lies
     * @param targetQueue                             to which queue subscription is bound
     * @param targetQueueOwner                        owner of the subscribed queue
     * @param targetQueueBoundExchange                name of one of exchanges to which queue of the subscriber is bound
     * @param targetQueueBoundExchangeType            type of one of exchanges to which queue of the subscriber is bound
     * @param isTargetQueueBoundExchangeAutoDeletable type of one of exchanges to which queue of the subscriber is bound
     * @param hasExternalSubscriptions                is this subscription entry is active (having a live TCP connection)
     */
    public BasicSubscription(String subscriptionID, String destination,
                             boolean isBoundToTopic, boolean isExclusive, boolean isDurable,
                             String subscribedNode, String targetQueue, String targetQueueOwner, String targetQueueBoundExchange,
                             String targetQueueBoundExchangeType, Short isTargetQueueBoundExchangeAutoDeletable, boolean hasExternalSubscriptions) {

        super();
        this.subscriptionID = subscriptionID;
        if (subscriptionID == null) {
            this.subscriptionID = UUID.randomUUID().toString();
        }
        //TODO: hasitha - need to handle hierarchical topic case
        this.destination = destination;
        this.isBoundToTopic = isBoundToTopic;
        this.isExclusive = isExclusive;
        this.isDurable = isDurable;
        this.subscribedNode = subscribedNode;
        this.targetQueue = targetQueue;
        this.targetQueueOwner = targetQueueOwner;
        this.targetQueueBoundExchange = targetQueueBoundExchange;
        this.targetQueueBoundExchangeType = targetQueueBoundExchangeType;
        this.isTargetQueueBoundExchangeAutoDeletable = isTargetQueueBoundExchangeAutoDeletable;
        this.hasExternalSubscriptions = hasExternalSubscriptions;
        setStorageQueueName();
    }

    @Override
    public String getSubscriptionID() {
        return subscriptionID;
    }

    @Override
    public String getSubscribedDestination() {
        return destination;
    }

    @Override
    public boolean isBoundToTopic() {
        return isBoundToTopic;
    }

    @Override
    public boolean isDurable() {
        return isDurable;
    }

    @Override
    public String getSubscribedNode() {
        return subscribedNode;
    }

    public boolean isExclusive() {
        return isExclusive;
    }

    public void setExclusive(boolean isExclusive) {
        this.isExclusive = isExclusive;
    }

    public String getTargetQueue() {
        return targetQueue;
    }

    public String getStorageQueueName() {
        return storageQueueName;
    }

    @Override
    public String getTargetQueueOwner() {
        return targetQueueOwner;
    }

    @Override
    public String getTargetQueueBoundExchangeName() {
        return targetQueueBoundExchange;
    }

    @Override
    public String getTargetQueueBoundExchangeType() {
        return targetQueueBoundExchangeType;
    }

    @Override
    public Short ifTargetQueueBoundExchangeAutoDeletable() {
        return isTargetQueueBoundExchangeAutoDeletable;
    }

    @Override
    public boolean hasExternalSubscriptions() {
        return hasExternalSubscriptions;
    }


    @Override
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("[").append(destination)
                .append("]ID=").append(subscriptionID)
                .append("@").append(subscribedNode)
                .append("/D=").append(isDurable)
                .append("/X=").append(isExclusive)
                .append("/O=").append(targetQueueOwner)
                .append("/E=").append(targetQueueBoundExchange)
                .append("/ET=").append(targetQueueBoundExchangeType)
                .append("/EUD=").append(isTargetQueueBoundExchangeAutoDeletable)
                .append("/S=").append(hasExternalSubscriptions);
        return buf.toString();
    }

    @Override
    public String encodeAsStr() {
        StringBuffer buf = new StringBuffer();
        buf.append("subscriptionID=").append(subscriptionID)
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
                .append(",hasExternalSubscriptions=").append(hasExternalSubscriptions);
        return buf.toString();
    }

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
        if(isBoundToTopic && !isDurable) {  // for normal topic subscriptions
            storageQueueName = AndesUtils.TOPIC_NODE_QUEUE_PREFIX + "|" + destination + "|" + subscribedNode;
        } else if(isBoundToTopic && isDurable) {  //for durable topic subscriptions
            storageQueueName = targetQueue;
        } else { //For queue subscriptions. This is a must. Otherwise queue will not be shared among nodes
            storageQueueName = destination;
        }
    }

}
