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


import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.disruptor.inbound.InboundSubscriptionEvent;

import java.util.UUID;

/**
 * Factory for creating AndesSubscriptions. When generating a subscription if it is a local
 * one subscription ID should be generated. Otherwise, (for cluster notifications)
 * use subscription ID coming with subscription event
 */
public class AndesSubscriptionFactory {

    /**
     * Validate and create an AndesSubscription for inbound subscription event
     *
     * @param subscriptionEvent incoming subscription event
     * @return created AndesSubscription by Factory
     */
    public AndesSubscription createLocalSubscription(InboundSubscriptionEvent subscriptionEvent,
                                                     StorageQueue queueToBind) throws SubscriptionException {

        AndesSubscription subscriptionAdded;
        ProtocolType protocol = subscriptionEvent.getProtocol();
        SubscriberConnection subscriberConnection = subscriptionEvent.getSubscriber();
        //For durable topic subscriptions
        //TODO:clientID+SubscriptionID?
        String subscriptionIdentifier = subscriptionEvent.getSubscriptionIdentifier();

        if (null == queueToBind) {
            throw new SubscriptionException("Storage queue " + subscriptionEvent.getBoundStorageQueueName() + " is "
                    + "not found in local queue registry. Cannot bind the subscription id= " + subscriptionIdentifier);
        }

        String subscriptionID = UUID.randomUUID().toString();

        subscriptionAdded = createSubscription(subscriptionID, subscriptionIdentifier,
                queueToBind, protocol, subscriberConnection);

        return subscriptionAdded;
    }

    /**
     * Create a AndesSubscription instance
     *
     * @param subscriptionId         Id of the subscription
     * @param subscriptionIdentifier Subscription ID of the subscriber (for durable topic subscriptions)
     * @param storageQueue           bound queue
     * @param protocol               protocol by which subscription is created
     * @param subscriberConnection   connection of the subscriber
     * @return AndesSubscription     created subscription
     * @throws SubscriptionException if error occurred
     */
    private AndesSubscription createSubscription(String subscriptionId, String subscriptionIdentifier,
                                                 StorageQueue storageQueue,
                                                 ProtocolType protocol,
                                                 SubscriberConnection subscriberConnection)
            throws SubscriptionException {

        String messageRouterName = storageQueue.getMessageRouter().getName();
        boolean isBoundQueueDurable = storageQueue.isDurable();

        if(messageRouterName.equals(AMQPUtils.TOPIC_EXCHANGE_NAME)
                && isBoundQueueDurable) {

            return new DurableTopicSubscriber(subscriptionId, subscriptionIdentifier, storageQueue,
                            protocol, subscriberConnection);

        } else {

            return new AndesSubscription(subscriptionId, storageQueue, protocol,
                    subscriberConnection);
        }

    }

}
