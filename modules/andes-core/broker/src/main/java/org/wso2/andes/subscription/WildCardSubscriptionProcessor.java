/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.andes.subscription;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.AndesSubscription.SubscriptionType;

import java.util.HashSet;
import java.util.Set;

public class WildCardSubscriptionProcessor {

    WildCardBitMapHandler mqttWildCardHandler;
    WildCardBitMapHandler amqpWildCardHandler;

    public WildCardSubscriptionProcessor() throws AndesException {
        mqttWildCardHandler = new WildCardBitMapHandler(SubscriptionType.MQTT);
        amqpWildCardHandler = new WildCardBitMapHandler(SubscriptionType.AMQP);
    }

    public void addWildCardSubscription(AndesSubscription subscription) throws AndesException {
        SubscriptionType subscriptionType = subscription.getSubscriptionType();
        if (SubscriptionType.AMQP == subscriptionType) {
            amqpWildCardHandler.addWildCardSubscription(subscription);
        } else if (SubscriptionType.MQTT == subscriptionType) {
            mqttWildCardHandler.addWildCardSubscription(subscription);
        } else {
            throw new AndesException("Subscription type " + subscriptionType + " is not recognized.");
        }
    }
    public void updateWildCardSubscription(AndesSubscription subscription) throws AndesException  {
        SubscriptionType subscriptionType = subscription.getSubscriptionType();
        if (SubscriptionType.AMQP == subscriptionType) {
            amqpWildCardHandler.updateWildCardSubscription(subscription);
        } else if (SubscriptionType.MQTT == subscriptionType) {
            mqttWildCardHandler.updateWildCardSubscription(subscription);
        } else {
            throw new AndesException("Subscription type " + subscriptionType + " is not recognized.");
        }
    }
    public void removeWildCardSubscription(AndesSubscription subscription) throws AndesException  {
        SubscriptionType subscriptionType = subscription.getSubscriptionType();
        if (SubscriptionType.AMQP == subscriptionType) {
            amqpWildCardHandler.removeWildCardSubscription(subscription);
        } else if (SubscriptionType.MQTT == subscriptionType) {
            mqttWildCardHandler.removeWildCardSubscription(subscription);
        } else {
            throw new AndesException("Subscription type " + subscriptionType + " is not recognized.");
        }
    }

    public Set<AndesSubscription> getMatchingSubscriptions(String destination, SubscriptionType subscriptionType)
            throws AndesException {
        Set<AndesSubscription> subscriptionSet;
        if (SubscriptionType.AMQP == subscriptionType) {
            subscriptionSet = amqpWildCardHandler.getMatchingSubscriptions(destination);
        } else if (SubscriptionType.MQTT == subscriptionType) {
            subscriptionSet = mqttWildCardHandler.getMatchingSubscriptions(destination);
        } else {
            throw new AndesException("Subscription type " + subscriptionType + " is not recognized.");
        }

        return subscriptionSet;
    }

    public Set<AndesSubscription> getActiveClusterSubscribersForNode(String nodeID) {
        Set<AndesSubscription> subscriptions = new HashSet<AndesSubscription>();

        // Get all AMQP subscriptions
        for (AndesSubscription subscription : amqpWildCardHandler.getAllWildCardSubscriptions()) {
            if (subscription.getSubscribedNode().equals(nodeID)) {
                subscriptions.add(subscription);
            }
        }

        // Get all MQTT subscriptions
        for (AndesSubscription subscription : mqttWildCardHandler.getAllWildCardSubscriptions()) {
            if (subscription.getSubscribedNode().equals(nodeID)) {
                subscriptions.add(subscription);
            }
        }

        return subscriptions;
    }
}
