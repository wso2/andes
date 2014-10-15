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

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.mqtt.MQTTLocalSubscription;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.AndesSubscriptionManager;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.subscription.BasicSubscription;

/**
 * This class will handle managing delivery threads depending on subscription behaviour
 */
public class MessageDeliveryThreadHandler implements SubscriptionListener {
    private static Log log = LogFactory
            .getLog(MessageDeliveryThreadHandler.class);
    AndesSubscriptionManager subscriptionManager = ClusterResourceHolder.getInstance().getSubscriptionManager();

    @Override
    public void handleClusterSubscriptionsChanged(AndesSubscription subscription, SubscriptionChange changeType) throws AndesException {

    }

    @Override
    public void handleLocalSubscriptionsChanged(LocalSubscription localSubscription, SubscriptionChange changeType) throws AndesException {

        switch (changeType) {
            case Added:
                //if it is a topic subscription, start a topicDeliveryWorker if one has not started
                if (localSubscription.getTargetQueueBoundExchangeName().equals(AMQPUtils.TOPIC_EXCHANGE_NAME) && localSubscription.hasExternalSubscriptions()) {
                    if (!ClusterResourceHolder.getInstance().getTopicDeliveryWorker().isWorking()) {
                        ClusterResourceHolder.getInstance().getTopicDeliveryWorker().setWorking();
                    }
                }

                //if it is a MQTT subscription on this node. Start a topicDeliveryWorker if one has not started
                else if (localSubscription instanceof MQTTLocalSubscription) {
                    if (!ClusterResourceHolder.getInstance().getTopicDeliveryWorker().isWorking()) {
                        ClusterResourceHolder.getInstance().getTopicDeliveryWorker().setWorking();
                    }
                }
                break;
            case Disconnected:
                if (localSubscription.getTargetQueueBoundExchangeName().equals(AMQPUtils.TOPIC_EXCHANGE_NAME) && !localSubscription.isDurable()) {

                    //stop Topic Delivery Worker If Having No active normal (not durable) topic subscriptions on this node
                    if (ClusterResourceHolder.getInstance().getTopicDeliveryWorker() != null) {
                        if (!subscriptionManager.checkIfActiveLocalNonDurableSubscriptionsExists(true)) {
                            ClusterResourceHolder.getInstance().getTopicDeliveryWorker().stopWorking();
                        }
                    }
                }
                break;
            case Deleted:
                if (localSubscription.getTargetQueueBoundExchangeName().equals(AMQPUtils.TOPIC_EXCHANGE_NAME) && !localSubscription.isDurable()) {

                    //stop Topic Delivery Worker If Having No active normal (not durable) topic subscriptions on this node
                    if (ClusterResourceHolder.getInstance().getTopicDeliveryWorker() != null) {
                        if (!subscriptionManager.checkIfActiveLocalNonDurableSubscriptionsExists(true)) {
                            ClusterResourceHolder.getInstance().getTopicDeliveryWorker().stopWorking();
                        }
                    }
                }
                break;
        }

        //if running in standalone mode short-circuit cluster notification
        //we have to create a basic subscription out of local subscription here
        if (!AndesContext.getInstance().isClusteringEnabled()) {
            handleClusterSubscriptionsChanged(new BasicSubscription(localSubscription.encodeAsStr()), changeType);
        }
    }
}
