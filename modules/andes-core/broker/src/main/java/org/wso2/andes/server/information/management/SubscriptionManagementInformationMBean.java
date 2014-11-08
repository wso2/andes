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
package org.wso2.andes.server.information.management;

import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.QueueAddress;
import org.wso2.andes.management.common.mbeans.SubscriptionManagementInformation;
import org.wso2.andes.server.management.AMQManagedObject;

import javax.management.NotCompliantMBeanException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class to Handle data for all subscription related UIs
 */
public class SubscriptionManagementInformationMBean extends AMQManagedObject implements SubscriptionManagementInformation {

    public SubscriptionManagementInformationMBean() throws NotCompliantMBeanException {
        super(SubscriptionManagementInformation.class, SubscriptionManagementInformation.TYPE);
    }

    @Override
    public String getObjectInstanceName() {
        return SubscriptionManagementInformation.TYPE;
    }

    @Override
    public String[] getAllQueueSubscriptions( String isDurable, String isActive) {
        try {
            List<String> allSubscriptionsForQueues = new ArrayList<String>();

            List<String> allQueues = AndesContext.getInstance().getAMQPConstructStore().getQueueNames();

            for (String queue : allQueues) {
                List<AndesSubscription> subscriptions = AndesContext.getInstance().getSubscriptionStore().getAllSubscribersForDestination(queue, false);

                for (AndesSubscription s : subscriptions) {

                    int pendingMessageCount = MessagingEngine.getInstance().getMessageCountOfQueue(queue);

                    if (!isDurable.equals("*") && (Boolean.parseBoolean(isDurable) != s.isDurable())) {
                        continue;
                    }
                    if (!isActive.equals("*") && (Boolean.parseBoolean(isActive) != s.hasExternalSubscriptions())) {
                        continue;
                    }

                    allSubscriptionsForQueues.add(renderSubscriptionForUI(s,pendingMessageCount));
                }
            }
            return allSubscriptionsForQueues.toArray(new String[allSubscriptionsForQueues.size()]);

        } catch (Exception e) {
            throw new RuntimeException("Error in accessing subscription information", e);
        }
    }

    @Override
    public String[] getAllTopicSubscriptions(String isDurable, String isActive) {
        try {
            List<String> allSubscriptionsForTopics = new ArrayList<String>();

            List<String> allTopics = AndesContext.getInstance().getSubscriptionStore().getTopics();

            for (String topic : allTopics) {

                List<AndesSubscription> subscriptions = AndesContext.getInstance().getSubscriptionStore().getAllSubscribersForDestination(topic, true);

                for (AndesSubscription s : subscriptions) {

                    int pendingMessageCount = MessagingEngine.getInstance().getMessageCountOfQueue(s.getTargetQueue());

                    if (!isDurable.equals("*") && (Boolean.parseBoolean(isDurable) != s.isDurable())) {
                        continue;
                    }
                    if (!isActive.equals("*") && (Boolean.parseBoolean(isActive) != s.hasExternalSubscriptions())) {
                        continue;
                    }

                    allSubscriptionsForTopics.add(renderSubscriptionForUI(s,pendingMessageCount));
                }
            }
            return allSubscriptionsForTopics.toArray(new String[allSubscriptionsForTopics.size()]);

        } catch (Exception e) {
            throw new RuntimeException("Error in accessing subscription information", e);
        }
    }

    @Override
    public int getMessageCount(String subscribedNode, String msgPattern ,String destinationName) {
        int messageCount = 0;
        try {
            QueueAddress.QueueType queueType = null;

            if (msgPattern.equals("topic")) {
                queueType = QueueAddress.QueueType.TOPIC_NODE_QUEUE;
                //TODO: implement - hasitha
                messageCount = 0;
            }
            if (msgPattern.equals("queue")) {
                queueType = QueueAddress.QueueType.QUEUE_NODE_QUEUE;
                messageCount =  MessagingEngine.getInstance().getMessageCountOfQueue(destinationName);
            }

        }catch (Exception e) {
            throw new RuntimeException("Error in retrieving pending message count", e);
        }
        return messageCount;
    }

    /**
     * This method returns the formatted subscription string to be compatible with the UI processor.
     * @param subscription
     * @param pendingMessageCount
     * @return
     */
    private static String renderSubscriptionForUI(AndesSubscription subscription, int pendingMessageCount) {

        //  subscriptionInfo =  subscriptionIdentifier |  subscribedQueueOrTopicName | subscriberQueueBoundExchange |
        // subscriberQueueName |  isDurable | isActive | numberOfMessagesRemainingForSubscriber | subscriberNodeAddress

        String nodeId = subscription.getSubscribedNode().split("_")[1];
        String subscriptionIdentifier = "1_"+nodeId+"@"+subscription.getTargetQueue();

        //in case of topic whats in v2 is : topicSubscriber.getDestination() + "@" + topicSubscriber.boundTopicName; --


        return subscriptionIdentifier + "|" + subscription.getTargetQueue() + "|" + subscription.getTargetQueueBoundExchangeName() +
                "|" + subscription.getTargetQueue() + "|" + subscription.isDurable() + "|" + subscription.isBoundToTopic() +
                "|" + pendingMessageCount + "|" + subscription.getSubscribedNode();
    }
}
