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

import org.apache.commons.lang.StringUtils;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.management.common.mbeans.SubscriptionManagementInformation;
import org.wso2.andes.server.management.AMQManagedObject;

import javax.management.NotCompliantMBeanException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
                Set<AndesSubscription> subscriptions = AndesContext.getInstance().getSubscriptionStore().getAllSubscribersForDestination(queue, false, AndesSubscription.SubscriptionType.AMQP);

                for (AndesSubscription s : subscriptions) {
                    Long pendingMessageCount = MessagingEngine.getInstance().getMessageCountOfQueue(queue);

                    if (!isDurable.equals("*") && (Boolean.parseBoolean(isDurable) != s.isDurable())) {
                        continue;
                    }
                    if (!isActive.equals("*") && (Boolean.parseBoolean(isActive) != s.hasExternalSubscriptions())) {
                        continue;
                    }

                    allSubscriptionsForQueues.add(renderSubscriptionForUI(s,pendingMessageCount.intValue()));
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
            boolean isDurableTopic;
            if("false".equals(isDurable)){
                isDurableTopic = false;
            } else {
                isDurableTopic = true;
            }
            List<String> allSubscriptionsForTopics = new ArrayList<String>();

            List<String> allTopics = AndesContext.getInstance().getSubscriptionStore().getTopics
                    (isDurableTopic);

            for (String topic : allTopics) {

                List<AndesSubscription> subscriptions;
                if (!isDurableTopic) {
                    subscriptions = AndesContext.getInstance().getSubscriptionStore().getAllSubscribersForDestination(topic, true);
                } else {
                    subscriptions = AndesContext.getInstance().getSubscriptionStore()
                            .getAllSubscribersForDestination(topic, false);
                }

                for (AndesSubscription s : subscriptions) {

                    Long pendingMessageCount = MessagingEngine.getInstance().getMessageCountOfQueue(s.getTargetQueue());

                    if (!isDurable.equals("*") && (Boolean.parseBoolean(isDurable) != s.isDurable())) {
                        continue;
                    }
                    if (!isActive.equals("*") && (Boolean.parseBoolean(isActive) != s.hasExternalSubscriptions())) {
                        continue;
                    } if(!s.isBoundToTopic()){
                        continue;
                    }

                    allSubscriptionsForTopics.add(renderSubscriptionForUI(s,pendingMessageCount.intValue()));
                }
            }
            return allSubscriptionsForTopics.toArray(new String[allSubscriptionsForTopics.size()]);

        } catch (Exception e) {
            throw new RuntimeException("Error in accessing subscription information", e);
        }
    }

    @Override
    public int getMessageCount(String subscribedNode, String msgPattern ,String destinationName) {
        try {
            Long messageCount = MessagingEngine.getInstance().getMessageCountOfQueue(destinationName);
            return messageCount.intValue();
        }catch (Exception e) {
            throw new RuntimeException("Error in retrieving pending message count", e);
        }
    }

    /**
     * This method returns the formatted subscription string to be compatible with the UI processor.
     * <p/>
     * Format of the string : "subscriptionInfo =  subscriptionIdentifier |
     * subscribedQueueOrTopicName | subscriberQueueBoundExchange | subscriberQueueName |
     * isDurable | isActive | numberOfMessagesRemainingForSubscriber | subscriberNodeAddress"
     *
     * @param subscription AndesSubscription object that is to be translated to UI view
     * @param pendingMessageCount Number of pending messages of subscription
     * @return String representation of the subscription meta information and pending message count
     */
    private static String renderSubscriptionForUI(AndesSubscription subscription,
                                                  int pendingMessageCount) throws AndesException {


        String nodeId = subscription.getSubscribedNode().split("/")[1];

        if (!StringUtils.isBlank(nodeId)) {

            String subscriptionIdentifier = "1_" + nodeId + "@" + subscription.getTargetQueue();

            //in case of topic whats in v2 is : topicSubscriber.getDestination() + "@" +
            // topicSubscriber.boundTopicName; --
            return subscriptionIdentifier + "|" + subscription.getTargetQueue() + "|" + subscription
                    .getTargetQueueBoundExchangeName() +
                    "|" + subscription.getTargetQueue() + "|" + subscription.isDurable() + "|" +
                    subscription.hasExternalSubscriptions() +
                    "|" + pendingMessageCount + "|" + subscription.getSubscribedNode();
        } else {
            throw new AndesException("Invalid format in Subscriber Node ID : " + subscription
                    .getSubscribedNode() + ". Delimiter should be /.");
        }
    }
}
