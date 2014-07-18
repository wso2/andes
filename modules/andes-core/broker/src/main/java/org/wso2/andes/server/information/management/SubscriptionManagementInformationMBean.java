package org.wso2.andes.server.information.management;

import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.QueueAddress;
import org.wso2.andes.kernel.Subscrption;
import org.wso2.andes.management.common.mbeans.SubscriptionManagementInformation;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.management.AMQManagedObject;

import javax.management.NotCompliantMBeanException;
import java.util.ArrayList;
import java.util.List;

/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
    public String[] getAllQueueSubscriptions(boolean isDurable, boolean isActive, boolean isLocal) {
        try {
            List<String> allSubscriptionsForQueues = new ArrayList<String>();

            List<String> allQueues = AndesContext.getInstance().getSubscriptionStore().listQueues();

            for (String queue : allQueues) {
                List<Subscrption> subscriptions = AndesContext.getInstance().getSubscriptionStore().getActiveClusterSubscribersForDestination(queue, false);

                for (Subscrption s : subscriptions) {

                    QueueAddress nodeQueueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE,s.getSubscribedNode());

                    int pendingMessageCount = MessagingEngine.getInstance().getDurableMessageStore().countMessagesOfQueue(nodeQueueAddress,queue);

                    allSubscriptionsForQueues.add(renderSubscriptionForUI(s,pendingMessageCount));
                }
            }
            return allSubscriptionsForQueues.toArray(new String[allSubscriptionsForQueues.size()]);

        } catch (Exception e) {
            throw new RuntimeException("Error in accessing subscription information", e);
        }
    }

    @Override
    public String[] getAllTopicSubscriptions(boolean isDurable, boolean isActive, boolean isLocal) {
        try {
            List<String> allSubscriptionsForTopics = new ArrayList<String>();

            List<String> allTopics = AndesContext.getInstance().getSubscriptionStore().getTopics();

            for (String topic : allTopics) {
                List<Subscrption> subscriptions = AndesContext.getInstance().getSubscriptionStore().getActiveClusterSubscribersForDestination(topic, true);

                for (Subscrption s : subscriptions) {

                    QueueAddress nodeQueueAddress = new QueueAddress(QueueAddress.QueueType.TOPIC_NODE_QUEUE,s.getSubscribedNode());

                    int pendingMessageCount = MessagingEngine.getInstance().getDurableMessageStore().countMessagesOfQueue(nodeQueueAddress,topic);

                    allSubscriptionsForTopics.add(renderSubscriptionForUI(s,pendingMessageCount));
                }
            }
            return allSubscriptionsForTopics.toArray(new String[allSubscriptionsForTopics.size()]);

        } catch (Exception e) {
            throw new RuntimeException("Error in accessing subscription information", e);
        }
    }

    @Override
    public int getSubscriptionCount(boolean destinationName) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private static String renderSubscriptionForUI(Subscrption subscription, int pendingMessageCount) {

        //  subscriptionInfo =  subscriptionIdentifier |  subscribedQueueOrTopicName | subscriberQueueBoundExchange |
        // subscriberQueueName |  isDurable | isActive | numberOfMessagesRemainingForSubscriber | subscriberNodeAddress

        return subscription.getSubscriptionID() + "|" + subscription.getTargetQueue() + "|" + subscription.getTargetQueueBoundExchangeName() +
                "|" + subscription.getTargetQueue() + "|" + subscription.isDurable() + "|" + subscription.isBoundToTopic() +
                "|" + pendingMessageCount + "|" + subscription.getSubscribedNode();
    }
}
