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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.management.common.mbeans.SubscriptionManagementInformation;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.subscription.LocalSubscription;
import org.wso2.andes.subscription.SubscriptionEngine;

import javax.management.MBeanException;
import javax.management.NotCompliantMBeanException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Class to handle data for all subscription related UI functions.
 */
public class SubscriptionManagementInformationMBean extends AMQManagedObject implements SubscriptionManagementInformation {

    private static Log log = LogFactory.getLog(SubscriptionManagementInformationMBean.class);

    private static final String ALL_WILDCARD = "*";

    /**
     * Subscription store used to query subscription related information
     */
    private SubscriptionEngine subscriptionEngine;

    /**
     * Instantiates the MBeans related to subscriptions.
     *
     * @throws NotCompliantMBeanException
     */
    public SubscriptionManagementInformationMBean() throws NotCompliantMBeanException {
        super(SubscriptionManagementInformation.class, SubscriptionManagementInformation.TYPE);

        subscriptionEngine = AndesContext.getInstance().getSubscriptionEngine();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getObjectInstanceName() {
        return SubscriptionManagementInformation.TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getAllSubscriptions( String isDurable, String isActive, String protocolType,
                                         String destinationType) throws MBeanException {
        try {

            ProtocolType protocolTypeArg = ProtocolType.valueOf(protocolType);
            DestinationType destinationTypeArg = DestinationType.valueOf(destinationType);

            Set<AndesSubscription> subscriptions = AndesContext.getInstance().getSubscriptionEngine()
                            .getAllClusterSubscriptionsForDestinationType(protocolTypeArg, destinationTypeArg);

            Set<AndesSubscription> subscriptionsToDisplay;

            if (DestinationType.TOPIC == destinationTypeArg) {
                subscriptionsToDisplay = filterTopicSubscriptions(isDurable, isActive, subscriptions);
            } else {
                subscriptionsToDisplay = filterQueueSubscriptions(isDurable, isActive, subscriptions);
            }

            String[] subscriptionArray = new String[subscriptionsToDisplay.size()];

            int index = 0;
            for (AndesSubscription subscription : subscriptionsToDisplay) {
                Long pendingMessageCount
                        = MessagingEngine.getInstance().getMessageCountOfQueue(subscription.getStorageQueueName());

                subscriptionArray[index] = renderSubscriptionForUI(subscription, pendingMessageCount.intValue());
                index++;

            }
            return subscriptionArray;

        } catch (Exception e) {
            log.error("Error while invoking MBeans to retrieve subscription information", e);
            throw new MBeanException(e, "Error while invoking MBeans to retrieve subscription information");
        }
    }

    private Set<AndesSubscription> filterQueueSubscriptions(String isDurable, String isActive,
                                                            Set<AndesSubscription> subscriptions) {
        Set<AndesSubscription> subscriptionsToDisplay = new HashSet<>();

        for (AndesSubscription subscription : subscriptions) {
            if (!isDurable.equals(ALL_WILDCARD)
                    && (Boolean.parseBoolean(isDurable) != subscription.isDurable())) {
                continue;
            }
            if (!isActive.equals(ALL_WILDCARD)
                    && (Boolean.parseBoolean(isActive) != subscription.hasExternalSubscriptions())) {
                continue;
            }

            subscriptionsToDisplay.add(subscription);
        }

        return subscriptionsToDisplay;
    }

    private Set<AndesSubscription> filterTopicSubscriptions(String isDurable, String isActive,
                                                            Set<AndesSubscription> subscriptions) {
        Set<AndesSubscription> subscriptionsToDisplay = new HashSet<>();

        Map<String, AndesSubscription> inactiveSubscriptions = new HashMap<>();
        Set<String> uniqueSubscriptionIDs = new HashSet<>();

        for (AndesSubscription subscription : subscriptions) {
            if (!isDurable.equals(ALL_WILDCARD)
                    && (Boolean.parseBoolean(isDurable) != subscription.isDurable())) {
                continue;
            }
            if (!isActive.equals(ALL_WILDCARD)
                    && (Boolean.parseBoolean(isActive) != subscription.hasExternalSubscriptions())) {
                continue;
            }

            if (subscription.isDurable()) {
                if (subscription.hasExternalSubscriptions()) {
                    uniqueSubscriptionIDs.add(subscription.getTargetQueue());
                } else {
                    // Since only one inactive shared subscription should be shown
                    // we replace the existing value if any
                    inactiveSubscriptions.put(subscription.getTargetQueue(), subscription);
                    // Inactive subscriptions will be added later considering shared subscriptions
                    continue;
                }
            }

            subscriptionsToDisplay.add(subscription);
        }

        // In UI only one inactive shared subscription should be shown if there are no active subscriptions.
        for (Map.Entry<String, AndesSubscription> inactiveEntry : inactiveSubscriptions.entrySet()) {
            // If there are active subscriptions with same target queue, we skip adding inactive subscriptions
            if (!(uniqueSubscriptionIDs.contains(inactiveEntry.getKey()))) {
                subscriptionsToDisplay.add(inactiveEntry.getValue());
            }
        }


        return subscriptionsToDisplay;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMessageCount(String subscribedNode, String msgPattern ,String destinationName) {
        try {
            Long messageCount = MessagingEngine.getInstance().getMessageCountOfQueue(destinationName);
            return messageCount.intValue();
        }catch (Exception e) {
            throw new RuntimeException("Error in retrieving pending message count", e);
        }
    }

    @Override
    public void removeSubscription(String subscriptionId, String destinationName, String protocolType,
                                   String destinationType) {
        try {
            Set<LocalSubscription> allSubscribersForDestination
                    = subscriptionEngine.getActiveLocalSubscribers(destinationName, ProtocolType.valueOf(protocolType),
                    DestinationType.valueOf(destinationType));

            for (LocalSubscription andesSubscription : allSubscribersForDestination) {

                String currentSubscriptionId = andesSubscription.getSubscriptionID();

                if (currentSubscriptionId.equals(subscriptionId)) {
                    andesSubscription.forcefullyDisconnect();
                    break;
                }
            }
        } catch (AndesException e) {
            throw new RuntimeException("Error in accessing subscription information", e);
        }
    }

    /**
     * This method returns the formatted subscription string to be compatible with the UI processor.
     * <p/>
     * Format of the string : "subscriptionInfo =  subscriptionIdentifier |
     * subscribedQueueOrTopicName (tenant domain appended destination) | subscriberQueueBoundExchange |
     * subscriberQueueName |
     * isDurable | isActive | numberOfMessagesRemainingForSubscriber | subscriberNodeAddress"
     *
     * @param subscription        AndesSubscription object that is to be translated to UI view
     * @param pendingMessageCount Number of pending messages of subscription
     * @return String representation of the subscription meta information and pending message count
     */
    private static String renderSubscriptionForUI(AndesSubscription subscription,
                                                  int pendingMessageCount) throws AndesException {

        String subscriptionIdentifier = subscription.getSubscriptionID();

        //in case of topic whats in v2 is : topicSubscriber.getDestination() + "@" +
        // topicSubscriber.boundTopicName; --
        return subscriptionIdentifier
                + "|" + subscription.getSubscribedDestination()
                + "|" + subscription.getTargetQueueBoundExchangeName()
                + "|" + subscription.getTargetQueue()
                + "|" + subscription.isDurable()
                + "|" + subscription.hasExternalSubscriptions()
                + "|" + pendingMessageCount
                + "|" + subscription.getSubscribedNode()
                + "|" + subscription.getSubscribedDestination();
    }
}
