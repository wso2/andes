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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanException;
import javax.management.NotCompliantMBeanException;

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
    public String[] getSubscriptions( String isDurable, String isActive, String protocolType,
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

    /**
     * {@inheritDoc}
     */
    public long getPendingMessageCount(String subscriptionId, String isDurable, String isActive, String protocolType,
                                       String destinationType) throws MBeanException {
        try {
            long pendingMessageCount = 0;
            //Set protocol type and destination type
            ProtocolType protocolTypeArg = ProtocolType.valueOf(protocolType);
            DestinationType destinationTypeArg = DestinationType.valueOf(destinationType);

            //Get all andes subscription set from cluster map
            Set<AndesSubscription> subscriptions = AndesContext.getInstance().getSubscriptionEngine()
                    .getAllClusterSubscriptionsForDestinationType(protocolTypeArg, destinationTypeArg);

            Set<AndesSubscription> subscriptionsToDisplay;

            //Filter all subscription by destination type
            if (DestinationType.TOPIC == destinationTypeArg || (DestinationType.DURABLE_TOPIC == destinationTypeArg)) {
                subscriptionsToDisplay = filterTopicSubscriptions(isDurable, isActive, subscriptions);
            } else {
                subscriptionsToDisplay = filterQueueSubscriptions(isDurable, isActive, subscriptions);
            }

            for (AndesSubscription andesSubscription : subscriptionsToDisplay) {
                if (subscriptionId.equals(andesSubscription.getSubscriptionID())) {
                    pendingMessageCount = MessagingEngine.getInstance().getMessageCountOfQueue(andesSubscription
                            .getStorageQueueName());
                    break;
                }
            }

            return pendingMessageCount;

        } catch (Exception e) {
            log.error("Error while invoking MBeans to retrieve subscription information", e);
            throw new MBeanException(e, "Error while invoking MBeans to retrieve subscription information");
        }
    }

    /**
     * {@inheritDoc}
     */
    public String[] getFilteredSubscriptions(String isDurable, String isActive, String protocolType,
                                             String destinationType, String filteredNamePattern, String identifierPattern,
                                             String ownNodeId, int pageNumber, int maxSubscriptionCount,
            boolean isFilteredNameByExactMatch, boolean isIdentifierPatternByExactMatch) throws MBeanException {

        try {
            int startingIndex = pageNumber * maxSubscriptionCount;
            String[] subscriptionArray;
            int resultSetSize = maxSubscriptionCount;
            int index = 0;
            int subscriptionDetailsIndex = 0;

            //Set protocol type and destination type
            ProtocolType protocolTypeArg = ProtocolType.valueOf(protocolType);
            DestinationType destinationTypeArg = DestinationType.valueOf(destinationType);

            //Get all andes subscription set from cluster map
            Set<AndesSubscription> subscriptions = AndesContext.getInstance().getSubscriptionEngine()
                    .getAllClusterSubscriptionsForDestinationType(protocolTypeArg, destinationTypeArg);

            Set<AndesSubscription> subscriptionsToDisplay;

            //Filter all subscription by destination type
            if (DestinationType.TOPIC == destinationTypeArg || (DestinationType.DURABLE_TOPIC == destinationTypeArg)) {
                subscriptionsToDisplay = filterTopicSubscriptions(isDurable, isActive, subscriptions);
            } else {
                subscriptionsToDisplay = filterQueueSubscriptions(isDurable, isActive, subscriptions);
            }

            //Get matching subscriptions from filter subscriptions according to given search parameters
            List<AndesSubscription> searchSubscriptionList = getSubscriptionListForSearchResult(subscriptionsToDisplay,
                    filteredNamePattern, identifierPattern, ownNodeId,
                    isFilteredNameByExactMatch, isIdentifierPatternByExactMatch);

            //Get only paginated subscription from search subscription result
            if ((searchSubscriptionList.size() - startingIndex) < maxSubscriptionCount) {
                resultSetSize = (searchSubscriptionList.size() - startingIndex);
            }
            subscriptionArray = new String[resultSetSize];

            for (AndesSubscription subscription : searchSubscriptionList) {
                if (startingIndex <= index) {

                    Long pendingMessageCount
                            = MessagingEngine.getInstance().getMessageCountOfQueue(subscription.getStorageQueueName());
                    subscriptionArray[subscriptionDetailsIndex] =
                            renderSubscriptionForUI(subscription, pendingMessageCount.intValue());
                    subscriptionDetailsIndex++;
                    if (subscriptionDetailsIndex == maxSubscriptionCount) {
                        break;
                    }
                }
                index++;
            }

            return subscriptionArray;
        } catch (Exception e) {
            log.error("Error while invoking MBeans to retrieve subscription information with these parameters : "
                      + "filteredNamePattern = " + filteredNamePattern + ", identifierPattern = " + identifierPattern
                      + ", ownNodeId = " + ownNodeId, e);
            throw new MBeanException(e, "Error while invoking MBeans to retrieve subscription information");
        }
    }

    /**
     * {@inheritDoc}
     */
    public int getTotalSubscriptionCountForSearchResult(String isDurable, String isActive, String protocolType,
                                                        String destinationType, String filteredNamePattern,
                                                        String identifierPattern, String ownNodeId,
            boolean isFilteredNameByExactMatch, boolean isIdentifierPatternByExactMatch) throws MBeanException {

        //Set protocol type and destination type
        ProtocolType protocolTypeArg = ProtocolType.valueOf(protocolType);
        DestinationType destinationTypeArg = DestinationType.valueOf(destinationType);

        //Get all andes subscription set from cluster map
        Set<AndesSubscription> subscriptions = AndesContext.getInstance().getSubscriptionEngine()
                .getAllClusterSubscriptionsForDestinationType(protocolTypeArg, destinationTypeArg);

        Set<AndesSubscription> subscriptionsToDisplay;

        //Filter all subscription by destination type
        if ((DestinationType.TOPIC == destinationTypeArg) || (DestinationType.DURABLE_TOPIC == destinationTypeArg)) {
            subscriptionsToDisplay = filterTopicSubscriptions(isDurable, isActive, subscriptions);
        } else {
            subscriptionsToDisplay = filterQueueSubscriptions(isDurable, isActive, subscriptions);
        }

        //Get matching subscriptions from filter subscriptions according to given search parameters
        List<AndesSubscription> searchSubscriptionList = getSubscriptionListForSearchResult(subscriptionsToDisplay,
                filteredNamePattern, identifierPattern, ownNodeId,
                isFilteredNameByExactMatch, isIdentifierPatternByExactMatch);

        //Count of search subscriptions
        return searchSubscriptionList.size();
    }

    /**
     * Get the matching subscription list for the given search criteria
     *
     * @param subscriptionSet  a set of subscriptions
     * @param queueNamePattern string pattern of the queue name
     * @param identifierPattern string pattern of the identifier
     * @param ownNodeId node Id of node which the subscribers subscribed to
     * @return a list of subscriptions matching to the given search criteria
     */
    private List<AndesSubscription> getSubscriptionListForSearchResult(Set<AndesSubscription> subscriptionSet,
                                                                       String queueNamePattern, String identifierPattern,
                                                                       String ownNodeId,
            boolean isFilteredNameByExactMatch, boolean isIdentifierPatternByExactMatch) {
        ArrayList<AndesSubscription> filteredSubscriptionList = new ArrayList<>();
        for(AndesSubscription sub: subscriptionSet){
            boolean isQueueOrTopicNameMatched = false;
            boolean isQueueOrTopicIdentifierMatched = false;
            boolean isOwnNodeIdMatched = false;

            if (isFilteredNameByExactMatch) {
                if (queueNamePattern.equals(sub.getSubscribedDestination())) {
                    isQueueOrTopicNameMatched = true;
                }
            } else if (org.apache.commons.lang.StringUtils.containsIgnoreCase(sub.getSubscribedDestination(),
                    queueNamePattern)) {
                isQueueOrTopicNameMatched = true;

            }

            if (isIdentifierPatternByExactMatch) {
                if (identifierPattern.equals(sub.getSubscriptionID())) {
                    isQueueOrTopicIdentifierMatched = true;
                }
            } else if (org.apache.commons.lang.StringUtils.containsIgnoreCase(sub.getSubscriptionID(),
                    identifierPattern)) {
                isQueueOrTopicIdentifierMatched = true;
            }

            if (ownNodeId.equals("All")) {
                isOwnNodeIdMatched = true;
            }  else{
                if(ownNodeId.equals(sub.getSubscribedNode())){
                    isOwnNodeIdMatched = true;
                }
            }
            if(isQueueOrTopicNameMatched && isQueueOrTopicIdentifierMatched && isOwnNodeIdMatched){
                filteredSubscriptionList.add(sub);
            }

        }

        return filteredSubscriptionList;
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
        Set<String> uniqueSubscriptionIds = new HashSet<>();

        for (AndesSubscription subscription : subscriptions) {
            if (!isDurable.equals(ALL_WILDCARD)
                    && (Boolean.parseBoolean(isDurable) != subscription.isDurable())) {
                continue;
            }

            if (subscription.isDurable()) {
                if (subscription.hasExternalSubscriptions()) {
                    uniqueSubscriptionIds.add(subscription.getTargetQueue());
                } else {
                    // Since only one inactive shared subscription should be shown
                    // we replace the existing value if any
                    inactiveSubscriptions.put(subscription.getTargetQueue(), subscription);
                    // Inactive subscriptions will be added later considering shared subscriptions
                    continue;
                }
            }
            if (isActive.equals(ALL_WILDCARD) || Boolean.parseBoolean(isActive)){
                subscriptionsToDisplay.add(subscription);
            }
        }

        // In UI only one inactive shared subscription should be shown if there are no active subscriptions.
        if (isActive.equals(ALL_WILDCARD) || !Boolean.parseBoolean(isActive)){
            for (Map.Entry<String, AndesSubscription> inactiveEntry : inactiveSubscriptions.entrySet()) {
                // If there are active subscriptions with same target queue, we skip adding inactive subscriptions
                if (!(uniqueSubscriptionIds.contains(inactiveEntry.getKey()))) {
                    subscriptionsToDisplay.add(inactiveEntry.getValue());
                }
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
                + "|" + subscription.getSubscribedDestination()
                + "|" + subscription.getProtocolType().name()
                + "|" + subscription.getDestinationType().name();
    }
}
