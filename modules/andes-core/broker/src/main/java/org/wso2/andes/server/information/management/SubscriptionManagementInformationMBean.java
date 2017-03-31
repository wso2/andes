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
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.management.common.mbeans.SubscriptionManagementInformation;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.server.management.AMQManagedObject;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import javax.management.MBeanException;
import javax.management.NotCompliantMBeanException;

/**
 * Class to handle data for all subscription related UI functions.
 */
public class SubscriptionManagementInformationMBean extends AMQManagedObject implements SubscriptionManagementInformation {

    private static Log log = LogFactory.getLog(SubscriptionManagementInformationMBean.class);

    /**
     * Separator used for render the subscriptions for UI
     */
    private static final String SEPARATOR = ";";

    /**
     * Used when the subscriber connection is not available
     */
    private static final String NOT_AVAILABLE = "N/A";

    /**
     * Subscription store used to query subscription related information
     */
    private AndesSubscriptionManager subscriptionManager;

    /**
     * Instantiates the MBeans related to subscriptions.
     *
     * @throws NotCompliantMBeanException
     */
    public SubscriptionManagementInformationMBean() throws NotCompliantMBeanException {
        super(SubscriptionManagementInformation.class, SubscriptionManagementInformation.TYPE);

        subscriptionManager = AndesContext.getInstance().getAndesSubscriptionManager();
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
    public String[] getSubscriptions(String isDurable, String isActive, String protocolType,
                                     String destinationType) throws MBeanException {
        try {

            //TODO: fix this method. isActive comes as *
            boolean durableSubscriptionsReq = Boolean.parseBoolean(isDurable);
            Set<AndesSubscription> subscriptionsToDisplay = getSubscriptionsOfBroker(durableSubscriptionsReq,
                    true, protocolType, destinationType);

            Set<AndesSubscription> inactiveSubscriptions = getSubscriptionsOfBroker(durableSubscriptionsReq,
                    false, protocolType, destinationType);
            subscriptionsToDisplay.addAll(inactiveSubscriptions);

            String[] subscriptionArray = new String[subscriptionsToDisplay.size()];

            int index = 0;
            for (AndesSubscription subscription : subscriptionsToDisplay) {
                Long pendingMessageCount = MessagingEngine.getInstance().getApproximateQueueMessageCount(subscription
                        .getStorageQueue().getName());

                subscriptionArray[index] = renderSubscriptionForUI(subscription.isActive(),
                        destinationType,
                        subscription,
                        pendingMessageCount
                                .intValue());

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
    public long getPendingMessageCount(String queueName) throws MBeanException {
        try {
            return MessagingEngine.getInstance().getApproximateQueueMessageCount(queueName);
        } catch (Exception e) {
            log.error("Error while invoking MBeans to calculate the pending message count for storage queue", e);
            throw new MBeanException(e, "Error while invoking MBeans to calculate the pending message count for "
                    + "storage queue");
        }
    }

    /**
     * {@inheritDoc}
     */
    public String[] getFilteredSubscriptions(boolean isDurable, boolean isActive, String protocolType,
                                             String destinationType, String filteredNamePattern,
                                             boolean isFilteredNameByExactMatch,String identifierPattern,
                                             boolean isIdentifierPatternByExactMatch, String ownNodeId, int pageNumber,
                                             int maxSubscriptionCount) throws MBeanException {

        try {
            int startingIndex = pageNumber * maxSubscriptionCount;
            String[] subscriptionArray;
            int resultSetSize = maxSubscriptionCount;
            int index = 0;
            int subscriptionDetailsIndex = 0;

            AndesSubscriptionManager subscriptionManager = AndesContext.getInstance().getAndesSubscriptionManager();

            Set<AndesSubscription> searchSubscriptionList = subscriptionManager
                    .getFilteredSubscriptions(isDurable, isActive, ProtocolType.valueOf(protocolType),
                            DestinationType.valueOf(destinationType), filteredNamePattern,
                            isFilteredNameByExactMatch,identifierPattern, isIdentifierPatternByExactMatch,
                            ownNodeId);

            int fullListSize = searchSubscriptionList.size() - startingIndex;

            //Get only paginated subscription from search subscription result
            if (fullListSize >= 0 && fullListSize < maxSubscriptionCount) {
                resultSetSize = (searchSubscriptionList.size() - startingIndex);
            }
            subscriptionArray = new String[resultSetSize];

            for (AndesSubscription subscription : searchSubscriptionList) {
                if (startingIndex <= index) {
                    Long pendingMessageCount
                            = MessagingEngine.getInstance().getApproximateQueueMessageCount(subscription.getStorageQueue()
                            .getName());
                    subscriptionArray[subscriptionDetailsIndex] =
                            renderSubscriptionForUI(isActive, destinationType,subscription,
                                    pendingMessageCount.intValue());
                    subscriptionDetailsIndex++;
                    if (subscriptionDetailsIndex == maxSubscriptionCount) {
                        break;
                    }
                }
                index++;
            }

            return subscriptionArray;
        } catch (Exception e) {
            log.error("Error while invoking MBeans to retrieve subscription information. The given method parameters "
                    + "are: isDurable- " + isDurable + " isActive- " + isActive + " protocolType- " + protocolType
                    + " destinationType- " + destinationType + " filteredNamePattern- " + filteredNamePattern
                    + " isFilteredNameByExactMatch- " + isFilteredNameByExactMatch + " identifierPattern- "
                    + identifierPattern + " isIdentifierPatternByExactMatch- " + isIdentifierPatternByExactMatch
                    + " ownNodeId- " + ownNodeId + " pageNumber- " + pageNumber + " maxSubscriptionCount- "
                    + maxSubscriptionCount, e);
            throw new MBeanException(e, "Error while invoking MBeans to retrieve subscription information");
        }
    }

    /**
     * Create the regex pattern for filtering.
     *
     * @param pattern String value of the patten
     * @return Regex pattern
     */
    private Pattern createRegEx(String pattern){

        String regex = pattern.replaceAll("\\*",".*");

        return Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
    }

    /**
     * {@inheritDoc}
     */
    public int getTotalSubscriptionCountForSearchResult(boolean isDurable, boolean isActive, String protocolType,
                                                        String destinationType, String filteredNamePattern, boolean
                                                        isFilteredNameByExactMatch, String identifierPattern, boolean
                                                        isIdentifierPatternByExactMatch, String ownNodeId)
            throws MBeanException {
        try {
            AndesSubscriptionManager subscriptionManager =
                    AndesContext.getInstance().getAndesSubscriptionManager();
            //get the searched subscriptions
            Set<AndesSubscription> searchSubscriptionList = subscriptionManager
                    .getFilteredSubscriptions(isDurable,isActive,
                            ProtocolType.valueOf(protocolType),DestinationType.valueOf(destinationType),
                            filteredNamePattern,isFilteredNameByExactMatch, identifierPattern,
                            isIdentifierPatternByExactMatch, ownNodeId);

            //count of search subscriptions
            return searchSubscriptionList.size();
        }catch (Exception e){
            log.error("Error while invoking MBeans to retrieve subscription information", e);
            throw new MBeanException(e, "Error while invoking MBeans to retrieve subscription information");
        }
    }


    private Set<AndesSubscription> getSubscriptionsOfBroker(boolean isDurable, boolean isActive, String protocolType,
                                      String destinationType) throws AndesException {

        AndesSubscriptionManager subscriptionManager =
                AndesContext.getInstance().getAndesSubscriptionManager();

        Iterable<AndesSubscription> matchingSubscriptions;
        Set<AndesSubscription> subscriptionsToDisplay = new HashSet<>(10);

        if(!isDurable && !isActive){
            return subscriptionsToDisplay;
        }

        ProtocolType protocolTypeArg = ProtocolType.valueOf(protocolType);
        DestinationType destinationTypeArg = DestinationType.valueOf(destinationType);
        if(isActive) {
            String messageRouterName;
            switch (destinationTypeArg){
                case TOPIC:
                    if(protocolTypeArg.equals(ProtocolType.AMQP)) {
                        messageRouterName = AMQPUtils.TOPIC_EXCHANGE_NAME;
                    } else {
                        messageRouterName = MQTTUtils.MQTT_EXCHANGE_NAME;
                    }
                    break;
                case QUEUE:
                    messageRouterName = AMQPUtils.DIRECT_EXCHANGE_NAME;
                    break;
                case DURABLE_TOPIC:
                    if(protocolTypeArg.equals(ProtocolType.AMQP)) {
                        messageRouterName = AMQPUtils.TOPIC_EXCHANGE_NAME;
                    } else {
                        messageRouterName = MQTTUtils.MQTT_EXCHANGE_NAME;
                    }
                    break;
                default:
                    messageRouterName = AMQPUtils.DIRECT_EXCHANGE_NAME;
                    break;
            }

            matchingSubscriptions = subscriptionManager.
                    getAllSubscriptionsByMessageRouter(protocolTypeArg, messageRouterName);

        } else {
            matchingSubscriptions = subscriptionManager.
                    getInactiveSubscriberRepresentations();
        }

        for (AndesSubscription subscription : matchingSubscriptions) {
            if (isDurable == subscription.isDurable()) {
                subscriptionsToDisplay.add(subscription);
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
            Long messageCount = MessagingEngine.getInstance().getApproximateQueueMessageCount(destinationName);
            return messageCount.intValue();
        }catch (Exception e) {
            throw new RuntimeException("Error in retrieving pending message count", e);
        }
    }

    @Override
    public void removeSubscription(String subscriptionId, String destinationName, String protocolType,
                                   String destinationType) {
        try {
            AndesSubscription subscriptionToRemove = subscriptionManager.getSubscriptionById(subscriptionId);

            if (null != subscriptionToRemove) {
                subscriptionToRemove.forcefullyDisconnectConnections();
            } else {
                throw new AndesException("Subscription to remove not found. Requested id = " + subscriptionId);
            }
        } catch (AndesException e) {
            throw new RuntimeException("Error in accessing subscription information", e);
        }
    }

    /**
     * This method returns the formatted subscription string to be compatible with the UI processor.
     *
     * @param subscription        AndesSubscription object that is to be translated to UI view
     * @param pendingMessageCount Number of pending messages of subscription
     * @return String representation of the subscription meta information and pending message count
     */
    private static String renderSubscriptionForUI(Boolean isActive, String destinationType, AndesSubscription
                                                  subscription, int pendingMessageCount) throws AndesException {

        String subscriptionIdentifier = subscription.getSubscriptionId();

        StringBuilder subscriptionForUI = new StringBuilder(subscriptionIdentifier);
        subscriptionForUI.append(SEPARATOR);
        subscriptionForUI.append(subscription.getStorageQueue().getMessageRouterBindingKey());
        subscriptionForUI.append(SEPARATOR);
        subscriptionForUI.append(subscription.getStorageQueue().getMessageRouter().getName());
        subscriptionForUI.append(SEPARATOR);
        if (subscription.isLocal()) {
            subscriptionForUI.append(subscription.isDurable() ? subscription.getStorageQueue().getName()
                    : subscription.getProtocolQueue());
        } else {
            subscriptionForUI.append(subscription.isDurable() ? subscription.getStorageQueue().getName()
                    : NOT_AVAILABLE);
        }
        subscriptionForUI.append(SEPARATOR);
        subscriptionForUI.append(subscription.isDurable());
        subscriptionForUI.append(SEPARATOR);
        subscriptionForUI.append(isActive);
        subscriptionForUI.append(SEPARATOR);
        subscriptionForUI.append(pendingMessageCount);
        subscriptionForUI.append(SEPARATOR);
        subscriptionForUI.append(((null == subscription.getSubscriberConnection()) ? NOT_AVAILABLE: subscription
                .getSubscriberConnection().getConnectedNode()));
        subscriptionForUI.append(SEPARATOR);
        subscriptionForUI.append(subscription.getStorageQueue().getMessageRouterBindingKey());
        subscriptionForUI.append(SEPARATOR);
        subscriptionForUI.append(subscription.getProtocolType().name());
        subscriptionForUI.append(SEPARATOR);
        subscriptionForUI.append(destinationType);
        subscriptionForUI.append(SEPARATOR);
        subscriptionForUI.append(((null == subscription.getSubscriberConnection()) ? NOT_AVAILABLE: subscription
                .getSubscriberConnection().getConnectedIP()));

        return subscriptionForUI.toString();
    }
}
