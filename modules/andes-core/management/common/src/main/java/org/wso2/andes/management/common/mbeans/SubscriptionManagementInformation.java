/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;

import javax.management.MBeanException;

/**
 * This is the interface for implementing subscription related information transfer to the UI.
 */
public interface SubscriptionManagementInformation {

    static final String TYPE = "SubscriptionManagementInformation";

    /**
     * MBean service to get filtered queue subscriptions
     * @param isDurable of type String (acceptable values => * | true | false)
     * @param isActive of type String (acceptable values => * | true | false)
     * @param protocolType The protocol type of the subscriptions
     * @param destinationType The destination type of the subscriptions
     * @return array of queue subscriptions
     */
    @MBeanAttribute(name="AllSubscriptions",description = "All subscriptions")
    String[] getSubscriptions(
            @MBeanOperationParameter(name = "isDurable" ,description = "get durable ?") String isDurable,
            @MBeanOperationParameter(name = "isActive" ,description = "get active ?") String isActive,
            @MBeanOperationParameter(
                    name = "protocolType", description = "protocol of subscriptions") String protocolType,
            @MBeanOperationParameter(
                    name = "destinationType",
                    description = "Destination type of the subscriptions") String destinationType)
            throws MBeanException;

    /**
     * Get the pending message count for the specified subscription
     * @param queueName for which the pending message cound need to be calculated
     * @return The pending message count for that subscription
     * @throws MBeanException
     */
    long getPendingMessageCount(
            @MBeanOperationParameter(name = "queueName" ,description ="get queue name ?") String queueName)
            throws MBeanException;

    /**
     * Search subscription according to provided patterns and return paginated subscription array to
     * render in the management console.
     * @param isDurable of type String (acceptable values => true | false)
     * @param isActive of type String (acceptable values => true | false)
     * @param protocolType The protocol type of the subscriptions
     * @param destinationType The destination type of the subscriptions
     * @param filteredNamePattern pattern to search by queue name
     * @param isFilteredNameByExactMatch exact match or not
     * @param identifierPattern pattern to search by subscription identifier
     * @param isIdentifierPatternByExactMatch exact match or not
     * @param ownNodeId pattern to search subscription own node id
     * @param startingIndex page number
     * @param maxSubscriptionCount page count
     * @return Array of subscription matching given criteria
     * @throws MBeanException
     */
    @MBeanAttribute(name="FilteredSubscriptions",description = "Filtered subscriptions")
    String[] getFilteredSubscriptions(
            @MBeanOperationParameter(name = "isDurable" ,description = "get durable ?") boolean isDurable,
            @MBeanOperationParameter(name = "isActive" ,description = "get active ?") boolean isActive,
            @MBeanOperationParameter(
                    name = "protocolType", description = "protocol of subscriptions") String protocolType,
            @MBeanOperationParameter(
                    name = "destinationType",
                    description = "Destination type of the subscriptions") String destinationType,
            @MBeanOperationParameter(
                    name = "filteredNamePattern" ,description = "queue name pattern") String filteredNamePattern,
            @MBeanOperationParameter(
                    name = "isFilteredNameByExactMatch" ,description = "exact match ?") boolean isFilteredNameByExactMatch,
            @MBeanOperationParameter(
                    name = "identifierPattern" , description = "subscription identifier pattern") String identifierPattern,
            @MBeanOperationParameter(
                    name = "isIdentifierPatternByExactMatch" , description = "exact match ?")
                    boolean isIdentifierPatternByExactMatch,
            @MBeanOperationParameter(
                    name = "ownNodeId" , description = "subscription node id pattern") String ownNodeId,
            @MBeanOperationParameter(
                    name = "startingIndex" , description = "page number") int startingIndex,
            @MBeanOperationParameter(
                    name = "maxSubscriptionCount" , description = "page count") int maxSubscriptionCount)
            throws MBeanException;

    /**
     * Return subscription result count according to provided patterns
     * @param isDurable of type String (acceptable values => true | false)
     * @param isActive of type String (acceptable values => true | false)
     * @param protocolType The protocol type of the subscriptions
     * @param destinationType The destination type of the subscriptions
     * @param filteredNamePattern pattern to search by queue name
     * @param isFilteredNameByExactMatch exact match or not
     * @param identifierPattern pattern to search by subscription identifier
     * @param  isIdentifierPatternByExactMatch exact match or not
     * @param ownNodeId pattern to search subscription own node id
     * @return subscription count
     * @throws MBeanException
     */
    int getTotalSubscriptionCountForSearchResult(
            @MBeanOperationParameter(name = "isDurable" ,description = "get durable ?") boolean isDurable,
            @MBeanOperationParameter(name = "isActive" ,description = "get active ?") boolean isActive,
            @MBeanOperationParameter(
                    name = "protocolType", description = "protocol of subscriptions") String protocolType,
            @MBeanOperationParameter(
                    name = "destinationType",
                    description = "Destination type of the subscriptions") String destinationType,
            @MBeanOperationParameter(
                    name = "filteredNamePattern" ,description = "queue name pattern") String filteredNamePattern,
            @MBeanOperationParameter(
                    name = "isFilteredNameByExactMatch" ,description = "exact match ?") boolean isFilteredNameByExactMatch,
            @MBeanOperationParameter(
                    name = "identifierPattern" ,
                    description = "subscription identifier pattern") String identifierPattern,
            @MBeanOperationParameter(
                    name = "isIdentifierPatternByExactMatch" , description = "exact match ?")
                    boolean isIdentifierPatternByExactMatch,
            @MBeanOperationParameter(
                    name = "ownNodeId" , description = "subscription node id pattern") String ownNodeId)
            throws MBeanException;


    /**
     * MBean service to get Pending Message count for a given destination
     * @param subscribedNode ID of the subscribed node
     * @param msgPattern queue/topic
     * @param destinationName destination querying for message count
     * @return pending message count for destination
     */
    //TODO: there is noting like message count of node now
    @Deprecated
    @MBeanAttribute(name="MessageCount", description = "Number of messages pending for the destination")
    int getMessageCount(
            @MBeanOperationParameter(name = "subscribedNode" ,description = "Subscribed node address") String subscribedNode,
            @MBeanOperationParameter(name = "msgPattern" ,description = "queue or topic or any other pattern") String msgPattern,
            @MBeanOperationParameter(name = "destinationName" ,description = "destination name") String destinationName);

    /**
     * MBean service to remove a subscription forcefully
     *
     * @param subscriptionId ID of the subscription
     * @param destinationName destination subscription is bound
     */
    @MBeanAttribute(name = "RemoveSubscription", description = "Remove a subscription forcefully")
    void removeSubscription(
            @MBeanOperationParameter(name = "subscriptionId", description = "ID of the Subscription to remove") String
                    subscriptionId,
            @MBeanOperationParameter(name = "destinationName", description = "Subscribed destination name") String
                    destinationName,
            @MBeanOperationParameter(
                    name = "protocolType", description = "protocol of subscriptions") String protocolType,
            @MBeanOperationParameter(
                    name = "destinationType",
                    description = "Destination type of the subscriptions") String destinationType);
}
