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

/**
 * This is the interface for implementing subscription related information transfer to the UI.
 */
public interface SubscriptionManagementInformation {

    static final String TYPE = "SubscriptionManagementInformation";

    /**
     * MBean service to get filtered queue subscriptions
     * @param isDurable of type String (acceptable values => * | true | false)
     * @param isActive of type String (acceptable values => * | true | false)
     * @return
     */
    @MBeanAttribute(name="AllQueueSubscriptions",description = "All queue subscriptions")
    String[] getAllQueueSubscriptions(
            @MBeanOperationParameter(name = "isDurable" ,description = "get durable ?") String isDurable,
            @MBeanOperationParameter(name = "isActive" ,description = "get active ?") String isActive);

    /**
     * MBean service to get filtered topic subscriptions
     * @param isDurable of type String (acceptable values => * | true | false)
     * @param isActive of type String (acceptable values => * | true | false)
     * @return
     */
    @MBeanAttribute(name="TopicSubscriptions",description = "All topic subscriptions")
    String[] getAllTopicSubscriptions(
            @MBeanOperationParameter(name = "isDurable" ,description = "get durable ?") String isDurable,
            @MBeanOperationParameter(name = "isActive" ,description = "get active ?") String isActive);


    /**
     * MBean service to get Pending Message count for a given destination
     * @param subscribedNode
     * @param msgPattern
     * @param destinationName
     * @return
     */
    //TODO: there is noting like message count of node now
    @Deprecated
    @MBeanAttribute(name="MessageCount", description = "Number of messages pending for the destination")
    int getMessageCount(
            @MBeanOperationParameter(name = "subscribedNode" ,description = "Subscribed node address") String subscribedNode,
            @MBeanOperationParameter(name = "msgPattern" ,description = "queue or topic or any other pattern") String msgPattern,
            @MBeanOperationParameter(name = "destinationName" ,description = "destination name") String destinationName);

}
