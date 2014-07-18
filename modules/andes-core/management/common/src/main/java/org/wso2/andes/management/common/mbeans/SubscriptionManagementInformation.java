package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;

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
public interface SubscriptionManagementInformation {

    static final String TYPE = "SubscriptionManagementInformation";

    @MBeanAttribute(name="AllQueueSubscriptions",description = "All queue subscriptions")
    String[] getAllQueueSubscriptions(
            @MBeanOperationParameter(name = "isDurable" ,description = "get durable ?") boolean isDurable,
            @MBeanOperationParameter(name = "isActive" ,description = "get active ?") boolean isActive,
            @MBeanOperationParameter(name = "isLocal" ,description = "get local ?") boolean isLocal);

    @MBeanAttribute(name="TopicSubscriptions",description = "All topic subscriptions")
    String[] getAllTopicSubscriptions(
            @MBeanOperationParameter(name = "isDurable" ,description = "get durable ?") boolean isDurable,
            @MBeanOperationParameter(name = "isActive" ,description = "get active ?") boolean isActive,
            @MBeanOperationParameter(name = "isLocal" ,description = "get local ?") boolean isLocal);

    @MBeanAttribute(name="SubscriptionCount", description = "Number of subscriptions for the destination")
    int getSubscriptionCount(
            @MBeanOperationParameter(name = "destinationName" ,description = "destination name") boolean destinationName);

}
