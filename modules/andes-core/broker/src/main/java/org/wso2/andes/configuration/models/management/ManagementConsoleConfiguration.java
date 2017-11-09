/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.andes.configuration.models.management;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for statistics view in management console.
 */
@Configuration(description = " This section is about how you want to view messaging statistics from the admin console"
        + " and how you plan to interact with it")
public class ManagementConsoleConfiguration {

    @Element(description = "Maximum number of messages to be fetched per page using Andes message browser when"
            + " browsing queues/dlc")
    private int messageBrowsePageSize = 100;

    @Element(description = "This property defines the maximum message content length that can be displayed at the\n"
            + "management console when browsing queues. If the message length exceeds the value, a\n"
            + "truncated content will be displayed with a statement \"message content too large to display.\"\n"
            + "at the end. default value is 100000 (can roughly display a 100KB message.)\n"
            + " * NOTE : Increasing this value could cause delays when loading the message content page")
    private int maximumMessageDisplayLength = 100000;

    @Element(description = "Enable users to reroute all messages from a specific destination(queue or durable topic) to"
            + " a specific queue")
    private boolean allowReRouteAllInDLC = false;

    public int getMessageBrowsePageSize() {
        return messageBrowsePageSize;
    }

    public int getMaximumMessageDisplayLength() {
        return maximumMessageDisplayLength;
    }

    public boolean isAllowReRouteAllInDLC() {
        return allowReRouteAllInDLC;
    }

}
