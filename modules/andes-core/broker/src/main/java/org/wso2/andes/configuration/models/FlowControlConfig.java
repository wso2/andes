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
package org.wso2.andes.configuration.models;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model Flow control configs.
 */
@Configuration(description = "Memory and resource exhaustion is something we should prevent and recover from.\n"
        + "    This section allows you to specify the threshold at which to reduce/stop frequently intensive\n"
        + "    operations within MB temporarily.\n"
        + "    highLimit - flow control is enabled when message chunk pending to be handled by inbound\n"
        + "         disruptor reaches above this limit\n"
        + "    lowLimit - flow control is disabled (if enabled) when message chunk pending to be handled\n"
        + "         by inbound disruptor reaches below this limit")
public class FlowControlConfig {

    @Element(description = "This is the global buffer limits which enable/disable the flow control globally")
    private FlowControlGlobalConfig  global = new FlowControlGlobalConfig();

    @Element(description = "This is the channel specific buffer limits which enable/disable the flow control locally.")
    private FlowControlBufferBasedConfig bufferBased = new FlowControlBufferBasedConfig();

}