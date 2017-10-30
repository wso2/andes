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
package org.wso2.andes.configuration.models.flowcontrol;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for  global based flow control related configs.
 */
@Configuration(description = "This is the channel specific buffer limits which enable/disable the flow control locally")
public class BufferBasedConfiguration {
    @Element(description = "flow control is enabled when message chunk pending to be handled by inbound \n"
            + "\t disruptor reaches above this limit")
    private int lowLimit = 100;

    @Element(description = "flow control is disabled (if enabled) when message chunk pending to be handled\n"
            + "\t by inbound disruptor reaches below this limit")
    private int highLimit = 1000;

    public int getLowLimit() {
        return lowLimit;
    }

    public int getHighLimit() {
        return highLimit;
    }
}
