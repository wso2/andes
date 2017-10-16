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
 * Configuration model for publisher transactions.
 */
@Configuration(description = "Publisher transaction related configurations.")
public class TransactionConfig {

    @Element(description = "Maximum batch size (Messages) in kilobytes for a transaction. Exceeding this limit will\n"
            + "result in a failure in the subsequent commit (or prepare) request. Default is set to 1MB.\n"
            + "Limit is calculated considering the payload of messages.")
    private int maxBatchSizeInKB = 10240;

    @Element(description = "Maximum number of parallel dtx enabled channel count. Distributed transaction\n"
            + "requests exceeding this limit will fail.")
    private int maxParallelDtxChannels = 20;

    public int getMaxBatchSizeInKB() {
        return maxBatchSizeInKB;
    }

    public int getMaxParallelDtxChannels() {
        return maxParallelDtxChannels;
    }
}