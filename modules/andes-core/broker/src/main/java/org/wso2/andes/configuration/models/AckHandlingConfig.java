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
 * Configuration model for acknowledgment handling in performance tuning config.
 */
@Configuration(description = "Acknowledgment handling configuration.")
public class AckHandlingConfig {

    @Element(description = "Number of message acknowledgement handlers to process acknowledgements concurrently.\n"
            + "            These acknowledgement handlers will batch and process acknowledgements.")
    private int ackHandlerCount = 1;

    @Element(description = "Maximum batch size of the acknowledgement handler. Andes process acknowledgements in\n"
            + "            batches using Disruptor Increasing the batch size reduces the number of calls made to\n"
            + "            database by MB. Depending on the database optimal batch size this value should be set.\n"
            + "            Batches will be of the maximum batch size mostly in high throughput scenarios.\n"
            + "            Underlying implementation use Disruptor for batching hence will batch message at a\n"
            + "            lesser value than this in low throughput scenarios")
    private int ackHandlerBatchSize = 100;

    @Element(description = "Message delivery from server to the client will be paused temporarily if number of\n"
            + "            delivered but unacknowledged message count reaches this size. Should be set considering\n"
            + "            message consume rate. This is to avoid overwhelming slow subscribers.")
    private int maxUnackedMessages = 100;

    public int getAckHandlerCount() {
        return ackHandlerCount;
    }

    public int getAckHandlerBatchSize() {
        return ackHandlerBatchSize;
    }

    public int getMaxUnackedMessages() {
        return maxUnackedMessages;
    }
}