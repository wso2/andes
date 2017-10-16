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
 * Configuration model class for Inbound event configs in Performance tuning section.
 */
@Configuration(description = "Inbound events config")
public class InboundEventsConfig {

    @Element(description = "Number of parallel writers used to write content to message store. Increasing this\n"
            + "            value will speed-up the message receiving mechanism. But the load on the data store will\n"
            + "            increase.")
    private int parallelMessageWriters = 1;

    @Element(description = "Size of the Disruptor ring buffer for inbound event handling. For publishing at\n"
            + "            higher rates increasing the buffer size may give some advantage on keeping messages in\n"
            + "            memory and write.\n"
            + "            NOTE: Buffer size should be a value of power of two")
    private int bufferSize = 65536;

    @Element(description = "Maximum batch size of the batch write operation for inbound messages. MB internals\n"
            + "            use Disruptor to batch events. Hence this batch size is set to avoid database requests\n"
            + "            with high load (with big batch sizes) to write messages. This need to be configured in\n"
            + "            high throughput messaging scenarios to regulate the hit on database from MB")
    private int messageWriterBatchSize = 70;

    @Element(description = "Timeout for waiting for a queue purge event to end to get the purged count. Doesn't\n"
            + "            affect actual purging. If purge takes time, increasing the value will improve the\n"
            + "            possibility of retrieving the correct purged count. Having a lower value doesn't stop\n"
            + "            purge event. Getting the purged count is affected by this")
    private int purgedCountTimeout = 180;

    @Element(description = "Number of parallel writers used to write content to message store for transaction\n"
            + "            based publishing. Increasing this value will speedup commit duration for a transaction.\n"
            + "            But the load on the data store will increase.")
    private int transactionMessageWriters = 1;

    public int getParallelMessageWriters() {
        return parallelMessageWriters;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getMessageWriterBatchSize() {
        return messageWriterBatchSize;
    }

    public int getPurgedCountTimeout() {
        return purgedCountTimeout;
    }

    public int getTransactionMessageWriters() {
        return transactionMessageWriters;
    }
}