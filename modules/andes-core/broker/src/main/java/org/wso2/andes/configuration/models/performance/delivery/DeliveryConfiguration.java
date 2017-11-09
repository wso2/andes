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
package org.wso2.andes.configuration.models.performance.delivery;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for message delivery related configs
 */
@Configuration(description = "Delivery related configurations")
public class DeliveryConfiguration {

    @Element(description = " Maximum number of undelivered messages that can have in memory. Increasing this\n"
            + " value increase the possibility of out of memory scenario but performance will be\n" + " improved")
    private int maxNumberOfReadButUndeliveredMessages = 1000;

    @Element(description = "This is the ring buffer size of the delivery disruptor. This value should be a\n"
            + "power of 2 (E.g. 1024, 2048, 4096). Use a small ring size if you want to reduce the\n" + "memory usage")
    private int ringBufferSize = 4096;

    @Element(description = "Number of parallel readers used to used to read content from message store.\n"
            + "Increasing this value will speed-up the message sending mechanism. But the load\n"
            + "on the data store will increase")
    private int parallelContentReaders = 5;

    @Element(description = "Number of parallel decompression handlers used to decompress messages before send to"
            + " subscribers.Increasing this value will speed-up the message decompressing mechanism. But the system "
            + "load will increase")
    private int parallelDecompressionHandlers = 5;

    @Element(description = "Number of parallel delivery handlers used to send messages to subscribers.\n"
            + "Increasing this value will speed-up the message sending mechanism. But the system load\n"
            + "will increase.")
    private int parallelDeliveryHandlers = 5;

    @Element(description = "The size of the batch represents, at a given time the number of messages that could \n"
            + "be retrieved from the database.")
    private int contentReadBatchSize = 65000;

    @Element(description = "Content cache related configurations")
    private ContentCacheConfiguration contentCache = new ContentCacheConfiguration();

    @Element(description = "Topic message delivery strategy related configs")
    private MessageDeliveryConfiguration topicMessageDeliveryStrategy = new MessageDeliveryConfiguration();

    public int getMaxNumberOfReadButUndeliveredMessages() {
        return maxNumberOfReadButUndeliveredMessages;
    }

    public int getRingBufferSize() {
        return ringBufferSize;
    }

    public int getParallelContentReaders() {
        return parallelContentReaders;
    }

    public int getParallelDecompressionHandlers() {
        return parallelDecompressionHandlers;
    }

    public int getParallelDeliveryHandlers() {
        return parallelDeliveryHandlers;
    }

    public int getContentReadBatchSize() {
        return contentReadBatchSize;
    }

    public ContentCacheConfiguration getContentCache() {
        return contentCache;
    }

    public MessageDeliveryConfiguration getTopicMessageDeliveryStrategy() {
        return topicMessageDeliveryStrategy;
    }

}
