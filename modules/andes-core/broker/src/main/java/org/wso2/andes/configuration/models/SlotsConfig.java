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
 * Configuration model for slots config in performance tuning section.
 */
@Configuration(description = "Slots related configurations.")
public class SlotsConfig {

    @Element(description = "Rough estimate for size of a slot. What is meant by size is the number of messages\n"
            + "contained within bounties of a slot.")
    private int windowSize = 1000;

    @Element(description = "If message publishers are slow, time taken to fill the slot (up to <windowSize>) will be longer.\n"
            + "This will add an latency to messages. Therefore broker will mark the slot as\n"
            + "ready to deliver before even the slot is entirely filled after specified time.\n"
            + "NOTE: specified in milliseconds.")
    private int messageAccumulationTimeout = 2000;

    @Element(description = "Time interval which broker check for slots that can be marked as 'ready to deliver'\n"
            + "(- slots which have a aged more than 'messageAccumulationTimeout')\n"
            + "NOTE: specified in milliseconds.")
    private int maxSubmitDelay = 1000;

    @Element(description = "Number of MessageDeliveryWorker threads that should be started")
    private int deliveryThreadCount = 5;

    @Element(description = "Number of parallel threads to execute slot deletion task. Increasing this value will remove slots\n"
            + "whose messages are read/delivered to consumers/acknowledged faster reducing heap memory used by\n"
            + "server.")
    private int deleteThreadCount = 5;

    @Element(description = "Max number of pending message count to delete per Slot Deleting Task. This config is used to raise\n"
            + "a WARN when pending scheduled number of slots exceeds this limit (indicate of an  issue that can lead to\n"
            + "message accumulation on server.")
    private int slotDeleteQueueDepthWarningThreshold = 1000;

    @Element(description = "Maximum number of thrift client connections that should be created in the thrift connection pool")
    private int thriftClientPoolSize = 10;

    public int getWindowSize() {
        return windowSize;
    }

    public int getMessageAccumulationTimeout() {
        return messageAccumulationTimeout;
    }

    public int getMaxSubmitDelay() {
        return maxSubmitDelay;
    }

    public int getDeliveryThreadCount() {
        return deliveryThreadCount;
    }

    public int getDeleteThreadCount() {
        return deleteThreadCount;
    }

    public int getSlotDeleteQueueDepthWarningThreshold() {
        return slotDeleteQueueDepthWarningThreshold;
    }

    public int getThriftClientPoolSize() {
        return thriftClientPoolSize;
    }
}
