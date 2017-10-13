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
 * Configuration model for Topic delivery strategy config.
 */
@Configuration(description = "When delivering topic messages to multiple topic\n"
        + "subscribers one of following stratigies can be choosen.\n"
        + "               1. DISCARD_NONE     - Broker do not loose any message to any subscriber.\n"
        + "                                     When there are slow subscribers this can cause broker\n"
        + "                                     go Out of Memory.\n"
        + "               2. SLOWEST_SUB_RATE - Broker deliver to the speed of the slowest\n"
        + "                                     topic subscriber. This can cause fast subscribers\n"
        + "                                     to starve. But eliminate Out of Memory issue.\n"
        + "               3. DISCARD_ALLOWED  - Broker will try best to deliver. To eliminate Out\n"
        + "                                     of Memory threat broker limits sent but not acked message\n"
        + "                                     count to <maxUnackedMessages>.\n"
        + "                                     If it is breached, and <deliveryTimeout> is also\n"
        + "                                     breached message can either be lost or actually\n"
        + "                                     sent but ack is not honoured.")
public class TopicMessageDeliveryStrategyConfig {
    private String strategyName = "DISCARD_NONE";

    @Element(description = "If you choose DISCARD_ALLOWED topic message delivery strategy,\n"
            + "broker keep messages in memory until ack is done until this timeout.\n"
            + "If an ack is not received under this timeout, ack will be simulated\n"
            + "internally and real acknowledgement is discarded.\n"
            + "deliveryTimeout is in seconds")
    private int deliveryTimeout = 60;

    public String getStrategyName() {
        return strategyName;
    }

    public int getDeliveryTimeout() {
        return deliveryTimeout;
    }
}