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
package org.wso2.andes.configuration.models.performance;

import org.wso2.andes.configuration.models.performance.delivery.DeliveryConfiguration;
import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for performance tuning related configs
 */
@Configuration(description = "Broker based performance tuning related configurations")
public class PerformanceTuningConfiguration {

    @Element(description = "Slots related configurations")
    private SlotsConfiguration slots = new SlotsConfiguration();

    @Element(description = "Delivery related configurations")
    private DeliveryConfiguration delivery = new DeliveryConfiguration();

    @Element(description = "Acknowledgement handling related configurations")
    private AckHandlingConfiguration ackHandling = new AckHandlingConfiguration();

    @Element(description = "Content handling related configurations")
    private ContentHandlingConfiguration contentHandling = new ContentHandlingConfiguration();

    @Element(description = "Inbound events related configurations")
    private InboundEventConfiguration inboundEvents = new InboundEventConfiguration();

    @Element(description = "Message expiration related configurations")
    private MessageExpirationConfiguration messageExpiration = new MessageExpirationConfiguration();

    public SlotsConfiguration getSlots() {
        return slots;
    }

    public DeliveryConfiguration getDelivery() {
        return delivery;
    }

    public AckHandlingConfiguration getAckHandling() {
        return ackHandling;
    }

    public ContentHandlingConfiguration getContentHandling() {
        return contentHandling;
    }

    public InboundEventConfiguration getInboundEvents() {
        return inboundEvents;
    }

    public MessageExpirationConfiguration getMessageExpiration() {
        return messageExpiration;
    }

}
