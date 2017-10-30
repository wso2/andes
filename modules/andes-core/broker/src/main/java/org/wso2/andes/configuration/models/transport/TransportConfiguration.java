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
package org.wso2.andes.configuration.models.transport;

import org.wso2.andes.configuration.models.transport.amqp.AMQPTransportConfiguration;
import org.wso2.andes.configuration.models.transport.mqtt.MQTTTransportConfiguration;
import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for broker messaging transports
 */
@Configuration(description = "Broker messaging transport config")
public class TransportConfiguration {

    @Element(description = "Configurations related to amqp messaging transport")
    private AMQPTransportConfiguration amqp = new AMQPTransportConfiguration();

    @Element(description = "Configurations related to mqtt messaging transport")
    private MQTTTransportConfiguration mqtt = new MQTTTransportConfiguration();

    public AMQPTransportConfiguration getAmqpConfiguration() {
        return amqp;
    }

    public MQTTTransportConfiguration getMqttConfiguration() {
        return mqtt;
    }

}
