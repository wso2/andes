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
package org.wso2.andes.configuration.models.transport.amqp;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for amqp messaging transport
 */
@Configuration(description = "Broker amqp messaging transport config")
public class AMQPTransportConfiguration {

    @Element(description = "Enabler for amqp transport")
    private boolean enabled = true;

    @Element(description = "Bind address for amqp transport")
    private String bindAddress = "0.0.0.0";

    @Element(description = "Max delivery attempts for amqp transport")
    private int maximumRedeliveryAttempts = 10;

    @Element(description = "Enabler of shared topic subscriptions for amqp transport")
    private boolean allowSharedTopicSubscriptions = false;

    @Element(description = "Enabler of strict name validation for amqp transport")
    private boolean allowStrictNameValidation = true;

    @Element(description = "Default connection configuration for amqp transport")
    private AMQPDefaultConnectionConfiguration defaultConnection = new AMQPDefaultConnectionConfiguration();

    @Element(description = "SSL connection configuration for amqp transport")
    private AMQPSSLConnectionConfiguration sslConnection = new AMQPSSLConnectionConfiguration();

    public boolean getEnabled() {
        return enabled;
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public int getMaximumRedeliveryAttempts() {
        return maximumRedeliveryAttempts;
    }

    public boolean isAllowSharedTopicSubscriptions() {
        return allowSharedTopicSubscriptions;
    }

    public boolean isAllowStrictNameValidation() {
        return allowStrictNameValidation;
    }

    public AMQPDefaultConnectionConfiguration getDefaultConnection() {
        return defaultConnection;
    }

    public AMQPSSLConnectionConfiguration getSslConnection() {
        return sslConnection;
    }

}
