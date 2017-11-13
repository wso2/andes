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
 * Configuration model for AMQP transport.
 */
@Configuration(description = "AMQP transport related configurations. Refer qpid-config.xml for further AMQP-specific\n"
        + "configurations.")
public class AMQPConfigs {

    @Element(description = "Enable AMQP transport")
    private boolean enabled = true;

    @Element(description = "AMQP bind address")
    private String bindAddress = "0.0.0.0";

    @Element(description = "Enable default connection")
    private boolean defaultConnectionEnabled = true;

    @Element(description = "Port for the default connection ")
    private int defaultConnectionPort = 1883;

    private SSLConnectionConfig sslConnection = new SSLConnectionConfig();

    private int maximumRedeliveryAttempts = 10;

    private boolean allowSharedTopicSubscriptions = false;

    private boolean allowStrictNameValidation = true;

    public boolean isEnabled() {
        return enabled;
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public boolean isDefaultConnectionEnabled() {
        return defaultConnectionEnabled;
    }

    public int getDefaultConnectionPort() {
        return defaultConnectionPort;
    }

    public SSLConnectionConfig getSslConnection() {
        return sslConnection;
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
}