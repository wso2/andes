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
package org.wso2.andes.configuration.models.transport.mqtt;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for mqtt messaging transport.
 */
@Configuration(description = "Broker mqtt messaging transport config")
public class MQTTTransportConfiguration {

    @Element(description = "Enabler for mqtt transport")
    private boolean enabled = true;

    @Element(description = "Bind address for mqtt transport")
    private String bindAddress = "0.0.0.0";

    @Element(description = "All receiving events/messages will be in this ring buffer. Ring buffer size \n"
            + "of MQTT inbound event disruptor. Default is set to 32768 (1024 * 32) \n"
            + " Having a large ring buffer will have a increase memory usage and will improve performance \n"
            + "and vise versa")
    private int inboundBufferSize = 32768;

    @Element(description = "Messages delivered to clients will be placed in this ring buffer. \n"
            + "Ring buffer size of MQTT delivery event disruptor. Default is set to 32768 (1024 * 32) \n"
            + "Having a large ring buffer will have a increase memory usage and will improve performance"
            + "and vise versa")
    private int deliveryBufferSize = 32768;

    @Element(description = "Security configuration related to mqtt transport")
    private MQTTSecurityConfiguration security = new MQTTSecurityConfiguration();

    @Element(description = "Default connection configuration for mqtt transport")
    private MQTTDefaultConnectionConfiguration defaultConnection = new MQTTDefaultConnectionConfiguration();

    @Element(description = "SSL connection configuration for mqtt transport")
    private MQTTSSLConnectionConfiguration sslConnection = new MQTTSSLConnectionConfiguration();

    public boolean getEnabled() {
        return enabled;
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public int getInboundBufferSize() {
        return inboundBufferSize;
    }

    public int getDeliveryBufferSize() {
        return deliveryBufferSize;
    }

    public MQTTSecurityConfiguration getSecurity() {
        return security;
    }

    public MQTTDefaultConnectionConfiguration getDefaultConnection() {
        return defaultConnection;
    }

    public MQTTSSLConnectionConfiguration getSslConnection() {
        return sslConnection;
    }

}
