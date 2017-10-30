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

import org.wso2.andes.configuration.models.transport.KeyStoreConfiguration;
import org.wso2.andes.configuration.models.transport.TrustStoreConfiguration;
import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for ssl connection in mqtt transport
 */
@Configuration(description = "Broker mqtt transport based default connection config")
public class MQTTSSLConnectionConfiguration {

    @Element(description = "Enabler for ssl connection in mqtt")
    private boolean enabled = true;

    @Element(description = "Port for ssl connection in mqtt")
    private int port = 8883;

    public boolean getEnabled() {
        return enabled;
    }

    public int getPort() {
        return port;
    }

    @Element(description = "Keystore configuration for SSL connection")
    private KeyStoreConfiguration keyStore = new KeyStoreConfiguration();

    @Element(description = "TrustStore  configuration for SSL connection")
    private TrustStoreConfiguration trustStore = new TrustStoreConfiguration();

    public KeyStoreConfiguration getKeyStore() {
        return keyStore;
    }

    public TrustStoreConfiguration getTrustStore() {
        return trustStore;
    }

}
