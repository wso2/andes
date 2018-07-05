/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.dna.mqtt.moquette.server.netty.metrics;

import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.configuration.modules.JKSStore;
import org.wso2.andes.transport.ConnectionSettings;
import org.wso2.andes.transport.network.security.ssl.SSLUtil;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Properties;

/**
 * This class is responsible of creating ssl context for mqtt
 */
public class SSLHandlerFactory {

    private static final Logger log = LoggerFactory.getLogger(SSLHandlerFactory.class);

    private SSLContext sslContext;

    public SSLHandlerFactory(Properties props) {
        this.sslContext = initSSLContext(props);
    }

    /**
     * Check whether the ssl context is null or not
     * @return
     */
    public boolean canCreate() {
        return this.sslContext != null;
    }

    /**
     * Initialize ssl context
     * @param props the configuration details
     * @return ssl context
     */
    private SSLContext initSSLContext(Properties props) {
        try {
            ConnectionSettings sslConnectionSettings = constructConnectionSettings(props);
            SSLContext serverContext = SSLUtil.createSSLContext(sslConnectionSettings);
            return serverContext;

        } catch (NoSuchAlgorithmException | UnrecoverableKeyException | CertificateException | KeyStoreException
                | KeyManagementException | IOException ex) {
            log.error("Can't start SSL layer!", ex);
            return null;
        } catch (Exception e) {
            //Here SSLUtils throws a generic type Exception, hence we need to catch it
            //This is bad anyhow
            log.error("Error while establishing ssl server context ", e);
            return null;
        }
    }

    /**
     * Constructs connection settings required for ssl connectivity
     *
     * @param props holds information necessary to establish ssl connectivity
     * @return ConnectionSettings
     */
    private ConnectionSettings constructConnectionSettings(Properties props) {
        ConnectionSettings connectionSettings = new ConnectionSettings();
        
        String sslTrustStoreLocation = ((JKSStore) AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_TRUSTSTORE)).getStoreLocation();
        String sslTrustStorePassword = ((JKSStore) AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_TRUSTSTORE)).getPassword();

        String trustStoreCertificateType = ((JKSStore) AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_TRUSTSTORE)).getStoreAlgorithm();
        
        String sslKeyStoreLocation = ((JKSStore) AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_KEYSTORE)).getStoreLocation();
        String sslKeyStorePassword = ((JKSStore) AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_KEYSTORE)).getPassword();

        String keyStoreCertificateType = ((JKSStore) AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_KEYSTORE)).getStoreAlgorithm();

        connectionSettings.setTrustStorePath(sslTrustStoreLocation);
        connectionSettings.setTrustStorePassword(sslTrustStorePassword);
        connectionSettings.setTrustStoreCertType(trustStoreCertificateType);

        connectionSettings.setKeyStorePath(sslKeyStoreLocation);
        connectionSettings.setKeyStorePassword(sslKeyStorePassword);
        connectionSettings.setKeyStoreCertType(keyStoreCertificateType);

        return connectionSettings;
    }

    /**
     * Create sslHandler object
     * @return SslHandler object
     */
    public ChannelHandler create() {
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(false);

        String protocols = AndesConfigurationManager
                .readValue(AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_PROTOCOLS);
        if (!protocols.isEmpty()) {
            sslEngine.setEnabledProtocols(protocols.split(","));
        }

        String ciphers = AndesConfigurationManager.readValue(AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_CIPHERS);
        if (!ciphers.isEmpty()) {
            sslEngine.setEnabledCipherSuites(ciphers.split(","));
        }

        return new SslHandler(sslEngine);
    }

}

