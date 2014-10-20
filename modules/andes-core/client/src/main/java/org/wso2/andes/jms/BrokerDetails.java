/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.jms;

import java.util.Map;

import org.wso2.andes.client.SSLConfiguration;

public interface BrokerDetails
{

    /*
     * Known URL Options
     * @see ConnectionURL
    */
    public static final String OPTIONS_RETRY = "retries";
    public static final String OPTIONS_CONNECT_TIMEOUT = "connecttimeout";
    public static final String OPTIONS_CONNECT_DELAY = "connectdelay";
    public static final String OPTIONS_IDLE_TIMEOUT = "idle_timeout"; // deprecated
    public static final String OPTIONS_HEARTBEAT = "heartbeat";
    public static final String OPTIONS_SASL_MECHS = "sasl_mechs";
    public static final String OPTIONS_SASL_ENCRYPTION = "sasl_encryption";
    public static final String OPTIONS_SSL = "ssl";
    public static final String OPTIONS_TCP_NO_DELAY = "tcp_nodelay";
    public static final String OPTIONS_SASL_PROTOCOL_NAME = "sasl_protocol";
    public static final String OPTIONS_SASL_SERVER_NAME = "sasl_server";
    
    public static final String OPTIONS_TRUST_STORE = "trust_store";
    public static final String OPTIONS_TRUST_STORE_PASSWORD = "trust_store_password";
    public static final String OPTIONS_KEY_STORE = "key_store";
    public static final String OPTIONS_KEY_STORE_PASSWORD = "key_store_password";
    public static final String OPTIONS_SSL_VERIFY_HOSTNAME = "ssl_verify_hostname";
    public static final String OPTIONS_SSL_CERT_ALIAS = "ssl_cert_alias";
    
    public static final int DEFAULT_PORT = 5672;

    public static final String TCP = "tcp";

    public static final String DEFAULT_TRANSPORT = TCP;

    public static final String URL_FORMAT_EXAMPLE =
            "<transport>://<hostname>[:<port Default=\"" + DEFAULT_PORT + "\">][?<option>='<value>'[,<option>='<value>']]";

    public static final long DEFAULT_CONNECT_TIMEOUT = 30000L;
    public static final boolean USE_SSL_DEFAULT = false;

    // pulled these properties from the new BrokerDetails class in the qpid package
    public static final String PROTOCOL_TCP = "tcp";
    public static final String PROTOCOL_TLS = "tls";

    public static final String VIRTUAL_HOST = "virtualhost";
    public static final String CLIENT_ID = "client_id";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";    

    String getHost();

    void setHost(String host);

    int getPort();

    void setPort(int port);

    String getTransport();

    void setTransport(String transport);

    String getProperty(String key);

    void setProperty(String key, String value);

    /**
     * Ex: keystore path
     *
     * @return the Properties associated with this connection.
     */
    public Map<String,String> getProperties();

    /**
     * Sets the properties associated with this connection
     *
     * @param props the new p[roperties.
     */
    public void setProperties(Map<String,String> props);

    long getTimeout();

    void setTimeout(long timeout);

    SSLConfiguration getSSLConfiguration();

    void setSSLConfiguration(SSLConfiguration sslConfiguration);
    
    boolean getBooleanProperty(String propName);

    String toString();

    boolean equals(Object o);
}
