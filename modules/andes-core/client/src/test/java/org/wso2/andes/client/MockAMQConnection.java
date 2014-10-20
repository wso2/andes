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
package org.wso2.andes.client;

import org.wso2.andes.AMQException;
import org.wso2.andes.client.state.AMQState;
import org.wso2.andes.framing.ProtocolVersion;
import org.wso2.andes.jms.ConnectionURL;
import org.wso2.andes.jms.BrokerDetails;
import org.wso2.andes.url.URLSyntaxException;

import java.io.IOException;

public class MockAMQConnection extends AMQConnection
{
    public MockAMQConnection(String broker, String username, String password, String clientName, String virtualHost)
            throws AMQException, URLSyntaxException
    {
        super(broker, username, password, clientName, virtualHost);
    }

    public MockAMQConnection(String broker, String username, String password, String clientName, String virtualHost, SSLConfiguration sslConfig)
            throws AMQException, URLSyntaxException
    {
        super(broker, username, password, clientName, virtualHost, sslConfig);
    }

    public MockAMQConnection(String host, int port, String username, String password, String clientName, String virtualHost)
            throws AMQException, URLSyntaxException
    {
        super(host, port, username, password, clientName, virtualHost);
    }

    public MockAMQConnection(String host, int port, String username, String password, String clientName, String virtualHost, SSLConfiguration sslConfig)
            throws AMQException, URLSyntaxException
    {
        super(host, port, username, password, clientName, virtualHost, sslConfig);
    }

    public MockAMQConnection(String host, int port, boolean useSSL, String username, String password, String clientName, String virtualHost, SSLConfiguration sslConfig)
            throws AMQException, URLSyntaxException
    {
        super(host, port, useSSL, username, password, clientName, virtualHost, sslConfig);
    }

    public MockAMQConnection(String connection)
            throws AMQException, URLSyntaxException
    {
        super(connection);
    }

    public MockAMQConnection(String connection, SSLConfiguration sslConfig)
            throws AMQException, URLSyntaxException
    {
        super(connection, sslConfig);
    }

    public MockAMQConnection(ConnectionURL connectionURL, SSLConfiguration sslConfig)
            throws AMQException
    {
        super(connectionURL, sslConfig);
    }

    @Override
    public ProtocolVersion makeBrokerConnection(BrokerDetails brokerDetail) throws IOException
    {
        _connected = true;
        _protocolHandler.getStateManager().changeState(AMQState.CONNECTION_OPEN);
        return null;
    }
}
