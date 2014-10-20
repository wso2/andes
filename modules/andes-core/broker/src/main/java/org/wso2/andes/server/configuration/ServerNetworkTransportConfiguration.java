/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.andes.server.configuration;

import org.wso2.andes.transport.NetworkTransportConfiguration;

public class ServerNetworkTransportConfiguration implements NetworkTransportConfiguration
{
    private final ServerConfiguration _serverConfig;
    private final int _port;
    private final String _host;
    private final String _transport;

    public ServerNetworkTransportConfiguration(final ServerConfiguration serverConfig, 
                                               final int port, final String host,
                                               final String transport)
    {
        _serverConfig = serverConfig;
        _port = port;
        _host = host;
        _transport = transport;
    }

    public Boolean getTcpNoDelay()
    {
        return _serverConfig.getTcpNoDelay();
    }

    public Integer getSendBufferSize()
    {
        return _serverConfig.getWriteBufferSize();
    }

    public Integer getReceiveBufferSize()
    {
        return _serverConfig.getReceiveBufferSize();
    }

    public Integer getPort()
    {
        return _port;
    }

    public String getHost()
    {
        return _host;
    }

    public String getTransport()
    {
        return _transport;
    }

    public Integer getConnectorProcessors()
    {
        return _serverConfig.getConnectorProcessors();
    }
}
