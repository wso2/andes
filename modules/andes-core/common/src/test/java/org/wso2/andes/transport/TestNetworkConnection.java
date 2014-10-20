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
package org.wso2.andes.transport;

import org.wso2.andes.protocol.ProtocolEngine;
import org.wso2.andes.protocol.ProtocolEngineFactory;
import org.wso2.andes.ssl.SSLContextFactory;
import org.wso2.andes.transport.network.NetworkConnection;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * Test implementation of IoSession, which is required for some tests. Methods not being used are not implemented,
 * so if this class is being used and some methods are to be used, then please update those.
 */
public class TestNetworkConnection implements NetworkConnection
{
    private String _remoteHost = "127.0.0.1";
    private String _localHost = "127.0.0.1";
    private int _port = 1;
    private SocketAddress _localAddress = null;
    private SocketAddress _remoteAddress = null;
    private final MockSender _sender;

    public TestNetworkConnection()
    {
        _sender = new MockSender();
    }

    public void bind(int port, InetAddress[] addresses, ProtocolEngineFactory protocolFactory,
            NetworkTransportConfiguration config, SSLContextFactory sslFactory) throws BindException
    {
        
    }

    public SocketAddress getLocalAddress()
    {
        return (_localAddress != null) ? _localAddress : new InetSocketAddress(_localHost, _port);
    }

    public SocketAddress getRemoteAddress()
    {
        return (_remoteAddress != null) ? _remoteAddress : new InetSocketAddress(_remoteHost, _port);
    }

    public void open(int port, InetAddress destination, ProtocolEngine engine, NetworkTransportConfiguration config,
            SSLContextFactory sslFactory) throws OpenException
    {
        
    }

    public void setMaxReadIdle(int idleTime)
    {
        
    }

    public void setMaxWriteIdle(int idleTime)
    {
        
    }

    public void close()
    {
           
    }

    public void flush()
    {
        
    }

    public void send(ByteBuffer msg)
    {
        
    }

    public void setIdleTimeout(int i)
    {
        
    }

    public void setPort(int port)
    {
        _port = port;
    }

    public int getPort()
    {
        return _port;
    }

    public void setLocalHost(String host)
    {
        _localHost = host;
    }

    public void setRemoteHost(String host)
    {
        _remoteHost = host;
    }

    public void setLocalAddress(SocketAddress address)
    {
        _localAddress = address;
    }

    public void setRemoteAddress(SocketAddress address)
    {
        _remoteAddress = address;
    }

    public Sender<ByteBuffer> getSender()
    {
        return _sender;
    }

    @Override
    public void block() {
        //Do nothing
    }

    @Override
    public void unblock() {
        //Do nothing
    }

    @Override
    public boolean isBlocked() {
        return false;
    }

}
