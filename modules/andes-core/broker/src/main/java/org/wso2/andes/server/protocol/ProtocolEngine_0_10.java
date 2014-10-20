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
package org.wso2.andes.server.protocol;

import org.wso2.andes.protocol.ProtocolEngine;
import org.wso2.andes.transport.network.InputHandler;
import org.wso2.andes.transport.network.Assembler;
import org.wso2.andes.transport.network.Disassembler;
import org.wso2.andes.transport.network.NetworkConnection;
import org.wso2.andes.server.configuration.*;
import org.wso2.andes.server.transport.ServerConnection;
import org.wso2.andes.server.logging.messages.ConnectionMessages;
import org.wso2.andes.server.registry.IApplicationRegistry;

import java.net.SocketAddress;
import java.util.UUID;

public class ProtocolEngine_0_10  extends InputHandler implements ProtocolEngine, ConnectionConfig
{
    public static final int MAX_FRAME_SIZE = 64 * 1024 - 1;

    private NetworkConnection _network;
    private long _readBytes;
    private long _writtenBytes;
    private ServerConnection _connection;
    private final UUID _id;
    private final IApplicationRegistry _appRegistry;
    private long _createTime = System.currentTimeMillis();

    public ProtocolEngine_0_10(ServerConnection conn,
                               NetworkConnection network,
                               final IApplicationRegistry appRegistry)
    {
        super(new Assembler(conn));
        _connection = conn;
        _connection.setConnectionConfig(this);
        _network = network;
        _id = appRegistry.getConfigStore().createId();
        _appRegistry = appRegistry;

        _connection.setSender(new Disassembler(_network.getSender(), MAX_FRAME_SIZE));
        _connection.onOpen(new Runnable()
        {
            public void run()
            {
                getConfigStore().addConfiguredObject(ProtocolEngine_0_10.this);
            }
        });

        // FIXME Two log messages to maintain compatibility with earlier protocol versions
        _connection.getLogActor().message(ConnectionMessages.OPEN(null, null, false, false));
        _connection.getLogActor().message(ConnectionMessages.OPEN(null, "0-10", false, true));
    }

    public SocketAddress getRemoteAddress()
    {
        return _network.getRemoteAddress();
    }

    public SocketAddress getLocalAddress()
    {
        return _network.getLocalAddress();
    }

    public long getReadBytes()
    {
        return _readBytes;
    }

    public long getWrittenBytes()
    {
        return _writtenBytes;
    }

    public void writerIdle()
    {
        //Todo
    }

    public void readerIdle()
    {
        //Todo
    }

    public VirtualHostConfig getVirtualHost()
    {
        return _connection.getVirtualHost();
    }

    public String getAddress()
    {
        return getRemoteAddress().toString();
    }

    public Boolean isIncoming()
    {
        return true;
    }

    public Boolean isSystemConnection()
    {
        return false;
    }

    public Boolean isFederationLink()
    {
        return false;
    }

    public String getAuthId()
    {
        return _connection.getAuthorizedPrincipal() == null ? null : _connection.getAuthorizedPrincipal().getName();
    }

    public String getRemoteProcessName()
    {
        return null;
    }

    public Integer getRemotePID()
    {
        return null;
    }

    public Integer getRemoteParentPID()
    {
        return null;
    }

    public ConfigStore getConfigStore()
    {
        return _appRegistry.getConfigStore();
    }

    public UUID getId()
    {
        return _id;
    }

    public ConnectionConfigType getConfigType()
    {
        return ConnectionConfigType.getInstance();
    }

    public ConfiguredObject getParent()
    {
        return getVirtualHost();
    }

    public boolean isDurable()
    {
        return false;
    }

    @Override
    public void closed()
    {
        super.closed();
        getConfigStore().removeConfiguredObject(this);
    }

    public long getCreateTime()
    {
        return _createTime;
    }

    public Boolean isShadow()
    {
        return false;
    }
    
    public void mgmtClose()
    {
        _connection.mgmtClose();
    }
}
