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
package org.wso2.andes.transport.network.mina;

import java.net.SocketAddress;
import java.nio.ByteBuffer;

import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.TrafficMask;
import org.wso2.andes.transport.Sender;
import org.wso2.andes.transport.network.NetworkConnection;

public class MinaNetworkConnection implements NetworkConnection
{
    private IoSession _session;
    private Sender<ByteBuffer> _sender;
    private volatile boolean _blocked = false;

    public MinaNetworkConnection(IoSession session)
    {
        _session = session;
        _sender = new MinaSender(_session);
    }

    public Sender<ByteBuffer> getSender()
    {
        return _sender;
    }
    
    public void close()
    {
        _session.close();
    }

    public SocketAddress getRemoteAddress()
    {
        return _session.getRemoteAddress();
    }

    public SocketAddress getLocalAddress()
    {
        return _session.getLocalAddress();
    }

    public long getReadBytes()
    {
        return _session.getReadBytes();
    }

    public long getWrittenBytes()
    {
        return _session.getWrittenBytes();
    }

    public void setMaxWriteIdle(int sec)
    {
        _session.setIdleTime(IdleStatus.WRITER_IDLE, sec);
    }

    public void setMaxReadIdle(int sec)
    {
        _session.setIdleTime(IdleStatus.READER_IDLE, sec);
    }

    @Override
    public void block() {
        _blocked = true;
        _session.suspendRead();
    }

    @Override
    public boolean isBlocked() {
        return _blocked;
    }

    @Override
    public void unblock() {
        _blocked = false;
        _session.resumeRead();
    }

}
