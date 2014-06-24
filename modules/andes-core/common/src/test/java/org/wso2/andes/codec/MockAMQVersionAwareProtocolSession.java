package org.wso2.andes.qpid.codec;
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


import java.nio.ByteBuffer;

import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQDataBlock;
import org.wso2.andes.framing.AMQMethodBody;
import org.wso2.andes.framing.ContentBody;
import org.wso2.andes.framing.ContentHeaderBody;
import org.wso2.andes.framing.HeartbeatBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.framing.ProtocolVersion;
import org.wso2.andes.protocol.AMQVersionAwareProtocolSession;
import org.wso2.andes.transport.Sender;

public class MockAMQVersionAwareProtocolSession implements AMQVersionAwareProtocolSession
{

    public void contentBodyReceived(int channelId, ContentBody body) throws AMQException
    {
        // TODO Auto-generated method stub

    }

    public void contentHeaderReceived(int channelId, ContentHeaderBody body) throws AMQException
    {
        // TODO Auto-generated method stub

    }

    public MethodRegistry getMethodRegistry()
    {
        return MethodRegistry.getMethodRegistry(ProtocolVersion.v0_9);
    }

    public void heartbeatBodyReceived(int channelId, HeartbeatBody body) throws AMQException
    {
        // TODO Auto-generated method stub

    }

    public void init()
    {
        // TODO Auto-generated method stub

    }

    public void methodFrameReceived(int channelId, AMQMethodBody body) throws AMQException
    {
        // TODO Auto-generated method stub

    }

    public void setSender(Sender<ByteBuffer> sender)
    {
        // TODO Auto-generated method stub

    }

    public void writeFrame(AMQDataBlock frame)
    {
        // TODO Auto-generated method stub

    }

    public byte getProtocolMajorVersion()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    public byte getProtocolMinorVersion()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    public ProtocolVersion getProtocolVersion()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
