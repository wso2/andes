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
package org.wso2.andes.server.message;

import org.wso2.andes.server.configuration.SessionConfig;
import org.wso2.andes.server.store.StoredMessage;
import org.wso2.andes.server.transport.ServerSession;
import org.wso2.andes.transport.Header;
import org.wso2.andes.transport.Session;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;


public class MessageTransferMessage implements InboundMessage, ServerMessage
{


    private StoredMessage<MessageMetaData_0_10> _storeMessage;


    private WeakReference<Session> _sessionRef;

    public MessageTransferMessage(StoredMessage<MessageMetaData_0_10> storeMessage, WeakReference<Session> sessionRef)
    {

        _storeMessage = storeMessage;
        _sessionRef = sessionRef;
    }

    private MessageMetaData_0_10 getMetaData()
    {
        return _storeMessage.getMetaData();
    }

    public String getRoutingKey()
    {
        return getMetaData().getRoutingKey();

    }

    public AMQMessageHeader getMessageHeader()
    {
        return getMetaData().getMessageHeader();
    }

    public boolean isPersistent()
    {
        return getMetaData().isPersistent();
    }


    public boolean isRedelivered()
    {
        // The *Message* is never redelivered, only queue entries are... this is here so that filters
        // can run against the message on entry to an exchange
        return false;
    }

    public long getSize()
    {

        return getMetaData().getSize();
    }

    public boolean isImmediate()
    {
        return getMetaData().isImmediate();
    }

    public long getExpiration()
    {
        return getMetaData().getExpiration();
    }

    public MessageReference newReference()
    {
        return new TransferMessageReference(this);
    }

    public Long getMessageNumber()
    {
        return _storeMessage.getMessageNumber();
    }

    public long getArrivalTime()
    {
        return getMetaData().getArrivalTime();
    }

    public int getContent(ByteBuffer buf, int offset)
    {
        return _storeMessage.getContent(offset, buf);
    }

    public Header getHeader()
    {
        return getMetaData().getHeader();
    }

    public ByteBuffer getBody()
    {
        ByteBuffer body = getMetaData().getBody();
        if(body == null && getSize() != 0l)
        {
            final int size = (int) getSize();
            int pos = 0;
            body = ByteBuffer.allocate(size);

            while(pos < size)
            {
                pos += getContent(body, pos);
            }

            body.flip();

            getMetaData().setBody(body.duplicate());
        }
        return body;
    }

    public Session getSession()
    {
        return _sessionRef == null ? null : _sessionRef.get();
    }

    public SessionConfig getSessionConfig()
    {
        return _sessionRef == null ? null : (ServerSession) _sessionRef.get();
    }
    
}
