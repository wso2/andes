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

import org.wso2.andes.server.store.StorableMessageMetaData;
import org.wso2.andes.server.store.MessageMetaDataType;
import org.wso2.andes.transport.MessageTransfer;
import org.wso2.andes.transport.DeliveryProperties;
import org.wso2.andes.transport.MessageProperties;
import org.wso2.andes.transport.Header;
import org.wso2.andes.transport.MessageDeliveryMode;
import org.wso2.andes.transport.Struct;
import org.wso2.andes.transport.codec.BBEncoder;
import org.wso2.andes.transport.codec.BBDecoder;

import java.nio.ByteBuffer;
import java.lang.ref.SoftReference;

public class MessageMetaData_0_10 implements StorableMessageMetaData
{
    private Header _header;
    private DeliveryProperties _deliveryProps;
    private MessageProperties _messageProps;
    private MessageTransferHeader _messageHeader;
    private long _arrivalTime;
    private int _bodySize;
    private volatile SoftReference<ByteBuffer> _body;

    private static final int ENCODER_SIZE = 1 << 16;

    public static final MessageMetaDataType.Factory<MessageMetaData_0_10> FACTORY = new MetaDataFactory();

    private volatile ByteBuffer _encoded;


    public MessageMetaData_0_10(MessageTransfer xfr)
    {
        this(xfr.getHeader(), xfr.getBodySize(), xfr.getBody(), System.currentTimeMillis());
    }

    private MessageMetaData_0_10(Header header, int bodySize, long arrivalTime)
    {
        this(header, bodySize, null, arrivalTime);
    }

    private MessageMetaData_0_10(Header header, int bodySize, ByteBuffer xfrBody, long arrivalTime)
    {
        _header = header;
        if(_header != null)
        {
            _deliveryProps = _header.get(DeliveryProperties.class);
            _messageProps = _header.get(MessageProperties.class);
        }
        else
        {
            _deliveryProps = null;
            _messageProps = null;
        }
        _messageHeader = new MessageTransferHeader(_deliveryProps, _messageProps);
        _arrivalTime = arrivalTime;
        _bodySize = bodySize;



        if(xfrBody == null)
        {
            _body = null;
        }
        else
        {
            ByteBuffer body = ByteBuffer.allocate(_bodySize);
            body.put(xfrBody);
            body.flip();
            _body = new SoftReference<ByteBuffer>(body);
        }


    }



    public MessageMetaDataType getType()
    {
        return MessageMetaDataType.META_DATA_0_10;
    }

    public int getStorableSize()
    {
        ByteBuffer buf = _encoded;

        if(buf == null)
        {
            buf = encodeAsBuffer();
            _encoded = buf;
        }

        //TODO -- need to add stuff
        return buf.limit();
    }

    private ByteBuffer encodeAsBuffer()
    {
        BBEncoder encoder = new BBEncoder(ENCODER_SIZE);

        encoder.writeInt64(_arrivalTime);
        encoder.writeInt32(_bodySize);
        Struct[] headers = _header == null ? new Struct[0] : _header.getStructs();
        encoder.writeInt32(headers.length);


        for(Struct header : headers)
        {
            encoder.writeStruct32(header);

        }

        ByteBuffer buf = encoder.buffer();
        return buf;
    }

    public int writeToBuffer(int offsetInMetaData, ByteBuffer dest)
    {
        ByteBuffer buf = _encoded;

        if(buf == null)
        {
            buf = encodeAsBuffer();
            _encoded = buf;
        }

        buf = buf.duplicate();

        buf.position(offsetInMetaData);

        if(dest.remaining() < buf.limit())
        {
            buf.limit(dest.remaining());
        }
        dest.put(buf);
        return buf.limit();
    }

    public int getContentSize()
    {
        return _bodySize;
    }

    public boolean isPersistent()
    {
        return _deliveryProps == null ? false : _deliveryProps.getDeliveryMode() == MessageDeliveryMode.PERSISTENT;
    }

    //todo: Add proper method implementations here
    @Override
    public boolean isTopic() {
        return false;
    }

    @Override
    public String getDestination() {
        return null;
    }

    @Override
    public boolean isCompressed() {
        return false;
    }


    public String getRoutingKey()
    {
        return _deliveryProps == null ? null : _deliveryProps.getRoutingKey();
    }

    public AMQMessageHeader getMessageHeader()
    {
        return _messageHeader;
    }

    public long getSize()
    {

        return _bodySize;
    }

    public boolean isImmediate()
    {
        return _deliveryProps != null && _deliveryProps.getImmediate();
    }

    public long getExpiration()
    {
        return _deliveryProps == null ? 0L : _deliveryProps.getExpiration();
    }

    public long getArrivalTime()
    {
        return _arrivalTime;
    }

    public Header getHeader()
    {
        return _header;
    }

    public ByteBuffer getBody()
    {
        ByteBuffer body = _body == null ? null : _body.get();
        return body;
    }

    public void setBody(ByteBuffer body)
    {
        _body = new SoftReference<ByteBuffer>(body);
    }

    private static class MetaDataFactory implements MessageMetaDataType.Factory<MessageMetaData_0_10>
    {
        public MessageMetaData_0_10 createMetaData(ByteBuffer buf)
        {
            BBDecoder decoder = new BBDecoder();
            decoder.init(buf);

            long arrivalTime = decoder.readInt64();
            int bodySize = decoder.readInt32();
            int headerCount = decoder.readInt32();

            Struct[] headers = new Struct[headerCount];

            for(int i = 0 ; i < headerCount; i++)
            {
                headers[i] = decoder.readStruct32();
            }

            Header header = new Header(headers);

            return new MessageMetaData_0_10(header, bodySize, arrivalTime);

        }
    }
}
