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

import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.ContentHeaderBody;
import org.wso2.andes.framing.EncodingUtils;
import org.wso2.andes.server.store.MessageMetaDataType;
import org.wso2.andes.server.store.StorableMessageMetaData;

import java.nio.ByteBuffer;

public class CustomMessagePublishInfo implements org.wso2.andes.framing.abstraction.MessagePublishInfo {

    private org.apache.mina.common.ByteBuffer minaSrc;
    private int size;
    public ContentHeaderBody chb;
    private AMQShortString exchange;
    private AMQShortString routingKey;
    private byte flags;
    public long arrivalTime;

    private static final byte MANDATORY_FLAG = 1;
    private static final byte IMMEDIATE_FLAG = 2;

    public CustomMessagePublishInfo(AMQMessage message) {

        StorableMessageMetaData metaData = message.getMessageMetaData();

        //convert metadata to bytes
        final int bodySize = 1 + metaData.getStorableSize();
        byte[] underlying = new byte[bodySize];
        underlying[0] = (byte) metaData.getType().ordinal();
        ByteBuffer buf = ByteBuffer.wrap(underlying);
        buf.position(1);
        buf = buf.slice();

        metaData.writeToBuffer(0, buf);

        ByteBuffer bufNew = ByteBuffer.wrap(underlying);
        bufNew.position(1);
        bufNew = bufNew.slice();

        //read the buffer
        MessageMetaDataType type = MessageMetaDataType.values()[underlying[0]];
        createMetaData(bufNew);

    }

    public CustomMessagePublishInfo(StorableMessageMetaData metaData) {

        //convert metadata to bytes
        final int bodySize = 1 + metaData.getStorableSize();
        byte[] underlying = new byte[bodySize];
        underlying[0] = (byte) metaData.getType().ordinal();
        ByteBuffer buf = ByteBuffer.wrap(underlying);
        buf.position(1);
        buf = buf.slice();

        metaData.writeToBuffer(0, buf);

        ByteBuffer bufNew = ByteBuffer.wrap(underlying);
        bufNew.position(1);
        bufNew = bufNew.slice();

        //read the buffer
        MessageMetaDataType type = MessageMetaDataType.values()[underlying[0]];
        createMetaData(bufNew);
    }

    private void createMetaData(ByteBuffer buf) {
        try {
            minaSrc = org.apache.mina.common.ByteBuffer.wrap(buf);
            size = EncodingUtils.readInteger(minaSrc);
            chb = ContentHeaderBody.createFromBuffer(minaSrc, size);
            exchange = EncodingUtils.readAMQShortString(minaSrc);
            routingKey = EncodingUtils.readAMQShortString(minaSrc);
            flags = EncodingUtils.readByte(minaSrc);
            arrivalTime = EncodingUtils.readLong(minaSrc);

        } catch (AMQException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AMQShortString getExchange() {
        return exchange;
    }

    @Override
    public void setExchange(AMQShortString exchange) {
        this.exchange = exchange;
    }

    public void setRoutingKey(AMQShortString routingKey) {
        this.routingKey = routingKey;
    }

    @Override
    public boolean isImmediate() {
        return (flags & IMMEDIATE_FLAG) != 0;
    }

    @Override
    public boolean isMandatory() {
        return (flags & MANDATORY_FLAG) != 0;
    }

    @Override
    public AMQShortString getRoutingKey() {
        return routingKey;
    }
}
