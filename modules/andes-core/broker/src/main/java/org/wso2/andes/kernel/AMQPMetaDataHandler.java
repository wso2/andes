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
package org.wso2.andes.kernel;

import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.ContentHeaderBody;
import org.wso2.andes.framing.abstraction.MessagePublishInfo;
import org.wso2.andes.server.message.CustomMessagePublishInfo;
import org.wso2.andes.server.message.MessageMetaData;
import org.wso2.andes.server.store.StorableMessageMetaData;

import java.nio.ByteBuffer;

/**
 * Will handle cloning of meta data at an even where AMQP message is published
 */
public class AMQPMetaDataHandler implements MetaDataHandler {

    @Override
    public byte[] constructMetadata(String routingKey, ByteBuffer buf, StorableMessageMetaData originalMeataData,
                                    String exchange) {
        ContentHeaderBody contentHeaderBody = ((MessageMetaData) originalMeataData)
                .getContentHeaderBody();
        int contentChunkCount = ((MessageMetaData) originalMeataData)
                .getContentChunkCount();
        long arrivalTime = ((MessageMetaData) originalMeataData).getArrivalTime();
        long sessionID =((MessageMetaData) originalMeataData).getPublisherSessionID();

        // modify routing key to the binding name
        MessagePublishInfo messagePublishInfo = new CustomMessagePublishInfo(
                originalMeataData);
        messagePublishInfo.setRoutingKey(new AMQShortString(routingKey));
        messagePublishInfo.setExchange(new AMQShortString(exchange));
        MessageMetaData modifiedMetaData = new MessageMetaData(
                messagePublishInfo, contentHeaderBody, sessionID, contentChunkCount, arrivalTime);

        final int bodySize = 1 + modifiedMetaData.getStorableSize();
        byte[] underlying = new byte[bodySize];
        underlying[0] = (byte) modifiedMetaData.getType().ordinal();
        buf = java.nio.ByteBuffer.wrap(underlying);
        buf.position(1);
        buf = buf.slice();
        modifiedMetaData.writeToBuffer(0, buf);

        return underlying;
    }
}
