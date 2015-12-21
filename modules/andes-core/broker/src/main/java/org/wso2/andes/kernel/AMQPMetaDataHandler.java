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
 * Will handle cloning of metadata at an even where AMQP message is published
 */
public class AMQPMetaDataHandler {

    /**
     * Update message metadata for AMQP; after an update of the routing key and the exchange of a message, for
     * durable topic subscriptions.
     *
     * @param routingKey       routing key of the message
     * @param buf              buffer of the original metadata
     * @param originalMetadata source metadata that needs to be copied
     * @param exchange         exchange of the message
     * @return copy of the metadata as a byte array
     */
    public static byte[] constructMetadata(String routingKey, ByteBuffer buf, StorableMessageMetaData originalMetadata,
                                           String exchange) {
        ContentHeaderBody contentHeaderBody = ((MessageMetaData) originalMetadata).getContentHeaderBody();
        int contentChunkCount = ((MessageMetaData) originalMetadata).getContentChunkCount();
        long arrivalTime = ((MessageMetaData) originalMetadata).getArrivalTime();
        long sessionID = ((MessageMetaData) originalMetadata).getPublisherSessionID();

        // modify routing key to the binding name
        MessagePublishInfo messagePublishInfo = new CustomMessagePublishInfo(originalMetadata);
        messagePublishInfo.setRoutingKey(new AMQShortString(routingKey));
        messagePublishInfo.setExchange(new AMQShortString(exchange));
        MessageMetaData modifiedMetaData = new MessageMetaData(messagePublishInfo, contentHeaderBody, sessionID,
                                                               contentChunkCount, arrivalTime);

        final int bodySize = 1 + modifiedMetaData.getStorableSize();
        byte[] underlying = new byte[bodySize];
        underlying[0] = (byte) modifiedMetaData.getType().ordinal();
        buf = java.nio.ByteBuffer.wrap(underlying);
        buf.position(1);
        buf = buf.slice();
        modifiedMetaData.writeToBuffer(0, buf);

        return underlying;
    }


    /**
     * Update message metadata for AMQP; to indicate the message is a compressed one.
     *
     * @param buf                       buffer of the original metadata
     * @param originalMetadata          source metadata that needs to be copied
     * @param newCompressedMessageValue Value to indicate if the message is compressed or not
     * @return copy of the metadata as a byte array
     */
    public static byte[] constructMetadata(ByteBuffer buf, StorableMessageMetaData originalMetadata,
                                           boolean newCompressedMessageValue) {
        ContentHeaderBody contentHeaderBody = ((MessageMetaData) originalMetadata).getContentHeaderBody();
        int contentChunkCount = ((MessageMetaData) originalMetadata).getContentChunkCount();
        long arrivalTime = ((MessageMetaData) originalMetadata).getArrivalTime();
        long sessionID = ((MessageMetaData) originalMetadata).getPublisherSessionID();

        //Modify message metadata, to update if the message is compressed or not
        MessagePublishInfo messagePublishInfo = new CustomMessagePublishInfo(originalMetadata);
        MessageMetaData modifiedMetaData = new MessageMetaData(messagePublishInfo, contentHeaderBody, sessionID,
                contentChunkCount, arrivalTime, newCompressedMessageValue);

        //bodySize = (1 for metadata type) + (size of metadata)
        final int bodySize = modifiedMetaData.getStorableSize() + 1;

        byte[] underlying = new byte[bodySize];

        //Writing metadata into a byte array
        //Write metadata type: as a position in its enum declaration
        underlying[0] = (byte) modifiedMetaData.getType().ordinal();

        //Wraps byte array into a buffer. Modifications to the buffer will cause the array.
        buf = java.nio.ByteBuffer.wrap(underlying);

        buf.position(1);

        //Creates a new byte buffer whose content is a shared subsequence of this buffer's content. Content of the
        // new buffer will start at this buffer's current position.
        buf = buf.slice();

        //Writing modified metadata into the buffer
        modifiedMetaData.writeToBuffer(0, buf);

        return underlying;
    }
}
