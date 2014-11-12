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

        // modify routing key to the binding name
        MessagePublishInfo messagePublishInfo = new CustomMessagePublishInfo(
                originalMeataData);
        messagePublishInfo.setRoutingKey(new AMQShortString(routingKey));
        messagePublishInfo.setExchange(new AMQShortString(exchange));
        MessageMetaData modifiedMetaData = new MessageMetaData(
                messagePublishInfo, contentHeaderBody, contentChunkCount,
                arrivalTime);

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
