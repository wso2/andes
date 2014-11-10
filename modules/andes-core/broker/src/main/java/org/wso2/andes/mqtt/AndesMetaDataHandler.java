package org.wso2.andes.mqtt;

import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.ContentHeaderBody;
import org.wso2.andes.framing.abstraction.MessagePublishInfo;
import org.wso2.andes.server.message.CustomMessagePublishInfo;
import org.wso2.andes.server.message.MessageMetaData;
import org.wso2.andes.server.store.StorableMessageMetaData;

import java.nio.ByteBuffer;

/**
 * Created by pamod on 10/28/14.
 */
public class AndesMetaDataHandler implements MetaDataHandler {
    @Override
    public byte[] constructMetadata(String routingKey, ByteBuffer buf,StorableMessageMetaData original_mdt) {
        ContentHeaderBody contentHeaderBody = ((MessageMetaData) original_mdt)
                .getContentHeaderBody();
        int contentChunkCount = ((MessageMetaData) original_mdt)
                .getContentChunkCount();
        long arrivalTime = ((MessageMetaData) original_mdt).getArrivalTime();

        // modify routing key to the binding name
        MessagePublishInfo messagePublishInfo = new CustomMessagePublishInfo(
                original_mdt);
        messagePublishInfo.setRoutingKey(new AMQShortString(routingKey));
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
