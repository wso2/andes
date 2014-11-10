package org.wso2.andes.mqtt;

import org.wso2.andes.server.store.StorableMessageMetaData;

import java.nio.ByteBuffer;

/**
 * Will handle data related to MQTT
 */
public class MQTTMetaDataHandler implements MetaDataHandler {
    @Override
    public  byte[] constructMetadata(String routingKey, ByteBuffer buf,StorableMessageMetaData original_mdt) {
        final int bodySize = 1 + original_mdt.getStorableSize();
        byte[] underlying = new byte[bodySize];
        underlying[0] = (byte) original_mdt.getType().ordinal();
        buf = java.nio.ByteBuffer.wrap(underlying);
        buf.position(1);
        buf = buf.slice();
        original_mdt.writeToBuffer(0, buf);

        return underlying;

    }
}
