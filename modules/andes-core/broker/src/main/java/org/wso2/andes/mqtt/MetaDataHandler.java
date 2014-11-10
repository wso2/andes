package org.wso2.andes.mqtt;

import org.wso2.andes.server.store.StorableMessageMetaData;

import java.nio.ByteBuffer;

/**
 * Created by pamod on 10/28/14.
 */
public interface MetaDataHandler {
    public byte[] constructMetadata(String routingKey, ByteBuffer buf,StorableMessageMetaData original_mdt);
}
