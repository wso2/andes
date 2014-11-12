package org.wso2.andes.mqtt;

import org.wso2.andes.kernel.MetaDataHandler;
import org.wso2.andes.server.store.StorableMessageMetaData;

import java.nio.ByteBuffer;

/**
 * Will be used to clone meta information of MQTT related topic messages recived
 */
public class MQTTMetaDataHandler implements MetaDataHandler {
    @Override
    public byte[] constructMetadata(String routingKey, ByteBuffer buf, StorableMessageMetaData originalMeataData,
                                    String exchange) {

        //TODO we need to return only the reference to the byte buffer creating an array for it is nuts
        //For MQTT we just need to take a copy
        byte[] oldBytes = buf.array();
        byte[] underlying = new byte[oldBytes.length];
        //Will clone and make a copy of the bytes
        System.arraycopy(oldBytes, 0, underlying, 0, oldBytes.length);
        return underlying;

    }
}
