package org.wso2.andes.kernel;

import org.wso2.andes.server.store.StorableMessageMetaData;

import java.nio.ByteBuffer;

/**
 * Will be used to handle meta data, when it comes to clone copies of it.
 */
public interface MetaDataHandler {
    /**
     * Will be called when its required to clone a copy of the meta information, this will be called during durable
     * subscriptions of AMQP and clean session in MQTT
     *
     * @param routingKey        the key wich described the destination of the message
     * @param buf               the buffer instance which will refer to the message meta information header
     * @param originalMeataData the meta data of the message
     * @param exchange          the exchange wich is defined through AMQP
     * @return the cloned copy of the message
     */
    public byte[] constructMetadata(String routingKey, ByteBuffer buf, StorableMessageMetaData originalMeataData,
                                    String exchange);
}
