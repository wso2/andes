package org.wso2.andes.kernel;

import org.wso2.andes.kernel.distruptor.inbound.PubAckHandler;

/**
 * This Implementation of ack handler drops any ack or nack received
 * from Andes.
 *
 * For instance AMQP 0.91 doesn't support publisher acknowledgements. For that use
 * case publisher acknowledgements can be dropped using this handler
 */
public class DisablePubAckImpl implements PubAckHandler{

    /**
     * Drops the publisher acknowledgment.
     * @param metadata AndesMessageMetadata
     */
    @Override
    public void ack(AndesMessageMetadata metadata) {
        // Do nothing
    }

    /**
     * Drop publisher negative acknowledgment
     * @param metadata AndesMessageMetadata
     */
    @Override
    public void nack(AndesMessageMetadata metadata) {
        // Do nothing
    }
}
