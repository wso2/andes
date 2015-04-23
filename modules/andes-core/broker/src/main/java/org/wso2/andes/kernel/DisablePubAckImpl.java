package org.wso2.andes.kernel;

import org.wso2.andes.kernel.distruptor.inbound.PubAckHandler;

/**
 * Drops any request to send publisher acknowledgements.
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
