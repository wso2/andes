package org.wso2.andes.kernel.distruptor.inbound;

import org.wso2.andes.kernel.AndesMessageMetadata;

/**
 * Interface to handle publisher side acknowledgement
 */
public interface PubAckHandler {

    /**
     * Acknowledge message was successfully persisted
     */
    public void ack(AndesMessageMetadata metadata);

    /**
     * Negative Acknowledgement to inform failure to persist message
     */
    public void nack(AndesMessageMetadata metadata);
}
