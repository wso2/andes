package org.wso2.andes.kernel;

import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.router.AndesMessageRouter;

/**
 * Defines different destination types for each protocol.
 */
public enum DestinationType {

    TOPIC(AMQPUtils.TOPIC_EXCHANGE_NAME),
    QUEUE(AMQPUtils.DIRECT_EXCHANGE_NAME),
    DURABLE_TOPIC(AMQPUtils.TOPIC_EXCHANGE_NAME);

    private String andesMessageRouter;

    private DestinationType(String andesMessageRouter) {
        this.andesMessageRouter = andesMessageRouter;
    }

    /**
     * Get andes messages router
     *
     * @return Andes message router
     */
    public String getAndesMessageRouter() {
        return andesMessageRouter;
    }
}
