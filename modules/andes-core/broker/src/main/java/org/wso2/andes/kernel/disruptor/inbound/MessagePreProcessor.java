/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.disruptor.inbound;

import com.lmax.disruptor.EventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.router.AndesMessageRouter;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.metrics.MetricsConstants;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.tools.utils.MessageTracer;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.Meter;
import org.wso2.carbon.metrics.manager.MetricManager;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * This event processor goes through the ring buffer first and update AndesMessage data event objects.
 * NOTE: Only one instance of this processor should process events from the ring buffer
 */
public class MessagePreProcessor implements EventHandler<InboundEventContainer> {

    private static final Log log = LogFactory.getLog(MessagePreProcessor.class);
    private final MessageIDGenerator idGenerator;

    public MessagePreProcessor() {
        idGenerator = new MessageIDGenerator();
    }

    @Override
    public void onEvent(InboundEventContainer inboundEvent, long sequence, boolean endOfBatch ) throws Exception {
        switch (inboundEvent.getEventType()) {
            case MESSAGE_EVENT:
                // NOTE: This is the MESSAGE_EVENT and this is the first processing event for this message
                // published to ring. Therefore there should be exactly one message in the list.
                // NO NEED TO CHECK FOR LIST SIZE
                AndesMessage message = inboundEvent.popMessage();
                updateRoutingInformation(inboundEvent, message, sequence);
                break;
            case TRANSACTION_COMMIT_EVENT:
                preProcessTransaction(inboundEvent, sequence);
                break;
            case SAFE_ZONE_DECLARE_EVENT:
                setSafeZoneLimit(inboundEvent, sequence);
                break;
            case PUBLISHER_RECOVERY_EVENT:
                inboundEvent.setRecoveryEventMessageId(idGenerator.getNextId());
                break;
            default:
                if (log.isDebugEnabled()) {
                    log.debug("[ sequence " + sequence + "] Unhandled event " + inboundEvent.eventInfo());
                }
                break;
        }
        inboundEvent.preProcessed = true;
    }

    /**
     * Pre process transaction related messages. This is
     * @param eventContainer InboundEventContainer
     * @param sequence Disruptor ring sequence number.
     * @throws AndesException
     */
    private void preProcessTransaction(InboundEventContainer eventContainer, long sequence) throws AndesException {

        // Routing information of all the messages of current transaction is updated.
        // Messages duplicated as needed.
        Collection<AndesMessage> messageList = eventContainer.getTransactionEvent().getQueuedMessages();
        for (AndesMessage message : messageList) {
            updateRoutingInformation(eventContainer, message, sequence);
        }

        // Internal message list of transaction object is updated to reflect the messages
        // actually written to DB
        eventContainer.getTransactionEvent().clearMessages();
        eventContainer.getTransactionEvent().addMessages(eventContainer.getMessageList());
    }

    /**
     * Calculate the current safe zone for this node (using the last generated message ID)
     * @param event event
     * @param sequence position of the event at the event ring buffer
     */
    private void setSafeZoneLimit(InboundEventContainer event, long sequence) {
        long safeZoneLimit = idGenerator.getNextId();
        event.setSafeZoneLimit(safeZoneLimit);
        if(log.isDebugEnabled()){
            log.debug("[ Sequence " + sequence + " ] Pre processing message. Setting the Safe Zone " + safeZoneLimit);
        }
    }

    /**
     * Route the message to queue/queues of subscribers matching in AMQP way. Hierarchical topic message routing is
     * evaluated here. This will duplicate message for each "subscription destination (not message destination)" at
     * different nodes
     *
     * @param event InboundEventContainer containing the message list
     * @param message Routing details updated for the given {@link org.wso2.andes.kernel.AndesMessage}
     * @param sequence Disruptor slot sequence number
     */
    private void updateRoutingInformation(InboundEventContainer event, AndesMessage message, long sequence) {

        AndesChannel andesChannel = event.getChannel();

        // Messages are processed in the order they arrive at ring buffer By this processor.
        // By setting message ID through message pre processor we assure, even in a multi publisher scenario, there is
        // no message id ordering issue at node level.
        setMessageID(message);

        if(log.isDebugEnabled()){
            log.debug("[ Sequence " + sequence + " ] Pre processing message. Message ID "
                    + message.getMetadata().getMessageID());
        }

        preProcessIncomingMessage(event, message, andesChannel);
    }

    private void preProcessIncomingMessage(InboundEventContainer event, AndesMessage message,
                                           AndesChannel andesChannel) {

        boolean isMessageRouted = false;

        //Get storage queues bound to the message router
        String messageRouterName = message.getMetadata().getMessageRouterName();
        AndesMessageRouter messageRouter = AndesContext.getInstance().
                getMessageRouterRegistry().getMessageRouter(messageRouterName);

        //do topic matching with the routing key of the message and get a list of
        //mating binding keys
        Set<StorageQueue> matchingQueues = messageRouter.getMatchingStorageQueues(message);

        boolean originalMessageConsumed = false;

        for (StorageQueue matchingQueue : matchingQueues) {

            if (!originalMessageConsumed) {
                message.getMetadata().setStorageQueueName(matchingQueue.getName());

                // add the topic wise cloned message to the events list. Message writers will pick that and
                // write it.
                event.addMessage(message);
                originalMessageConsumed = true;

            } else {
                AndesMessage clonedMessage = cloneAndesMessageMetadataAndContent(message);

                //Message should be written to storage queue name. This is
                //determined by destination of the message. So should be
                //updated (but internal metadata will have topic name as usual)
                clonedMessage.getMetadata().setStorageQueueName(matchingQueue.getName());

                /**
                 * Update cloned message metadata if isCompressed set true.
                 */
                if (clonedMessage.getMetadata().isCompressed()) {
                    clonedMessage.getMetadata().updateMetadata(true);
                }

                if (MessageTracer.isEnabled()) {
                    MessageTracer.trace(message, MessageTracer.MESSAGE_CLONED + clonedMessage.getMetadata()
                            .getMessageID() + " for " + clonedMessage.getMetadata().getStorageQueueName());
                }

                // add the topic wise cloned message to the events list. Message writers will pick that and
                // write it.
                event.addMessage(clonedMessage);
            }

            isMessageRouted = true;
        }



        // If there is no matching subscriber at the moment there is no point of storing the message
        if (!isMessageRouted) {

            // Even though we drop the message pub ack needs to be sent
            event.pubAckHandler.ack(message.getMetadata());

            //Adding metrics meter for ack rate
            Meter ackMeter = MetricManager.meter(MetricsConstants.ACK_SENT_RATE
                    + MetricsConstants.METRICS_NAME_SEPARATOR + message.getMetadata().getMessageRouterName()
                    + MetricsConstants.METRICS_NAME_SEPARATOR + message.getMetadata().getDestination(), Level.INFO);
            ackMeter.mark();

            // since inbound message has no routes, inbound message list will be cleared.
            event.clearMessageList(andesChannel);
            log.info("Message routing key: " + message.getMetadata().getDestination() + " No routes in " +
                    "cluster. Ignoring Message id " + message.getMetadata().getMessageID());
        }
    }

    /**
     * Create a clone of the message
     *
     * @param message message to be cloned
     * @return Cloned reference of AndesMessage
     */
    private AndesMessage cloneAndesMessageMetadataAndContent(AndesMessage message) {
        long newMessageId = idGenerator.getNextId();
        AndesMessageMetadata clonedMetadata = message.getMetadata().shallowCopy(newMessageId);
        AndesMessage clonedMessage = new AndesMessage(clonedMetadata);

        //Duplicate message content
        List<AndesMessagePart> messageParts = message.getContentChunkList();
        for (AndesMessagePart messagePart : messageParts) {
            clonedMessage.addMessagePart(messagePart.shallowCopy(newMessageId));
        }

        return clonedMessage;

    }

    /**
     * Set Message ID for AndesMessage.
     * @param message messageID
     */
    private void setMessageID(AndesMessage message) {
        long messageId = idGenerator.getNextId();

        //Tracing message
        if (MessageTracer.isEnabled()) {
            MessageTracer.trace(message, MessageTracer.MESSAGE_ID_MAPPED + " id: " + messageId);
        }

        message.getMetadata().setMessageID(messageId);

        for (AndesMessagePart messagePart: message.getContentChunkList()) {
            messagePart.setMessageID(messageId);
        }
    }

    /**
     * Generates IDs. This id generator cannot be used in a multi threaded environment. Removed any locking behaviour to
     * improve id generation in single threaded approach
     */
    private static class MessageIDGenerator {

        /** REFERENCE_START time set to 2011 */
        private static final long REFERENCE_START = 41L * 365L * 24L * 60L * 60L * 1000L;
        private int uniqueIdForNode;
        private long lastTimestamp;
        private long lastID;
        private int offset;

        MessageIDGenerator() {
            uniqueIdForNode = 0;
            lastTimestamp = 0;
            lastID = 0;
            offset = 0;
        }

        /**
         * Out of 64 bits for long, we will use the range as follows
         * [1 sign bit][45bits for time spent from reference time in milliseconds][8bit node id][10 bit offset for ID
         * falls within the same timestamp]
         * This assumes there will not be more than 1024 hits within a given millisecond. Range is sufficient for
         * 6029925857 years.
         *
         * @return Generated ID
         */
        public long getNextId() {

            // id might change at runtime. Hence reading the value
            uniqueIdForNode = ClusterResourceHolder.getInstance().getClusterManager().getUniqueIdForLocalNode();
            long ts = System.currentTimeMillis();

            if (ts == lastTimestamp) {
                offset = offset + 1;
            } else {
                offset = 0;
            }
            lastTimestamp = ts;
            long id = (ts - REFERENCE_START) * 256 * 1024 + uniqueIdForNode * 1024 + offset;
            if (lastID == id) {
                throw new RuntimeException("duplicate ids detected. This should never happen");
            }
            lastID = id;
            return id;
        }
    }
}
