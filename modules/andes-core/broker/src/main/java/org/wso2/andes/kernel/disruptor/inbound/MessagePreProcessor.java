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
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.*;
import org.wso2.andes.metrics.MetricsConstants;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.store.MessageMetaDataType;
import org.wso2.andes.subscription.SubscriptionStore;
import org.wso2.andes.tools.utils.MessageTracer;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.Meter;
import org.wso2.carbon.metrics.manager.MetricManager;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This event processor goes through the ring buffer first and update AndesMessage data event objects.
 * NOTE: Only one instance of this processor should process events from the ring buffer
 */
public class MessagePreProcessor implements EventHandler<InboundEventContainer> {

    private static final Log log = LogFactory.getLog(MessagePreProcessor.class);
    private final SubscriptionStore subscriptionStore;
    private final MessageIDGenerator idGenerator;

    public MessagePreProcessor(SubscriptionStore subscriptionStore) {
        this.subscriptionStore = subscriptionStore;
        idGenerator = new MessageIDGenerator();
    }

    @Override
    public void onEvent(InboundEventContainer inboundEvent, long sequence, boolean endOfBatch ) throws Exception {
        switch (inboundEvent.getEventType()) {
            case MESSAGE_EVENT:
                // NOTE: This is the MESSAGE_EVENT and this is the first processing event for this message
                // published to ring. Therefore there should be exactly one message in the list.
                // NO NEED TO CHECK FOR LIST SIZE
                AndesMessage message = inboundEvent.getMessageList().remove(0);
                updateRoutingInformation(inboundEvent, message, sequence);
                break;
            case TRANSACTION_COMMIT_EVENT:
                preProcessTransaction(inboundEvent, sequence);
                break;
            case SAFE_ZONE_DECLARE_EVENT:
                setSafeZoneLimit(inboundEvent, sequence);
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

        int size = 0;
        for (AndesMessage message : eventContainer.getMessageList()) {
            size = size + message.getContentChunkList().size();
        }
        eventContainer.getChannel().recordRemovalFromBuffer(size);
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

        if (message.getMetadata().isTopic()) {
            handleTopicRoutine(event, message, andesChannel);
        } else {
            andesChannel.recordAdditionToBuffer(message.getContentChunkList().size());
            AndesMessageMetadata messageMetadata = message.getMetadata();
            messageMetadata.setStorageQueueName(messageMetadata.getDestination());
            event.addMessage(message);
        }
    }

    private void handleTopicRoutine(InboundEventContainer event, AndesMessage message, AndesChannel andesChannel) {
        String messageRoutingKey = message.getMetadata().getDestination();
        boolean isMessageRouted = false;

        //get all topic subscriptions in the cluster matching to routing key
        //including hierarchical topic case
        Set<AndesSubscription> subscriptionList;
        try {

            // Get subscription list according to the message type
            AndesSubscription.SubscriptionType subscriptionType;

            if (MessageMetaDataType.META_DATA_MQTT == message.getMetadata().getMetaDataType()) {
                subscriptionType = AndesSubscription.SubscriptionType.MQTT;
            } else {
                subscriptionType = AndesSubscription.SubscriptionType.AMQP;
            }

            subscriptionList = subscriptionStore.getClusterSubscribersForDestination(messageRoutingKey, true, subscriptionType);

            Set<String> alreadyStoredQueueNames = new HashSet<>();
            for (AndesSubscription subscription : subscriptionList) {
                if (!alreadyStoredQueueNames.contains(subscription.getStorageQueueName())) {

                    //Check protocol specific rules for validate delivery to given subscription.
                    //If there are no protocol specific delivery rules implemented, message will deliver by default.
                    if(!message.isDelivarable(subscription)){
                        continue;
                    }

                    AndesMessage clonedMessage = cloneAndesMessageMetadataAndContent(message);

                    //Message should be written to storage queue name. This is
                    //determined by destination of the message. So should be
                    //updated (but internal metadata will have topic name as usual)
                    clonedMessage.getMetadata().setStorageQueueName(subscription.getStorageQueueName());

                    if (subscription.isDurable()) {
                        /**
                         * For durable topic subscriptions we must update the routing key
                         * in metadata as well so that they become independent messages
                         * baring subscription bound queue name as the destination
                         */
                        clonedMessage.getMetadata().updateMetadata(subscription.getTargetQueue(),
                                AMQPUtils.DIRECT_EXCHANGE_NAME);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Storing metadata queue " + subscription.getStorageQueueName() + " messageID "
                                + clonedMessage.getMetadata().getMessageID() + " isTopic");
                    }

                    // add the topic wise cloned message to the events list. Message writers will pick that and
                    // write it.
                    event.addMessage(clonedMessage);
                    andesChannel.recordAdditionToBuffer(clonedMessage.getContentChunkList().size());
                    isMessageRouted = true;
                    alreadyStoredQueueNames.add(subscription.getStorageQueueName());
                }
            }

            // If retain enabled, need to store the retained message. Set the retained message
            // so the message writer will persist the retained message
            if(message.getMetadata().isRetain()) {
                event.retainMessage = message;
            }

            // If there is no matching subscriber at the moment there is no point of storing the message
            if (!isMessageRouted) {

                // Even though we drop the message pub ack needs to be sent
                event.pubAckHandler.ack(message.getMetadata());

                //Adding metrics meter for ack rate
                Meter ackMeter = MetricManager.meter(Level.INFO, MetricsConstants.ACK_SENT_RATE);
                ackMeter.mark();


                if (message.getMetadata().isRetain()) {
                    // Since there are no subscribers we don't need to store the original message.
                    // Only the retained message need to be stored
                    event.clearMessageList();
                } else {
                    // Since no matching subscribers and not a retained enabled message, Event can be
                    // cleared and set to ignore the message by message writers
                    event.clear();
                    log.info("Message routing key: " + message.getMetadata().getDestination() + " No routes in " +
                             "cluster. Ignoring Message id " + message.getMetadata().getMessageID());
                }

            }

        } catch (AndesException e) {
            log.error("Error occurred while processing routing information fot topic message. Routing Key " +
                    messageRoutingKey + ", Message ID " + message.getMetadata().getMessageID());
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
        if(MessageTracer.isEnabled()) {
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
