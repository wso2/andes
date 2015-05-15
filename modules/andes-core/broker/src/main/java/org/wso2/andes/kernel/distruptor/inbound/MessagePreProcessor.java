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

package org.wso2.andes.kernel.distruptor.inbound;

import com.lmax.disruptor.EventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.store.MessageMetaDataType;
import org.wso2.andes.subscription.SubscriptionStore;

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
                updateRoutingInformation(inboundEvent, sequence);
                break;
            case SAFE_ZONE_DECLARE_EVENT:
                setSafeZoneLimit(inboundEvent, sequence);
                break;
        }
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
     */
    private void updateRoutingInformation(InboundEventContainer event, long sequence) {

        // NOTE: This is the MESSAGE_EVENT and this is the first processing event for this message published to ring
        // Therefore there should be exactly one message in the list.
        // NO NEED TO CHECK FOR LIST SIZE
        AndesMessage message = event.messageList.get(0);

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
        }
    }

    private void handleTopicRoutine(InboundEventContainer event, AndesMessage message, AndesChannel andesChannel) {
        String messageRoutingKey = message.getMetadata().getDestination();
        boolean isMessageRouted = false;

        if (message.getMetadata().isRetain()) {
            event.retainMessage = message;
            isMessageRouted = true;
        }
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

            // current message is removed from list and updated cloned messages added later
            event.messageList.clear();
            Set<String> alreadyStoredQueueNames = new HashSet<String>();
            for (AndesSubscription subscription : subscriptionList) {
                if (!alreadyStoredQueueNames.contains(subscription.getStorageQueueName())) {

                    // Avoid adding QOS 0 MQTT messages to clean session = false subscribers if disconnected
                    if (AndesSubscription.SubscriptionType.MQTT == subscriptionType
                            && subscription.isDurable()
                            && !subscription.hasExternalSubscriptions()
                            && 0 == message.getMetadata().getQosLevel()) {
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
                    event.messageList.add(clonedMessage);
                    andesChannel.recordAdditionToBuffer(clonedMessage.getContentChunkList().size());
                    isMessageRouted = true;
                    alreadyStoredQueueNames.add(subscription.getStorageQueueName());
                }
            }

            // If there is no matching subscriber at the moment there is no point of storing the message
            if (!isMessageRouted) {
                log.info("Message routing key: " + message.getMetadata().getDestination() + " No routes in " +
                        "cluster. Ignoring Message id " + message.getMetadata().getMessageID());

                // Only one message in list. Clear it and set to ignore the message by message writers
                event.setEventType(InboundEventContainer.Type.IGNORE_EVENT);
                event.messageList.clear();
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
         * [1 sign bit][45bits for time spent from reference time in milliseconds][8bit node id][10 bit offset for ID falls within the same timestamp]
         * This assumes there will not be more than 1024 hits within a given millisecond. Range is sufficient for 6029925857 years.
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
