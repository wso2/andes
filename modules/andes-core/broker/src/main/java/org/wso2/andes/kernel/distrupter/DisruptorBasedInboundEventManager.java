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

package org.wso2.andes.kernel.distrupter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.IgnoreExceptionHandler;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.InboundEventManager;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Disruptor based inbound event handling class.
 * Inbound events are represent within the buffer as InboundEvent objects. Four types of event processors goes through
 * the ring buffer processing events.
 */
public class DisruptorBasedInboundEventManager implements InboundEventManager {

    private static Log log = LogFactory.getLog(DisruptorBasedInboundEventManager.class);
    private final RingBuffer<InboundEvent> ringBuffer;

    public DisruptorBasedInboundEventManager(SubscriptionStore subscriptionStore,
                                             MessagingEngine messagingEngine) {

        Integer bufferSize = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_PUBLISHING_BUFFER_SIZE);
        Integer writeHandlerCount = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_PARALLEL_MESSAGE_WRITERS);
        Integer ackHandlerCount = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_ACK_HANDLER_COUNT);
        Integer writerBatchSize = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_MESSAGE_WRITER_BATCH_SIZE);
        Integer ackHandlerBatchSize = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_ACKNOWLEDGEMENT_HANDLER_BATCH_SIZE);

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                                    .setNameFormat("Disruptor Inbound Event Thread %d").build();
        ExecutorService executorPool = Executors.newCachedThreadPool(namedThreadFactory);

        Disruptor<InboundEvent> disruptor;
        disruptor = new Disruptor<InboundEvent>(InboundEvent.getFactory(), executorPool,
                new MultiThreadedClaimStrategy(bufferSize),
                new BlockingWaitStrategy());

        // Pre processor runs first then Write handlers and ack handlers run in parallel. State event handler comes
        // after them
        SequenceBarrier barrier = disruptor.handleEventsWith(new MessagePreProcessor(subscriptionStore))
                .asSequenceBarrier();

        ConcurrentBatchProcessor[] processors = new ConcurrentBatchProcessor[writeHandlerCount + ackHandlerCount];

        for (int turn = 0; turn < writeHandlerCount; turn++) {
            processors[turn] = new ConcurrentBatchProcessor(
                    disruptor.getRingBuffer(),
                    barrier,
                    new MessageWriter(messagingEngine, writerBatchSize),
                    turn,
                    writeHandlerCount,
                    writerBatchSize,
                    InboundEvent.Type.MESSAGE_EVENT
            );
        }

        for (int turn = 0; turn < ackHandlerCount; turn++) {
            processors[writeHandlerCount + turn] = new ConcurrentBatchProcessor(
                    disruptor.getRingBuffer(),
                    barrier,
                    new AckHandler(),
                    turn,
                    ackHandlerCount,
                    ackHandlerBatchSize,
                    InboundEvent.Type.ACKNOWLEDGEMENT_EVENT
            );
        }

        disruptor.handleEventsWith(processors);

        // State event handler should run at last.
        // State event handler update the state of Andes after other handlers work is done.
        disruptor.after(processors).handleEventsWith(new StateEventHandler(messagingEngine));

        disruptor.handleExceptionsWith(new IgnoreExceptionHandler());
        ringBuffer = disruptor.start();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void messageReceived(AndesMessage message, AndesChannel andesChannel) {
        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        InboundEvent event = ringBuffer.get(sequence);

        event.setEventType(InboundEvent.Type.MESSAGE_EVENT);
        event.messageList.add(message);
        event.setChannel(andesChannel);
        // make the event available to EventProcessors
        ringBuffer.publish(sequence);

        if (log.isDebugEnabled()) {
            log.debug("[ sequence: " + sequence + " ] Message published to disruptor.");
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void ackReceived(AndesAckData ackData) {
        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        InboundEvent event = ringBuffer.get(sequence);

        event.setEventType(InboundEvent.Type.ACKNOWLEDGEMENT_EVENT);
        event.setAckData(ackData);
        if(null == event.getAckData()) {
            log.info("Setting null and publish");
        }
        // make the event available to EventProcessors
        ringBuffer.publish(sequence);

        if (log.isDebugEnabled()) {
            log.debug("[ sequence: " + sequence + " ] Message acknowledgement published to disruptor. Message id " +
                    ackData.getMessageID());
        }
    }

    @Override
    public void messageRejected(AndesMessageMetadata metadata) {

    }

    @Override
    public void reQueueMessage(AndesMessageMetadata messageMetadata, LocalSubscription subscription) throws AndesException {

    }

    @Override
    public void moveMessageToDeadLetterChannel(long messageId, String destinationQueueName) throws AndesException {

    }

    @Override
    public void clearMessagesFromQueueInMemory(String storageQueueName, Long purgedTimestamp) throws AndesException {
    }

    @Override
    public void purgeQueue(String destinationQueue, String ownerName, boolean isTopic) throws AndesException {
    }

    @Override
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDeadLetterChannel) throws AndesException {

    }

    @Override
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws AndesException {

    }

    /**
     * @inheritDoc
     */
    @Override
    public void startMessageDelivery() throws Exception {
        publishToRingBuffer(InboundEvent.Type.START_MESSAGE_DELIVERY_EVENT, null,
                "Start message delivery event published to disruptor.");
    }

    /**
     * @inheritDoc
     */
    @Override
    public void stopMessageDelivery() {
        publishToRingBuffer(InboundEvent.Type.STOP_MESSAGE_DELIVERY_EVENT, null,
                "Stop message delivery event published to disruptor");
    }

    /**
     * @inheritDoc
     */
    @Override
    public void shutDown() {
        publishToRingBuffer(InboundEvent.Type.SHUTDOWN_MESSAGING_ENGINE_EVENT, null,
                "Shutdown messaging engine event published to disruptor.");
    }

    /**
     * @inheritDoc
     */
    @Override
    public void startMessageExpirationWorker() {
        publishToRingBuffer(InboundEvent.Type.START_EXPIRATION_WORKER_EVENT, null,
                "Start message expiration worker event published to disruptor.");

    }

    /**
     * @inheritDoc
     */
    @Override
    public void stopMessageExpirationWorker() {
         publishToRingBuffer(InboundEvent.Type.STOP_EXPIRATION_WORKER_EVENT, null,
                 "Shutdown message expiration worker event published to disruptor.");

    }

    /**
     * @inheritDoc
     */
    @Override
    public void clientConnectionClosed(long channelID) {
        publishToRingBuffer(InboundEvent.Type.CHANNEL_CLOSE_EVENT, channelID, "Channel close event published to disruptor.");
    }

    /**
     * @inheritDoc
     */
    @Override
    public void clientConnectionCreated(long channelID) {
        publishToRingBuffer(InboundEvent.Type.CHANNEL_OPEN_EVENT, channelID, "Channel open event published to disruptor.");
    }

    /**
     * @inheritDoc
     */
    @Override
    public void closeLocalSubscription(LocalSubscription localSubscription) {
        publishToRingBuffer(InboundEvent.Type.CLOSE_SUBSCRIPTION_EVENT, localSubscription,
                "Close subscription event published to disruptor.");
    }

    /**
     * @inheritDoc
     */
    @Override
    public void openLocalSubscription(LocalSubscription localSubscription) {
        publishToRingBuffer(InboundEvent.Type.OPEN_SUBSCRIPTION_EVENT, localSubscription,
                "Open new subscription event published to disruptor.");
    }

    /**
     * Publish the event to ring buffer
     * @param eventType Event type (e.g: MESSAGE_EVENT, ACKNOWLEDGEMENT_EVENT
     * @param data data related to the event
     * @param eventDescription brief description of event
     */
    private void publishToRingBuffer(InboundEvent.Type eventType, Object data, String eventDescription) {
        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        InboundEvent event = ringBuffer.get(sequence);

        event.setEventType(eventType);
        event.setData(data);
        // make the event available to EventProcessors
        ringBuffer.publish(sequence);

        if (log.isDebugEnabled()) {
            log.debug("[ sequence: " + sequence + " ] " + eventDescription);
        }
    }
}
