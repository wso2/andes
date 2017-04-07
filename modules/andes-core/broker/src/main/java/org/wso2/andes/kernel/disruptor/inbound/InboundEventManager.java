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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesAckEvent;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.DisablePubAckImpl;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.disruptor.ConcurrentBatchEventHandler;
import org.wso2.andes.kernel.disruptor.InboundEventHandler;
import org.wso2.andes.kernel.disruptor.LogExceptionHandler;
import org.wso2.andes.kernel.disruptor.compression.LZ4CompressionHelper;
import org.wso2.andes.kernel.disruptor.waitStrategy.SleepingBlockingWaitStrategy;
import org.wso2.andes.kernel.dtx.DtxBranch;
import org.wso2.andes.metrics.MetricsConstants;
import org.wso2.andes.tools.utils.MessageTracer;
import org.wso2.carbon.metrics.manager.Gauge;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.MetricManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.wso2.andes.configuration.enums.AndesConfiguration.PERFORMANCE_TUNING_ACKNOWLEDGEMENT_HANDLER_BATCH_SIZE;
import static org.wso2.andes.configuration.enums.AndesConfiguration.PERFORMANCE_TUNING_ACK_HANDLER_COUNT;
import static org.wso2.andes.configuration.enums.AndesConfiguration.PERFORMANCE_TUNING_CONTENT_CHUNK_HANDLER_COUNT;
import static org.wso2.andes.configuration.enums.AndesConfiguration.PERFORMANCE_TUNING_MAX_CONTENT_CHUNK_SIZE;
import static org.wso2.andes.configuration.enums.AndesConfiguration.PERFORMANCE_TUNING_MESSAGE_WRITER_BATCH_SIZE;
import static org.wso2.andes.configuration.enums.AndesConfiguration.PERFORMANCE_TUNING_PARALLEL_MESSAGE_WRITERS;
import static org.wso2.andes.configuration.enums.AndesConfiguration.PERFORMANCE_TUNING_PARALLEL_TRANSACTION_MESSAGE_WRITERS;
import static org.wso2.andes.configuration.enums.AndesConfiguration.PERFORMANCE_TUNING_PUBLISHING_BUFFER_SIZE;
import static org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer.Type.ACKNOWLEDGEMENT_EVENT;
import static org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer.Type.MESSAGE_EVENT;
import static org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer.Type.PUBLISHER_RECOVERY_EVENT;
import static org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer.Type.SAFE_ZONE_DECLARE_EVENT;
import static org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer.Type.STATE_CHANGE_EVENT;
import static org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer.Type.MESSAGE_RECOVERY_EVENT;
import static org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer.Type.TRANSACTION_CLOSE_EVENT;
import static org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer.Type.TRANSACTION_COMMIT_EVENT;
import static org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer.Type.TRANSACTION_ENQUEUE_EVENT;
import static org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer.Type.TRANSACTION_ROLLBACK_EVENT;

/**
 * Disruptor based inbound event handling class.
 * Inbound events are represent within the buffer as InboundEventContainer objects. Four types of event processors goes through
 * the ring buffer processing events.
 */
public class InboundEventManager {

    private static Log log = LogFactory.getLog(InboundEventManager.class);
    private final RingBuffer<InboundEventContainer> ringBuffer;
    private AtomicInteger ackedMessageCount = new AtomicInteger();
    private Disruptor<InboundEventContainer> disruptor;
    private final DisablePubAckImpl disablePubAck;
    private LZ4CompressionHelper lz4CompressionHelper;

    public InboundEventManager(MessagingEngine messagingEngine) {

        Integer bufferSize = AndesConfigurationManager.readValue(
                PERFORMANCE_TUNING_PUBLISHING_BUFFER_SIZE);
        Integer writeHandlerCount = AndesConfigurationManager.readValue(
                PERFORMANCE_TUNING_PARALLEL_MESSAGE_WRITERS);
        Integer ackHandlerCount = AndesConfigurationManager.readValue(
                PERFORMANCE_TUNING_ACK_HANDLER_COUNT);
        Integer writerBatchSize = AndesConfigurationManager.readValue(
                PERFORMANCE_TUNING_MESSAGE_WRITER_BATCH_SIZE);
        Integer ackHandlerBatchSize = AndesConfigurationManager.readValue(
                PERFORMANCE_TUNING_ACKNOWLEDGEMENT_HANDLER_BATCH_SIZE);
        Integer transactionHandlerCount = AndesConfigurationManager.readValue(
                PERFORMANCE_TUNING_PARALLEL_TRANSACTION_MESSAGE_WRITERS);

        Integer dtxDbWriterCount = 1; // need to merge local and distributed transaction writing logic to the same

        disablePubAck = new DisablePubAckImpl();
        int maxContentChunkSize = AndesConfigurationManager.readValue(
                PERFORMANCE_TUNING_MAX_CONTENT_CHUNK_SIZE);
        int contentChunkHandlerCount = AndesConfigurationManager.readValue(
                PERFORMANCE_TUNING_CONTENT_CHUNK_HANDLER_COUNT);

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("DisruptorInboundEventThread-%d").build();
        ExecutorService executorPool = Executors.newCachedThreadPool(namedThreadFactory);

        disruptor = new Disruptor<>(InboundEventContainer.getFactory(),
                bufferSize,
                executorPool,
                ProducerType.MULTI,
                new SleepingBlockingWaitStrategy());

        disruptor.handleExceptionsWith(new LogExceptionHandler());

        InboundEventHandler[] batchEventHandlers = new InboundEventHandler[
                writeHandlerCount + transactionHandlerCount + ackHandlerCount + dtxDbWriterCount];

        lz4CompressionHelper = new LZ4CompressionHelper();

        ContentChunkHandler[] chunkHandlers = new ContentChunkHandler[contentChunkHandlerCount];
        for (int i = 0; i < contentChunkHandlerCount; i++) {
            if(lz4CompressionHelper.isCompressionEnabled()) {
                chunkHandlers[i] = new ContentChunkHandler(maxContentChunkSize,
                            new LZ4ContentCompressionStrategy(lz4CompressionHelper));
            } else {
                chunkHandlers[i] = new ContentChunkHandler(maxContentChunkSize, new DisabledContentCompressionStrategy());
            }
        }

        for (int turn = 0; turn < writeHandlerCount; turn++) {
            batchEventHandlers[turn] =
                    new ConcurrentBatchEventHandler(turn, writeHandlerCount, writerBatchSize, MESSAGE_EVENT,
                                                    new MessageWriter(messagingEngine, writerBatchSize));
        }

        for (int turn = 0; turn < transactionHandlerCount; turn++) {
            batchEventHandlers[writeHandlerCount + turn] =
                    new ConcurrentBatchEventHandler(turn, transactionHandlerCount,
                            writerBatchSize,
                            TRANSACTION_COMMIT_EVENT,
                            new MessageWriter(messagingEngine, writerBatchSize));
        }

        for (int turn = 0; turn < ackHandlerCount; turn++) {
            batchEventHandlers[writeHandlerCount+ transactionHandlerCount + turn] =
                    new AckEventBatchHandler(turn, ackHandlerCount, ackHandlerBatchSize,
                                             new AckHandler(messagingEngine));
        }

        for (int turn = 0; turn < dtxDbWriterCount; turn++) {
            batchEventHandlers[writeHandlerCount+ transactionHandlerCount + ackHandlerCount + turn] =
                    new DtxDbWriter(messagingEngine, turn, dtxDbWriterCount);
        }

        MessagePreProcessor preProcessor = new MessagePreProcessor();
        StateEventHandler stateEventHandler = new StateEventHandler();

        // Order in which handlers run in Disruptor
        // - ContentChunkHandlers
        // - MessagePreProcessor
        // - AckHandlers, MessageWriters, DtxDbWriter
        // - StateEventHandler
        disruptor.handleEventsWith(chunkHandlers).then(preProcessor);
        disruptor.after(preProcessor)
                .handleEventsWith(batchEventHandlers)
                .then(stateEventHandler);   // State event handler update the state of Andes after other handlers work
                                            // is done. State event handler will execute last. This handler will clear
                                            // the event container.

        ringBuffer = disruptor.start();

        //Will add the gauge to metrics manager
        MetricManager.gauge(MetricsConstants.DISRUPTOR_INBOUND_RING, Level.INFO, new InBoundRingGauge());
        MetricManager.gauge(MetricsConstants.DISRUPTOR_MESSAGE_ACK, Level.INFO, new AckedMessageCountGauge());
    }

    /**
     * When a message is received from a transport it is handed over to MessagingEngine through the implementation of
     * inbound event manager. (e.g: through a disruptor ring buffer) Eventually the message will be stored
     *
     * @param message AndesMessage
     * @param andesChannel AndesChannel
     * @param pubAckHandler PubAckHandler
     */
    public void messageReceived(AndesMessage message, AndesChannel andesChannel, PubAckHandler pubAckHandler) {
        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        InboundEventContainer event = ringBuffer.get(sequence);
        try {
            event.setEventType(MESSAGE_EVENT);
            event.setChannel(andesChannel);
            event.addMessage(message, andesChannel);
            event.pubAckHandler = pubAckHandler;
        } finally {
            // make the event available to EventProcessors
            ringBuffer.publish(sequence);

            //Tracing message activity
            MessageTracer.trace(message, MessageTracer.PUBLISHED_TO_INBOUND_DISRUPTOR);

            if (log.isDebugEnabled()) {
                log.debug("[ sequence: " + sequence + " ] Message published to disruptor. Message id: "
                          + message.getMetadata().getMessageID());
            }
        }
    }

    /**
     * Acknowledgement received from clients for sent messages will be handled through this method
     *
     * @param ackData Acknowledgement information by protocol
     */
    public void ackReceived(AndesAckData ackData) {
        //For metrics
        ackedMessageCount.getAndIncrement();

        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        InboundEventContainer event = ringBuffer.get(sequence);
        try {
            event.setEventType(ACKNOWLEDGEMENT_EVENT);
            event.ackData = new AndesAckEvent(ackData);
        } finally {
            // make the event available to EventProcessors
            ringBuffer.publish(sequence);

            //Tracing message
            if (MessageTracer.isEnabled()) {
                MessageTracer.traceAck(ackData, MessageTracer.ACK_PUBLISHED_TO_DISRUPTOR );
            }

            if (log.isDebugEnabled()) {
                log.debug("[ sequence: " + sequence + " ] Message acknowledgement published to disruptor. "
                        + "Message id " + ackData.getMessageId());
            }
        }
    }

    /**
     * Publish state change event to event Manager
     * @param stateEvent AndesInboundStateEvent
     */
    public void publishStateEvent(AndesInboundStateEvent stateEvent) {

        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        InboundEventContainer event = ringBuffer.get(sequence);
        try {
            event.setEventType(STATE_CHANGE_EVENT);
            event.setStateEvent(stateEvent);
        } finally {
            // make the event available to EventProcessors
            ringBuffer.publish(sequence);
            if (log.isDebugEnabled()) {
                log.debug("[ Sequence: " + sequence + " ] State change event '" + stateEvent.eventInfo() +
                        "' published to Disruptor");
            }
        }

    }

    /**
     * Publish an event to update safe zone message ID
     * as per this node (this is used when deleting slots)
     */
    public void updateSlotDeletionSafeZone() {
        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        InboundEventContainer event = ringBuffer.get(sequence);
        try {
            event.setEventType(SAFE_ZONE_DECLARE_EVENT);
        } finally {
            // make the event available to EventProcessors
            ringBuffer.publish(sequence);
            if (log.isDebugEnabled()) {
                log.debug("[ Sequence: " + sequence + " ] " + event.getEventType() + "' published to Disruptor");
            }
        }
    }

    /**
     * Publish an event to recover from a member left without a submit slot event for messages written to DB
     */
    public void publishRecoveryEvent() {
        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        InboundEventContainer event = ringBuffer.get(sequence);
        try {
            event.setEventType(PUBLISHER_RECOVERY_EVENT);
        } finally {
            // make the event available to EventProcessors
            ringBuffer.publish(sequence);
            if (log.isDebugEnabled()) {
                log.debug("[ Sequence: " + sequence + " ] " + event.getEventType() + "' published to Disruptor");
            }
        }
    }

    /**
     * Publish an event to recover messages that were delivered to a given channel.
     *
     * @param recoveryEvent event containing required information regarding the event
     */
    public void publishMessageRecoveryEvent(InboundMessageRecoveryEvent recoveryEvent) {
        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        InboundEventContainer event = ringBuffer.get(sequence);
        try {
            event.setEventType(MESSAGE_RECOVERY_EVENT);
            event.setMessageRecoveryEvent(recoveryEvent);
        } finally {
            // make the event available to EventProcessors
            ringBuffer.publish(sequence);
            if (log.isDebugEnabled()) {
                log.debug("[ Sequence: " + sequence + " ] " + event.getEventType() + "' published to Disruptor");
            }
        }
    }

    /**
     * Publish transaction enqueue event to Disruptor.
     * This will go through the {@link org.wso2.andes.kernel.disruptor.inbound.ContentChunkHandler} and
     * add the re-sized message to the {@link org.wso2.andes.kernel.disruptor.inbound.InboundTransactionEvent}
     *
     * @param message enqueued {@link org.wso2.andes.kernel.AndesMessage}
     * @param transactionEvent {@link org.wso2.andes.kernel.disruptor.inbound.InboundTransactionEvent}
     * @param channel {@link org.wso2.andes.kernel.AndesChannel} of the publisher
     */
    public void requestTransactionEnqueueEvent(AndesMessage message,
                                               InboundTransactionEvent transactionEvent, AndesChannel channel) {
        long sequence = ringBuffer.next();
        InboundEventContainer eventContainer = ringBuffer.get(sequence);

        try {
            eventContainer.setEventType(TRANSACTION_ENQUEUE_EVENT);
            eventContainer.setTransactionEvent(transactionEvent);
            eventContainer.setChannel(channel);
            eventContainer.addMessage(message, channel);
            eventContainer.pubAckHandler = disablePubAck;
        } finally {
            ringBuffer.publish(sequence);

            //Tracing message activity
            MessageTracer.traceTransaction(message, channel, MessageTracer
                    .ENQUEUE_EVENT_PUBLISHED_TO_INBOUND_DISRUPTOR);

            if (log.isDebugEnabled()) {
                log.debug("[ Sequence: " + sequence + " ] " + eventContainer.getEventType() +
                          "' published to Disruptor");
            }
        }
    }

    /**
     * Publish transaction commit event to Disruptor for processing
     *
     * @param transactionEvent {@link org.wso2.andes.kernel.disruptor.inbound.InboundTransactionEvent}
     * @param channel {@link org.wso2.andes.kernel.AndesChannel}
     */
    public void requestTransactionCommitEvent(InboundTransactionEvent transactionEvent, AndesChannel channel) {
        requestTransactionEvent(transactionEvent, TRANSACTION_COMMIT_EVENT, channel);

        //Tracing message activity
        MessageTracer.traceTransaction(channel, transactionEvent.getQueuedMessages().size(), MessageTracer
                .TRANSACTION_COMMIT_EVENT_PUBLISHED_TO_INBOUND_DISRUPTOR);
    }

    /**
     * Publish rollback event to Disruptor for processing.
     * @param transactionEvent {@link org.wso2.andes.kernel.disruptor.inbound.InboundTransactionEvent}
     * @param channel {@link org.wso2.andes.kernel.AndesChannel}
     */
    public void requestTransactionRollbackEvent(InboundTransactionEvent transactionEvent, AndesChannel channel) {
        requestTransactionEvent(transactionEvent, TRANSACTION_ROLLBACK_EVENT, channel);

        //Tracing message activity
        MessageTracer.traceTransaction(channel, transactionEvent.getQueuedMessages().size(), MessageTracer
                .TRANSACTION_ROLLBACK_EVENT_PUBLISHED_TO_INBOUND_DISRUPTOR);
    }

    /**
     * Publish transaction close event to Disruptor for processing
     * @param transactionEvent {@link org.wso2.andes.kernel.disruptor.inbound.InboundTransactionEvent}
     * @param channel {@link org.wso2.andes.kernel.AndesChannel}
     */
    public void requestTransactionCloseEvent(InboundTransactionEvent transactionEvent, AndesChannel channel) {
        requestTransactionEvent(transactionEvent, TRANSACTION_CLOSE_EVENT, channel);

        //Tracing message activity
        MessageTracer.traceTransaction(channel, transactionEvent.getQueuedMessages().size(), MessageTracer
                .TRANSACTION_CLOSE_EVENT_PUBLISHED_TO_INBOUND_DISRUPTOR);
    }

    /**
     * Different transaction related events are published to Disruptor using this method
     * @param transactionEvent {@link InboundTransactionEvent}
     * @param eventType {@link org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer.Type}
     * @param channel {@link org.wso2.andes.kernel.AndesChannel}
     */
    private void requestTransactionEvent(InboundTransactionEvent transactionEvent,
                                         InboundEventContainer.Type eventType, AndesChannel channel) {
        long sequence = ringBuffer.next();
        InboundEventContainer eventContainer = ringBuffer.get(sequence);

        try {
            eventContainer.setEventType(eventType);
            eventContainer.setTransactionEvent(transactionEvent);
            eventContainer.pubAckHandler = disablePubAck;
            eventContainer.setChannel(channel);
        } finally {
            ringBuffer.publish(sequence);
            if (log.isDebugEnabled()) {
                log.debug("[ Sequence: " + sequence + " ] " + eventContainer.getEventType() +
                        "' published to Disruptor");
            }
        }
    }

    /**
     * Stop disruptor. This wait until disruptor process pending events in ring buffer.
     */
    public void stop() {
        disruptor.shutdown();
    }

    /**
     * Publish the distributed transaction event to Disruptor
     *
     * @param dtxBranch {@link DtxBranch} related to the commit request
     * @param channel {@link AndesChannel} related to the {@link DtxBranch} commit request
     * @param type Dtx event type of {@link org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer.Type}
     */
    public void requestDtxEvent(DtxBranch dtxBranch, AndesChannel channel, InboundEventContainer.Type type) {
        long sequence = ringBuffer.next();
        InboundEventContainer eventContainer = ringBuffer.get(sequence);
        try {
            eventContainer.setEventType(type);
            eventContainer.setDtxBranch(dtxBranch);
            eventContainer.pubAckHandler = disablePubAck;
            eventContainer.setChannel(channel);
        } finally {
            ringBuffer.publish(sequence);
            if (log.isDebugEnabled()) {
                log.debug("[ Sequence: " + sequence + " ] " + eventContainer.getEventType() +
                                  "' published to Disruptor");
            }
        }
    }

    /**
     * Utility to get the in bound ring gauge
     */
    private class InBoundRingGauge implements Gauge<Long> {

        @Override
        public Long getValue() {
            //The total message size will be reduced by the remaining capacity to get the total ring size
            return ringBuffer.getBufferSize() - ringBuffer.remainingCapacity();
        }
    }

    /**
     * Utility to get the acked message count
     */
    private class AckedMessageCountGauge implements Gauge<Integer> {

        @Override
        public Integer getValue() {
            //Acknowledged message count at a given time
            return ackedMessageCount.getAndSet(0);
        }
    }

}
