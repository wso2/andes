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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.kernel.*;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.wso2.andes.configuration.enums.AndesConfiguration.*;
import static org.wso2.andes.kernel.distruptor.inbound.InboundEvent.Type.*;

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
                PERFORMANCE_TUNING_PUBLISHING_BUFFER_SIZE);
        Integer writeHandlerCount = AndesConfigurationManager.readValue(
                PERFORMANCE_TUNING_PARALLEL_MESSAGE_WRITERS);
        Integer ackHandlerCount = AndesConfigurationManager.readValue(
                PERFORMANCE_TUNING_ACK_HANDLER_COUNT);
        Integer writerBatchSize = AndesConfigurationManager.readValue(
                PERFORMANCE_TUNING_MESSAGE_WRITER_BATCH_SIZE);
        Integer ackHandlerBatchSize = AndesConfigurationManager.readValue(
                PERFORMANCE_TUNING_ACKNOWLEDGEMENT_HANDLER_BATCH_SIZE);

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                                    .setNameFormat("Disruptor Inbound Event Thread %d").build();
        ExecutorService executorPool = Executors.newCachedThreadPool(namedThreadFactory);

        Disruptor<InboundEvent> disruptor;
        disruptor = new Disruptor<InboundEvent>(InboundEvent.getFactory(), executorPool,
                new MultiThreadedClaimStrategy(bufferSize),
                new BlockingWaitStrategy());

        disruptor.handleExceptionsWith(new InboundLogExceptionHandler());

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
                    MESSAGE_EVENT
            );
        }

        for (int turn = 0; turn < ackHandlerCount; turn++) {
            processors[writeHandlerCount + turn] = new ConcurrentBatchProcessor(
                    disruptor.getRingBuffer(),
                    barrier,
                    new AckHandler(messagingEngine),
                    turn,
                    ackHandlerCount,
                    ackHandlerBatchSize,
                    ACKNOWLEDGEMENT_EVENT
            );
        }

        disruptor.handleEventsWith(processors);

        // State event handler should run at last.
        // State event handler update the state of Andes after other handlers work is done.
        disruptor.after(processors).handleEventsWith(new StateEventHandler(messagingEngine));

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

        event.setEventType(MESSAGE_EVENT);
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

        event.setEventType(ACKNOWLEDGEMENT_EVENT);
        event.ackData = ackData;
        // make the event available to EventProcessors
        ringBuffer.publish(sequence);

        if (log.isDebugEnabled()) {
            log.debug("[ sequence: " + sequence + " ] Message acknowledgement published to disruptor. Message id " +
                    ackData.getMessageID());
        }
    }

    @Override
    public void moveMessageToDeadLetterChannel(long messageId, String destinationQueueName) throws AndesException {

    }

    @Override
    public void clearMessagesFromQueueInMemory(String storageQueueName, Long purgedTimestamp) throws AndesException {
    }

    @Override
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws AndesException {

    }

    @Override
    public void publishStateEvent(AndesInboundStateEvent stateEvent) {

        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        InboundEvent event = ringBuffer.get(sequence);
        try {
            event.setEventType(STATE_CHANGE_EVENT);
            event.setStateEvent(stateEvent);
        } finally {
            // make the event available to EventProcessors
            ringBuffer.publish(sequence);
            if (log.isDebugEnabled()) {
                log.debug("[ Sequence: " + sequence + " ] State change event '" + stateEvent.getEventType() +
                        "' published to Disruptor");
            }
        }

    }

}
