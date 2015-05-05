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
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.distruptor.ConcurrentBatchEventHandler;
import org.wso2.andes.kernel.distruptor.LogExceptionHandler;
import org.wso2.andes.matrics.DataAccessMatrixManager;
import org.wso2.andes.matrics.MatrixConstants;
import org.wso2.andes.subscription.SubscriptionStore;
import org.wso2.carbon.metrics.manager.Gauge;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.wso2.andes.configuration.enums.AndesConfiguration.*;
import static org.wso2.andes.kernel.distruptor.inbound.InboundEventContainer.Type.*;

/**
 * Disruptor based inbound event handling class.
 * Inbound events are represent within the buffer as InboundEventContainer objects. Four types of event processors goes through
 * the ring buffer processing events.
 */
public class DisruptorBasedInboundEventManager implements InboundEventManager {

    private static Log log = LogFactory.getLog(DisruptorBasedInboundEventManager.class);
    private final RingBuffer<InboundEventContainer> ringBuffer;
    private AtomicInteger ackedMessageCount = new AtomicInteger();
    private Disruptor<InboundEventContainer> disruptor;

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


        disruptor = new Disruptor<InboundEventContainer>(InboundEventContainer.getFactory(),
                bufferSize, 
                executorPool,
                ProducerType.MULTI,
                new BlockingWaitStrategy());

        disruptor.handleExceptionsWith(new LogExceptionHandler());

        ConcurrentBatchEventHandler[] concurrentBatchEventHandlers =
                new ConcurrentBatchEventHandler[writeHandlerCount + ackHandlerCount];

        for (int turn = 0; turn < writeHandlerCount; turn++) {
            concurrentBatchEventHandlers[turn] = new ConcurrentBatchEventHandler(turn, writeHandlerCount,
                    writerBatchSize,
                    MESSAGE_EVENT,
                    new MessageWriter(messagingEngine, writerBatchSize));
        }

        for (int turn = 0; turn < ackHandlerCount; turn++) {
            concurrentBatchEventHandlers[writeHandlerCount + turn] = new ConcurrentBatchEventHandler(turn, ackHandlerCount,
                    ackHandlerBatchSize,
                    ACKNOWLEDGEMENT_EVENT,
                    new AckHandler(messagingEngine));
        }

        // Pre processor runs first then Write handlers and ack handlers run in parallel. State event handler comes
        // after them
        disruptor.handleEventsWith(new MessagePreProcessor(subscriptionStore)).then(concurrentBatchEventHandlers);

        // State event handler should run at last.
        // State event handler update the state of Andes after other handlers work is done.
        disruptor.after(concurrentBatchEventHandlers).handleEventsWith(new StateEventHandler(messagingEngine));

        ringBuffer = disruptor.start();

        //Will start the gauge
        DataAccessMatrixManager.addGuage(MatrixConstants.DISRUPTOR_INBOUND_RING,this.getClass(),
                new InBoundRingGauge());
        DataAccessMatrixManager.addGuage(MatrixConstants.DISRUPTOR_MESSAGE_ACK,this.getClass(),
                new AckedMessageCountGauge());
    }

    /**
     * @inheritDoc
     */
    @Override
    public void messageReceived(AndesMessage message, AndesChannel andesChannel, PubAckHandler pubAckHandler) {
        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        InboundEventContainer event = ringBuffer.get(sequence);

        event.setEventType(MESSAGE_EVENT);
        event.messageList.add(message);
        event.pubAckHandler = pubAckHandler;
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
        //For matrics
        ackedMessageCount.getAndIncrement();
        
        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        InboundEventContainer event = ringBuffer.get(sequence);

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
     * {@inheritDoc}
     */
    @Override
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
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        disruptor.shutdown();
    }

    /**
     * Utility to get the in bound ring guage
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
            //Acked message count at a given time
            return ackedMessageCount.getAndSet(0);
        }
    }

}
