/*
 * Copyright (c) 2015-2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.disruptor.delivery;

import com.gs.collections.impl.set.mutable.primitive.LongHashSet;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SequenceReportingEventHandler;
import com.lmax.disruptor.Sequencer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.ProtocolMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Concurrently read content read tasks and batch the event and forward the batched events to event handler
 * for batched content read task
 */
public class ConcurrentContentReadTaskBatchProcessor implements EventProcessor {

    private static Log log = LogFactory.getLog(ConcurrentContentReadTaskBatchProcessor.class);

    private final AtomicBoolean running;
    private ExceptionHandler exceptionHandler;
    private final RingBuffer<DeliveryEventData> ringBuffer;
    private final SequenceBarrier sequenceBarrier;
    private final ContentCacheCreator eventHandler;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final long turn;
    private final int groupCount;
    private int batchSize;

    /**
     * Construct a {@link EventProcessor} that will automatically track the progress by updating its sequence when
     * the {@link com.lmax.disruptor.EventHandler#onEvent(Object, long, boolean)} method returns.
     *
     * @param ringBuffer      to which events are published.
     * @param sequenceBarrier on which it is waiting.
     * @param eventHandler    is the delegate to which events are dispatched.
     * @param turn            is the value of, sequence % groupCount this batch processor process events. Turn must be
     *                        less than groupCount
     * @param groupCount      total number of concurrent batch processors for the event type
     * @param batchSize       size limit of total content size to batch. This is a loose limit
     */
    public ConcurrentContentReadTaskBatchProcessor(final RingBuffer<DeliveryEventData> ringBuffer,
                                                   final SequenceBarrier sequenceBarrier, final ContentCacheCreator eventHandler, long turn, int groupCount,
                                                   int batchSize) {
        if (turn >= groupCount) {
            throw new IllegalArgumentException("Turn should be less than groupCount");
        }

        this.ringBuffer = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.eventHandler = eventHandler;
        this.turn = turn;
        this.groupCount = groupCount;
        this.batchSize = batchSize;

        exceptionHandler = new DeliveryExceptionHandler();
        running = new AtomicBoolean(false);
        if (eventHandler instanceof SequenceReportingEventHandler) {
            ((SequenceReportingEventHandler<?>) eventHandler).setSequenceCallback(sequence);
        }
    }

    @Override
    public Sequence getSequence() {
        return sequence;
    }

    @Override
    public void halt() {
        running.set(false);
        sequenceBarrier.alert();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Set a new {@link ExceptionHandler} for handling exceptions propagated out of the {@link com.lmax.disruptor.BatchEventProcessor}
     *
     * @param exceptionHandler to replace the existing exceptionHandler.
     */
    public void setExceptionHandler(final ExceptionHandler exceptionHandler) {
        if (null == exceptionHandler) {
            throw new NullPointerException("Exception handler cannot be null.");
        }

        this.exceptionHandler = exceptionHandler;
    }

    /**
     * DeliveryEventDataList is a data structure that contain DeliveryEventData and relevant contain contentLength.
     */
    private class DeliveryEventDataList {

        private int contentLength;
        private ArrayList<DeliveryEventData> deliveryEventArrayList;

        DeliveryEventDataList() {
            this.contentLength = 0;
            this.deliveryEventArrayList = new ArrayList<>();
        }

        ArrayList<DeliveryEventData> getDeliveryEventArrayList() {
            return this.deliveryEventArrayList;
        }

        public int getContentLength() {
            return this.contentLength;
        }

        /**
         * Add given deliveryEventData to an array list.
         * After adding to the array list content length is automatically added per array list
         *
         * @param deliveryEventData deliveryEventData in the messageMetadataList
         */
        private void addDeliveryEventDataToList(DeliveryEventData deliveryEventData) {
            deliveryEventArrayList.add(deliveryEventData);
            contentLength = contentLength + deliveryEventData.getMetadata().getMessage().getMessageContentLength();
        }

        /**
         * Reset values as it begins.
         */
        private void clearDeliveryEventDataList() {
            deliveryEventArrayList = new ArrayList<>();
            contentLength = 0;
        }
    }

    /**
     * It is ok to have another thread rerun this method after a halt().
     * In here we made DeliveryEventDataList per queue. When  the conditions are become true,
     * It passes storageQueueName and messageMetadataList to ContentCacheCreator.
     */
    @Override
    public void run() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Thread is already running");
        }
        sequenceBarrier.clearAlert();

        notifyStart();
        DeliveryEventData event = null;
        // Hashmap that contain queueName as the key
        // and relevant delivery event data list as the value
        HashMap<String, DeliveryEventDataList> messageMap = new HashMap<>();
        LongHashSet messageIdSet = new LongHashSet();
        long currentTurn;
        long nextSequence = sequence.get() + 1L;
        DeliveryEventDataList messageMetadataList;

        String storageQueueName;

        while (true) {
            try {
                final long availableSequence = sequenceBarrier.waitFor(nextSequence);

                while (nextSequence <= availableSequence) {
                    event = ringBuffer.get(nextSequence);

                    ProtocolMessage metadata = event.getMetadata();
                    long currentMessageId = metadata.getMessageID();

                    //Current Queue name for the usage of multiple tables.
                    storageQueueName = metadata.getMessage().getStorageQueueName();
                    currentTurn = currentMessageId % groupCount;

                    if (turn == currentTurn) {
                        messageIdSet.add(currentMessageId);
                        messageMetadataList = messageMap.get(storageQueueName);

                        if (messageMetadataList == null) {
                            // (messageMetadataList == null) means hash map doesn't contain
                            // the key as currently processing queueName,
                            // so first create new messageMetadataList 
                            // Put storageQueueName and messageMetadataList to messageMap hash.
                            messageMetadataList = new DeliveryEventDataList();
                            messageMap.put(storageQueueName, messageMetadataList);
                        }
                        messageMetadataList.addDeliveryEventDataToList(event);

                        // Batch and invoke deliveryEventArrayList handler.
                        if (messageMetadataList.getContentLength() >= batchSize) {
                            eventHandler.onEvent(storageQueueName, messageMetadataList.getDeliveryEventArrayList());
                            // reset counters and lists
                            messageMetadataList.clearDeliveryEventDataList();
                            messageIdSet.clear();

                            if (log.isDebugEnabled()) {
                                log.debug("Event handler called with message id list " + messageIdSet);
                            }
                        }
                    }

                    if ((nextSequence == availableSequence)) {
                        for (Map.Entry<String, DeliveryEventDataList> entry : messageMap.entrySet()) {
                            if (!(entry.getValue().getDeliveryEventArrayList().isEmpty())) {
                                eventHandler.onEvent(entry.getKey(), entry.getValue().getDeliveryEventArrayList());
                                // reset counters and lists
                                entry.getValue().clearDeliveryEventDataList();
                                messageIdSet.clear();
                            }
                        }
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("[ " + nextSequence + " ] Current turn " + currentTurn + ", turn " + turn
                                + ", groupCount " + groupCount);
                    }
                    nextSequence++;
                }
                sequence.set(nextSequence - 1L);
            } catch (final AlertException ex) {
                if (!running.get()) {
                    break;
                }
            } catch (final Throwable ex) {
                log.error("Exception occurred while processing batched content reads. ", ex);
                exceptionHandler.handleEventException(ex, nextSequence, event);
                sequence.set(nextSequence);

                // Dropping events with errors from batch processor.
                // Relevant deliveryEventArrayList handler should take care of the events.
                // If not cleared next iteration would contain the previous iterations deliveryEventArrayList list.
                messageMap.clear();
                messageIdSet.clear();
                nextSequence++;
            }
        }

        notifyShutdown();

        running.set(false);
    }

    private void notifyStart() {
        if (eventHandler instanceof LifecycleAware) {
            try {
                ((LifecycleAware) eventHandler).onStart();
            } catch (final Throwable ex) {
                exceptionHandler.handleOnStartException(ex);
            }
        }
    }

    private void notifyShutdown() {
        if (eventHandler instanceof LifecycleAware) {
            try {
                ((LifecycleAware) eventHandler).onShutdown();
            } catch (final Throwable ex) {
                exceptionHandler.handleOnShutdownException(ex);
            }
        }
    }

}
