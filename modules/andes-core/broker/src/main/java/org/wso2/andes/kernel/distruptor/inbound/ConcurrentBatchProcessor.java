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

import com.lmax.disruptor.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is a turn based concurrent batch processor to handle InboundEvents.
 * The processor will batch event until end of current available sequence numbers is visited or max batch size
 * of interested event type is met.
 */
public class ConcurrentBatchProcessor implements EventProcessor {

    private static Log log = LogFactory.getLog(ConcurrentBatchProcessor.class);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExceptionHandler exceptionHandler = new IgnoreExceptionHandler();
    private final RingBuffer<InboundEvent> ringBuffer;
    private final SequenceBarrier sequenceBarrier;
    private final BatchEventHandler eventHandler;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final long turn;
    private final int groupCount;
    private int batchSize;
    private final InboundEvent.Type eventType;
    private final List<InboundEvent> eventList;

    /**
     * Construct a {@link EventProcessor} that will automatically track the progress by updating its sequence when
     * the {@link EventHandler#onEvent(Object, long, boolean)} method returns.
     *
     * @param ringBuffer      to which events are published.
     * @param sequenceBarrier on which it is waiting.
     * @param eventHandler    is the delegate to which events are dispatched.
     * @param turn            is the value of, sequence % groupCount this batch processor process events. Turn must be
     *                        less than groupCount
     * @param groupCount      total number of concurrent batch processors for the event type
     * @param batchSize       maximum size of the batch
     * @param eventType       type of event to batch
     */
    public ConcurrentBatchProcessor(final RingBuffer<InboundEvent> ringBuffer,
                                    final SequenceBarrier sequenceBarrier,
                                    final BatchEventHandler eventHandler,
                                    long turn,
                                    int groupCount,
                                    int batchSize,
                                    InboundEvent.Type eventType) {
        if (turn >= groupCount) {
            throw new IllegalArgumentException("Turn should be less than groupCount");
        }

        this.ringBuffer = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.eventHandler = eventHandler;
        this.turn = turn;
        this.groupCount = groupCount;
        this.batchSize = batchSize;
        this.eventType = eventType;
        eventList = new ArrayList<InboundEvent>(this.batchSize);

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

    /**
     * Set a new {@link ExceptionHandler} for handling exceptions propagated out of the {@link BatchEventProcessor}
     *
     * @param exceptionHandler to replace the existing exceptionHandler.
     */
    public void setExceptionHandler(final ExceptionHandler exceptionHandler) {
        if (null == exceptionHandler) {
            throw new NullPointerException();
        }

        this.exceptionHandler = exceptionHandler;
    }

    /**
     * It is ok to have another thread rerun this method after a halt().
     */
    @Override
    public void run() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Thread is already running");
        }
        sequenceBarrier.clearAlert();

        notifyStart();
        InboundEvent event = null;
        long currentTurn;
        long nextSequence = sequence.get() + 1L;
        while (true) {
            try {
                final long availableSequence = sequenceBarrier.waitFor(nextSequence);

                while (nextSequence <= availableSequence) {
                    event = ringBuffer.get(nextSequence);

                    // Batch only relevant event type. Skip the rest
                    if (eventType == event.getEventType()) {

                        currentTurn = nextSequence % groupCount;
                        if (turn == currentTurn) {
                            eventList.add(event);
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("[ " + nextSequence + " ] Current turn " + currentTurn + ", turn " + turn
                                    + ", groupCount " + groupCount + ", EventType " + eventType);
                        }
                    }

                    // Batch and invoke event handler. Irrespective of event type following should execute.
                    // End of batch may come in an irrelevant even type slot.
                    if (((eventList.size() >= batchSize) || (nextSequence == availableSequence))
                            && !eventList.isEmpty()) {

                        eventHandler.onEvent(eventList);
                        if (log.isDebugEnabled()) {
                            log.debug("Event handler called with " + eventList.size() + " events. EventType "
                                    + eventType);
                        }
                        eventList.clear();
                    }
                    nextSequence++;
                }

                sequence.set(nextSequence - 1L);
            } catch (final AlertException ex) {
                if (!running.get()) {
                    break;
                }
            } catch (final Throwable ex) {
                log.error("Exception occurred while processing batch for type " + eventType, ex);
                exceptionHandler.handleEventException(ex, nextSequence, event);
                sequence.set(nextSequence);

                // Dropping events with errors from batch processor. Relevant event handler should take care of
                // the events. If not cleared next iteration would contain the previous iterations event list
                eventList.clear();
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
