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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.disruptor.delivery;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.BrokerConfigurationService;
import org.wso2.andes.kernel.ProtocolMessage;
import org.wso2.andes.kernel.disruptor.waitStrategy.SleepingBlockingWaitStrategy;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Disruptor based message flusher. This use a ring buffer to deliver message to subscribers
 */
public class DisruptorBasedFlusher {

    private static Log log = LogFactory.getLog(DisruptorBasedFlusher.class);

    /**
     * Maximum time waited to shut down the outbound disruptor. This is specified in seconds.
     */
    public static final int OUTBOUND_DISRUPTOR_SHUTDOWN_WAIT_TIME = 30;

    /**
     * Disruptor instance used in the flusher
     */
    private final Disruptor<DeliveryEventData> disruptor;

    /**
     * Ring buffer used for delivery
     */
    private final RingBuffer<DeliveryEventData> ringBuffer;

    public DisruptorBasedFlusher() {
        Integer ringBufferSize = BrokerConfigurationService.getInstance().getBrokerConfiguration()
                .getPerformanceTuning().getDelivery().getRingBufferSize();
        Integer parallelContentReaders = BrokerConfigurationService.getInstance().getBrokerConfiguration()
                .getPerformanceTuning().getDelivery().getParallelContentReaders();
        Integer parallelDecompressionHandlers = BrokerConfigurationService.getInstance().getBrokerConfiguration()
                .getPerformanceTuning().getDelivery().getParallelDecompressionHandlers();
        Integer parallelDeliveryHandlers = BrokerConfigurationService.getInstance().getBrokerConfiguration()
                .getPerformanceTuning().getDelivery().getParallelDeliveryHandlers();
        Integer contentSizeToBatch = BrokerConfigurationService.getInstance().getBrokerConfiguration()
                .getPerformanceTuning().getDelivery().getContentReadBatchSize();
        int maxContentChunkSize = BrokerConfigurationService.getInstance().getBrokerConfiguration()
                .getPerformanceTuning().getContentHandling().getMaxContentChunkSize();

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("DisruptorBasedFlusher-%d").build();
        Executor threadPoolExecutor = Executors.newCachedThreadPool(namedThreadFactory);

        disruptor = new Disruptor<>(new DeliveryEventData.DeliveryEventDataFactory(), ringBufferSize,
                                                     threadPoolExecutor,
                                                     ProducerType.MULTI,
                                                     new SleepingBlockingWaitStrategy());

        disruptor.handleExceptionsWith(new DeliveryExceptionHandler());

        // This barrier is used for contentReaders. Content read processors process events first. Hence take the
        // barrier directly from ring buffer
        SequenceBarrier barrier = disruptor.getRingBuffer().newBarrier();

        // Initialize content readers
        ConcurrentContentReadTaskBatchProcessor[] contentReadTaskBatchProcessor = new ConcurrentContentReadTaskBatchProcessor[parallelContentReaders];
        for (int i = 0; i < parallelContentReaders; i++) {
            contentReadTaskBatchProcessor[i] = new ConcurrentContentReadTaskBatchProcessor(
                    disruptor.getRingBuffer(),
                    barrier,
                    new ContentCacheCreator(maxContentChunkSize),
                    i,
                    parallelContentReaders,
                    contentSizeToBatch);

            contentReadTaskBatchProcessor[i].setExceptionHandler(new DeliveryExceptionHandler());
        }

        // Initialize decompression handlers
        ContentDecompressionHandler[] decompressionEventHandlers =
                new ContentDecompressionHandler[parallelDecompressionHandlers];
        for (int i = 0; i < parallelDecompressionHandlers; i++) {
            decompressionEventHandlers[i] = new ContentDecompressionHandler(maxContentChunkSize);
        }

        // Initialize delivery handlers
        DeliveryEventHandler[] deliveryEventHandlers = new DeliveryEventHandler[parallelDeliveryHandlers];
        for (int i = 0; i < parallelDeliveryHandlers; i++) {
            deliveryEventHandlers[i] = new DeliveryEventHandler(i, parallelDeliveryHandlers);
        }

        disruptor.handleEventsWith(contentReadTaskBatchProcessor).then(decompressionEventHandlers)
                .then(deliveryEventHandlers);

        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();

        //Will add the gauge listener to periodically calculate the outbound messages in the ring
//        MetricManager.gauge(MetricsConstants.DISRUPTOR_OUTBOUND_RING, Level.INFO, new OutBoundRingGauge());
    }

    /**
     * Submit a delivery task to the flusher
     *
     * @param subscription
     *         Local subscription
     * @param metadata
     *         Message metadata
     */
    public void submit(AndesSubscription subscription, ProtocolMessage metadata) {

        //Tracing Message
        MessageTracer.trace(metadata.getMessage(), MessageTracer.PUBLISHED_TO_OUTBOUND_DISRUPTOR);

        long nextSequence = ringBuffer.next();

        // Initialize event data holder
        DeliveryEventData data = ringBuffer.get(nextSequence);
        data.setLocalSubscription(subscription);
        data.setMetadata(metadata);

        ringBuffer.publish(nextSequence);
    }

    /**
     * Waits until all events currently in the disruptor have been processed by all event processors
     * and then halts the processors. It is critical that publishing to the ring buffer has stopped
     * before calling this method, otherwise it may never return.
     */
    public void stop() {
        try {
            disruptor.shutdown(OUTBOUND_DISRUPTOR_SHUTDOWN_WAIT_TIME, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("Outbound disruptor did not shut down properly.");
        }
    }

//        TODO: Metrics revamp
    /**
     * Utility class used to gauge ring size.
     *
     */
//    private class OutBoundRingGauge implements Gauge<Long> {
//        @Override
//        public Long getValue() {
//            //The total ring size will be reduced from the remaining ring size
//            return ringBuffer.getBufferSize() - ringBuffer.remainingCapacity();
//        }
//    }
}

