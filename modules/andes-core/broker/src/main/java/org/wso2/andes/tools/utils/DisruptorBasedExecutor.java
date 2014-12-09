/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.tools.utils;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.distrupter.*;
import org.wso2.andes.server.cassandra.SequentialThreadPoolExecutor;

public class DisruptorBasedExecutor {

    private static Log log = LogFactory.getLog(SequentialThreadPoolExecutor.class);
    private static boolean isDebugEnabled = log.isDebugEnabled();

//    private static DisruptorRuntime<CassandraDataEvent> cassandraRWDisruptorRuntime;
//    private static DisruptorRuntime<CassandraDataEvent> ackDataEvenRuntime;
    private final static Map<UUID, PendingJob> pendingJobsTracker = new ConcurrentHashMap<UUID, PendingJob>();

    private Disruptor<CassandraDataEvent> disruptor;
    private RingBuffer<CassandraDataEvent> ringBuffer;
//    private RingBuffer<CassandraDataEvent> ackRingBuffer;

    public DisruptorBasedExecutor(MessageStore messageStore) {

//        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("DisruptorBasedExecutor-%d").build();
//        ExecutorService executorPool = Executors.newCachedThreadPool(namedThreadFactory);
//
//        disruptor = new Disruptor<CassandraDataEvent>(CassandraDataEvent.getFactory(), executorPool,
//                new MultiThreadedClaimStrategy(65536), // this is for multiple publishers)
//                new BlockingWaitStrategy());
//
//        int MAX_WRITE_HANDLERS = 8;
//        AlternatingCassandraWriter[] writerHandlers = new AlternatingCassandraWriter[MAX_WRITE_HANDLERS];
//        for (int i = 0; i < MAX_WRITE_HANDLERS; i++) {
//            writerHandlers[i] = new AlternatingCassandraWriter(MAX_WRITE_HANDLERS, i, messageStore);
//        }
//
//        AckHandler[] ackHandlers = new AckHandler[MAX_WRITE_HANDLERS];
//        for (int i = 0; i < MAX_WRITE_HANDLERS; i++) {
//            ackHandlers[i] = new AckHandler(i, MAX_WRITE_HANDLERS);
//        }
//
//        // Write handlers and ack handlers run in parallel. state change handler comes after them
//        disruptor.handleEventsWith(writerHandlers).handleEventsWith(ackHandlers).then(new StateChangeHandler());
//
//        disruptor.handleExceptionsWith(new IgnoreExceptionHandler());
//        ringBuffer = disruptor.start();
//
//        executorPool = Executors.newCachedThreadPool(namedThreadFactory);
//        disruptor = new Disruptor<CassandraDataEvent>(CassandraDataEvent.getFactory(), executorPool,
//                new MultiThreadedClaimStrategy(65536), // this is for multiple publishers)
//                new BlockingWaitStrategy());
//
//        disruptor.handleEventsWith(new AckHandler()).then(new ChannelCloseEventHandler());
//        disruptor.handleExceptionsWith(new IgnoreExceptionHandler());
//

    }


    // TODO : Disruptor - pass the buffer and reuse
    public void messagePartReceived(AndesMessagePart part) {

        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        CassandraDataEvent event = ringBuffer.get(sequence);

        event.eventType = CassandraDataEvent.EventType.MESSAGE_PART_EVENT;
        event.part = part;
        // make the event available to EventProcessors
        ringBuffer.publish(sequence);
    }

    public void messageCompleted(final AndesMessageMetadata metadata) {
        UUID channelID = metadata.getChannelId();
        //This count how many jobs has finished
        synchronized (pendingJobsTracker) {
            PendingJob pendingJob = pendingJobsTracker.get(channelID);
            if (pendingJob == null) {
                pendingJob = new PendingJob();
                pendingJobsTracker.put(channelID, pendingJob);
            }
            pendingJob.submittedJobs = pendingJob.submittedJobs + 1;
        }

        long sequence = ringBuffer.next();
        CassandraDataEvent event = ringBuffer.get(sequence);
        event.eventType = CassandraDataEvent.EventType.META_DATA_EVENT;
        event.metadata = metadata;
        event.metadata.setPendingJobsTracker(pendingJobsTracker);
        // make the event available to EventProcessors
        //todo uncomment this and comment executer
        ringBuffer.publish(sequence);

    }

    public void channelCloseEvent(UUID channelID) {
//        RingBuffer<CassandraDataEvent> ringBuffer = ackDataEvenRuntime.getRingBuffer();
        long sequence = ringBuffer.next();
        CassandraDataEvent event = ringBuffer.get(sequence);
        event.eventType = CassandraDataEvent.EventType.CHANNEL_CLOSE_EVENT;
        event.channelID = channelID;
        ringBuffer.publish(sequence);
    }

    public void channelOpenEvent(UUID channelID) {
//        RingBuffer<CassandraDataEvent> ringBuffer = ackDataEvenRuntime.getRingBuffer();
        long sequence = ringBuffer.next();
        CassandraDataEvent event = ringBuffer.get(sequence);
        event.eventType = CassandraDataEvent.EventType.CHANNEL_OPEN_EVENT;
        event.channelID = channelID;
        ringBuffer.publish(sequence);
    }

    public void ackReceived(AndesAckData ackData) {
//        RingBuffer<CassandraDataEvent> ringBuffer = ackDataEvenRuntime.getRingBuffer();
        long sequence = ringBuffer.next();
        CassandraDataEvent event = ringBuffer.get(sequence);
        event.eventType = CassandraDataEvent.EventType.ACKNOWLEDGEMENT_EVENT;
        event.ackData = ackData;
        // make the event available to EventProcessors
        ringBuffer.publish(sequence);
    }

    public static void wait4JobsfromThisChannel2End(int channelId) {
        PendingJob pendingJobs;
        synchronized (pendingJobsTracker) {
            pendingJobs = pendingJobsTracker.get(channelId);
        }

        if (pendingJobs != null) {
            try {
                pendingJobs.semaphore.tryAcquire(pendingJobs.submittedJobs, 20, TimeUnit.SECONDS);
                if (isDebugEnabled) {
                    log.debug("All " + pendingJobs.submittedJobs + " completed for channel " + channelId);
                }
            } catch (InterruptedException e) {
                log.warn("Closing Channel " + channelId + " timed out waiting for submitted jobs to finish");
            } finally {
                synchronized (pendingJobsTracker) {
                    pendingJobsTracker.remove(channelId);
                }
            }
        }
    }

    public static class PendingJob {
        public Semaphore semaphore = new Semaphore(0);
        public int submittedJobs = 0;
    }

}
