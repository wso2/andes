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


import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.distrupter.*;
import org.wso2.andes.server.cassandra.SequentialThreadPoolExecutor;

import com.lmax.disruptor.RingBuffer;

public class DisruptorBasedExecutor {

    private static Log log = LogFactory.getLog(SequentialThreadPoolExecutor.class);
    private static boolean isDebugEnabled = log.isDebugEnabled();

    private static DisruptorRuntime<CassandraDataEvent> cassandraRWDisruptorRuntime;
    private static DisruptorRuntime<AndesAckData> ackDataEvenRuntime;
    //private static DisruptorRuntime<SubscriptionDataEvent> dataDeliveryDisruptorRuntime;
    private static Map<UUID, PendingJob> pendingJobsTracker = new ConcurrentHashMap<UUID, PendingJob>();
    private MessageStoreManager messageStoreManager;


    public DisruptorBasedExecutor(MessageStoreManager messageStoreManager) {
        this.messageStoreManager = messageStoreManager;
        int MAX_WRITE_HANDLERS = 10;
        AlternatingCassandraWriter[] writerHandlers = new AlternatingCassandraWriter[MAX_WRITE_HANDLERS];
        for (int i = 0; i < writerHandlers.length; i++) {
            writerHandlers[i] = new AlternatingCassandraWriter(MAX_WRITE_HANDLERS, i, messageStoreManager);
        }
        cassandraRWDisruptorRuntime = new DisruptorRuntime<CassandraDataEvent>(CassandraDataEvent.getFactory(), writerHandlers);

        ackDataEvenRuntime = new DisruptorRuntime<AndesAckData>(AndesAckData.getFactory(), new AckHandler[]{new AckHandler(
                messageStoreManager)});

    }


    // TODO : Disruptor - pass the buffer and reuse
    public void messagePartReceived(AndesMessagePart part) {
        // Get the Disruptor ring from the runtime
        RingBuffer<CassandraDataEvent> ringBuffer = cassandraRWDisruptorRuntime.getRingBuffer();
        // Publishers claim events in sequence
        long sequence = ringBuffer.next();
        CassandraDataEvent event = ringBuffer.get(sequence);

        event.isPart = true;
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

        RingBuffer<CassandraDataEvent> ringBuffer = cassandraRWDisruptorRuntime.getRingBuffer();
        long sequence = ringBuffer.next();
        CassandraDataEvent event = ringBuffer.get(sequence);
        event.isPart = false;
        event.metadata = metadata;
        event.metadata.setPendingJobsTracker(pendingJobsTracker);
        // make the event available to EventProcessors
        //todo uncomment this and comment executer
        ringBuffer.publish(sequence);

    }

    public void ackReceived(AndesAckData ackData) {
        RingBuffer<AndesAckData> ringBuffer = ackDataEvenRuntime.getRingBuffer();
        long sequence = ringBuffer.next();
        AndesAckData event = ringBuffer.get(sequence);
        event.messageID = ackData.messageID;
        event.destination = ackData.destination;
        event.msgStorageDestination = ackData.msgStorageDestination;
        event.channelID = ackData.channelID;
        event.isTopic = ackData.isTopic;
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
                log.warn("Closing Channnel " + channelId + "timedout waiting for submitted jobs to finish");
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
