package org.wso2.andes.tools.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.distrupter.*;
import org.wso2.andes.pool.AndesExecuter;
import org.wso2.andes.server.cassandra.SequentialThreadPoolExecutor;

import com.lmax.disruptor.RingBuffer;

public class DisruptorBasedExecutor {

    private static Log log = LogFactory.getLog(SequentialThreadPoolExecutor.class);
    private static boolean isDebugEnabled = log.isDebugEnabled();

    private static DisruptorRuntime<CassandraDataEvent> cassandraRWDisruptorRuntime;
    private static DisruptorRuntime<AndesAckData> ackDataEvenRuntime;
    //private static DisruptorRuntime<SubscriptionDataEvent> dataDeliveryDisruptorRuntime;
    private static Map<Long, PendingJob> pendingJobsTracker = new ConcurrentHashMap<Long, PendingJob>();
     private MessageStoreManager messageStoreManager;

    public DisruptorBasedExecutor(MessageStoreManager messageStoreManager) {
        log.info("Changed v1 ");
        this.messageStoreManager = messageStoreManager;
        int MAX_WRITE_HANDLERS = 10;
        AlternatingCassandraWriter[] writerHandlers = new AlternatingCassandraWriter[MAX_WRITE_HANDLERS];
        for (int i = 0; i < writerHandlers.length; i++) {
            writerHandlers[i] = new AlternatingCassandraWriter(MAX_WRITE_HANDLERS, i, messageStoreManager);
        }
        cassandraRWDisruptorRuntime = new DisruptorRuntime<CassandraDataEvent>(CassandraDataEvent.getFactory(), writerHandlers);

        ackDataEvenRuntime = new DisruptorRuntime<AndesAckData>(AndesAckData.getFactory(), new AckHandler[]{new AckHandler(
                messageStoreManager)});

        int MAX_SEND_HANDLERS = 10;
//        SubscriptionDataSender[] subscriptionHandlers = new SubscriptionDataSender[MAX_SEND_HANDLERS];
//        for (int i = 0; i < subscriptionHandlers.length; i++) {
//            subscriptionHandlers[i] = new SubscriptionDataSender(MAX_SEND_HANDLERS, i, delivery);
//        }
//        dataDeliveryDisruptorRuntime = new DisruptorRuntime<SubscriptionDataEvent>(SubscriptionDataEvent.getFactory(), subscriptionHandlers);
    }

//    public void messagesReadyToBeSent(final Subscription subscription, final QueueEntry message){
//        // Get the Disruptor ring from the runtime
//        RingBuffer<SubscriptionDataEvent> ringBuffer = dataDeliveryDisruptorRuntime.getRingBuffer();
//        // Publishers claim events in sequence
//        long sequence = ringBuffer.next();
//        SubscriptionDataEvent event = ringBuffer.get(sequence);
//
//        event.subscription = subscription;
//        event.message = message;
//        // make the event available to EventProcessors
//        ringBuffer.publish(sequence);
//    }

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
        long channelID = metadata.getChannelId();
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
       // ringBuffer.publish(sequence);
          final AndesMessageMetadata mdata = metadata;
                    AndesExecuter.runAsync(new Runnable() {
                @Override
                public void run() {
                    try {
                        messageStoreManager.storeMetadata(mdata);
                    } catch (AndesException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }

            });
    }

    public void ackReceived(AndesAckData ackData) {
        RingBuffer<AndesAckData> ringBuffer = ackDataEvenRuntime.getRingBuffer();
        long sequence = ringBuffer.next();
        AndesAckData event = ringBuffer.get(sequence);
        event.messageID = ackData.messageID;
        event.qName = ackData.qName;
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
