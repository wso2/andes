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
package org.wso2.andes.server.cassandra;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class SequentialThreadPoolExecutor {

    private static Log log = LogFactory.getLog(SequentialThreadPoolExecutor.class);
    private static boolean isDebugEnabled = log.isDebugEnabled();
    private static Map<Long, PendingJob> pendingJobsTracker = new ConcurrentHashMap<Long, PendingJob>();
    private List<ExecutorService> executorServiceList;
    private int size = -1;

    public SequentialThreadPoolExecutor(int size, String poolName) {
        this.size = size;
        executorServiceList = new ArrayList<ExecutorService>(size);
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(poolName + "-%d").build();
        for (int i = 0; i < size; i++) {
            executorServiceList.add(Executors.newFixedThreadPool(1, namedThreadFactory));
        }
    }

    public void submit(Runnable runnable, long uniqueId) {
        synchronized (pendingJobsTracker) {
            PendingJob pendingJob = pendingJobsTracker.get(uniqueId);
            if (pendingJob == null) {
                pendingJob = new PendingJob();
                pendingJobsTracker.put(uniqueId, pendingJob);
            }
            pendingJob.submittedJobs = pendingJob.submittedJobs + 1;
        }

        int executorId = Math.abs((int) (uniqueId % size));
        executorServiceList.get(executorId).submit(new RunnableWrapper(runnable, uniqueId));
    }

    private class RunnableWrapper implements Runnable {
        Runnable runnable;
        long channelID;

        @Override
        public void run() {
            try {
                long start = System.currentTimeMillis();
                runnable.run();
                if (isDebugEnabled) {
                    long timetook = System.currentTimeMillis() - start;
                    if(timetook > 20){
                        log.debug(new StringBuffer().append("took ").append(runnable.getClass().getName()).append(" ")
                                .append(timetook));
                    }
                }
            } finally {
                synchronized (pendingJobsTracker) {
                    pendingJobsTracker.get(channelID).semaphore.release();
                }
            }
        }

        public RunnableWrapper(Runnable runnable, long channelID) {
            this.runnable = runnable;
            this.channelID = channelID;
        }
    }

    public static void wait4JobsfromThisChannel2End(String channelId) {
        PendingJob pendingJobs;
        synchronized (pendingJobsTracker) {
            pendingJobs = pendingJobsTracker.get(channelId);
        }

        if (pendingJobs != null) {
            try {
                pendingJobs.semaphore.tryAcquire(pendingJobs.submittedJobs, 1, TimeUnit.SECONDS);
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

    public int getSize() {
        int workQueueSize = 0;
        for (ExecutorService executor : executorServiceList) {
            workQueueSize = workQueueSize + ((ThreadPoolExecutor) executor).getQueue().size();
        }
        return workQueueSize;
    }

    public static class PendingJob {
        Semaphore semaphore = new Semaphore(0);
        int submittedJobs = 0;
    }
}
