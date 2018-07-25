/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.client.pool;


import org.wso2.andes.pool.ReadWriteJobQueue;
import org.wso2.andes.pool.ReferenceCountingService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * This class is a replica of org.wso2.andes.pool.ReferenceCountingClientExecutorService in andes-core/common module
 * Reference class 'ReferenceCountingClientExecutorService' uses Guva library for thread pooling, which cannot be
 * applied as a dependency for the client module, hence the difference between the following class
 * ReferenceCountingClientExecutorService and ReferenceCountingClientExecutorService would be the usage of different
 * thread factories
 *
 * @link ReferenceCountingClientExecutorService
 */
public class ReferenceCountingClientExecutorService implements ReferenceCountingService {


    /**
     * Defines the smallest thread pool that will be allocated, irrespective of the number of processors.
     */
    private static final int MINIMUM_POOL_SIZE = 4;

    /**
     * Holds the number of processors on the machine.
     */
    private static final int NUM_CPUS = Runtime.getRuntime().availableProcessors();

    /**
     * Defines the thread pool size to use, which is the larger of the number of CPUs or the minimum size.
     */
    private static final int DEFAULT_POOL_SIZE = Math.max(NUM_CPUS, MINIMUM_POOL_SIZE);

    /**
     * Holds the singleton instance of this reference counter. This is only created once, statically, so the
     * {@link #getInstance()} method does not need to be synchronized.
     */
    private static final ReferenceCountingClientExecutorService _instance = new
            ReferenceCountingClientExecutorService();

    /**
     * This lock is used to ensure that reference counts are updated atomically with create/destroy operations.
     */
    private final Object _lock = new Object();

    /**
     * The shared executor service that is reference counted.
     */
    private ExecutorService _pool;

    /**
     * Holds the number of references given out to the executor service.
     */
    private int _refCount = 0;

    /**
     * Holds the number of executor threads to create.
     */
    private int _poolSize = Integer.getInteger("amqj.read_write_pool_size", DEFAULT_POOL_SIZE);

    /**
     * Thread Factory used to create Job thread pool.
     */
    private ThreadFactory _threadFactory = new NamedThreadFactoryBuilder().setNameFormat("AndesJobPoolThread-%d")
                                                                          .build();

    private final boolean _useBiasedPool = Boolean.getBoolean("org.apache.qpid.use_write_biased_pool");

    /**
     * Retrieves the singleton instance of this reference counter.
     *
     * @return The singleton instance of this reference counter.
     */
    public static ReferenceCountingClientExecutorService getInstance() {
        return _instance;
    }

    /**
     * Private constructor to ensure that only a singleton instance can be created.
     */
    private ReferenceCountingClientExecutorService() {
    }

    /**
     * Provides a reference to a shared executor service, incrementing the reference count.
     *
     * @return An executor service.
     */
    public ExecutorService acquireExecutorService() {
        synchronized (_lock) {
            if (_refCount++ == 0) {
                // Use a job queue that biases to writes
                if (_useBiasedPool) {
                    _pool = new ThreadPoolExecutor(_poolSize, _poolSize,
                            0L, TimeUnit.MILLISECONDS,
                            new ReadWriteJobQueue(),
                            _threadFactory);

                } else {
                    _pool = new ThreadPoolExecutor(_poolSize, _poolSize,
                            0L, TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<Runnable>(),
                            _threadFactory);
                }

            }


            return _pool;
        }
    }

    /**
     * Releases a reference to a shared executor service, decrementing the reference count. If the reference count falls
     * to zero, the executor service is shut down.
     */
    public void releaseExecutorService() {
        synchronized (_lock) {
            if (--_refCount == 0) {
                _pool.shutdownNow();
            }
        }
    }

    /**
     * Provides access to the executor service, without touching the reference count.
     *
     * @return The shared executor service, or <tt>null</tt> if none has been instantiated yet.
     */
    public ExecutorService getPool() {
        return _pool;
    }

    /**
     * Return the ReferenceCount to this ExecutorService
     *
     * @return reference count
     */
    public int getReferenceCount() {
        return _refCount;
    }

    /**
     * Return the thread factory used by the {@link ThreadPoolExecutor} to create new threads.
     *
     * @return thread factory
     */
    public ThreadFactory getThreadFactory() {
        return _threadFactory;
    }

    /**
     * Sets the thread factory used by the {@link ThreadPoolExecutor} to create new threads.
     * <p/>
     * If the pool has been already created, the change will have no effect until
     * {@link #getReferenceCount()} reaches zero and the pool recreated.  For this reason,
     * callers must invoke this method <i>before</i> calling {@link #acquireExecutorService()}.
     *
     * @param threadFactory thread factory
     */
    public void setThreadFactory(final ThreadFactory threadFactory) {
        if (threadFactory == null) {
            throw new NullPointerException("threadFactory cannot be null");
        }
        _threadFactory = threadFactory;
    }

}
