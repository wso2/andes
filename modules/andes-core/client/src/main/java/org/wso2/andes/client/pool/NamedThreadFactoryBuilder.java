/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.

 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at

 *    http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.client.pool;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is extracted from the ThreadFactoryBuilder in Guava library.
 *
 * https://github.com/google/guava/
 */
public class NamedThreadFactoryBuilder {

    private String nameFormat = null;
    private Boolean daemon = null;
    private Integer priority = null;
    private UncaughtExceptionHandler uncaughtExceptionHandler = null;
    private ThreadFactory backingThreadFactory = null;

    /**
     * Creates a new {@link ThreadFactory} builder.
     */
    public NamedThreadFactoryBuilder() {
    }

    /**
     * Sets the naming format to use when naming threads ({@link Thread#setName}) which are created
     * with this ThreadFactory.
     *
     * @param nameFormat a {@link String#format(String, Object...)}-compatible format String, to which
     *                   a unique integer (0, 1, etc.) will be supplied as the single parameter. This integer will
     *                   be unique to the built instance of the ThreadFactory and will be assigned sequentially. For
     *                   example, {@code "rpc-pool-%d"} will generate thread names like {@code "rpc-pool-0"}, {@code
     *                   "rpc-pool-1"}, {@code "rpc-pool-2"}, etc.
     * @return this for the builder pattern
     */
    public NamedThreadFactoryBuilder setNameFormat(String nameFormat) {
        String unused = format(nameFormat, 0); // fail fast if the format is bad or null
        this.nameFormat = nameFormat;
        return this;
    }

    /**
     * Sets daemon or not for new threads created with this ThreadFactory.
     *
     * @param daemon whether or not new Threads created with this ThreadFactory will be daemon threads
     * @return this for the builder pattern
     */
    public NamedThreadFactoryBuilder setDaemon(boolean daemon) {
        this.daemon = daemon;
        return this;
    }

    /**
     * Sets the priority for new threads created with this ThreadFactory.
     *
     * @param priority the priority for new Threads created with this ThreadFactory
     * @return this for the builder pattern
     */
    public NamedThreadFactoryBuilder setPriority(int priority) {
        this.priority = priority;
        return this;
    }

    /**
     * Sets the {@link UncaughtExceptionHandler} for new threads created with this ThreadFactory.
     *
     * @param uncaughtExceptionHandler the uncaught exception handler for new Threads created with
     *                                 this ThreadFactory
     * @return this for the builder pattern
     */
    public NamedThreadFactoryBuilder setUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        return this;
    }

    /**
     * Sets the backing {@link ThreadFactory} for new threads created with this ThreadFactory. Threads
     * will be created by invoking #newThread(Runnable) on this backing {@link ThreadFactory}.
     *
     * @param backingThreadFactory the backing {@link ThreadFactory} which will be delegated to during
     *                             thread creation.
     * @return this for the builder pattern
     */
    public NamedThreadFactoryBuilder setThreadFactory(ThreadFactory backingThreadFactory) {
        this.backingThreadFactory = backingThreadFactory;
        return this;
    }

    /**
     * Returns a new thread factory using the options supplied during the building process. After
     * building, it is still possible to change the options used to build the ThreadFactory and/or
     * build again. State is not shared amongst built instances.
     *
     * @return the fully constructed {@link ThreadFactory}
     */
    public ThreadFactory build() {
        return doBuild(this);
    }

    private ThreadFactory doBuild(NamedThreadFactoryBuilder builder) {
        final String nameFormat = builder.nameFormat;
        final Boolean daemon = builder.daemon;
        final Integer priority = builder.priority;
        final UncaughtExceptionHandler uncaughtExceptionHandler = builder.uncaughtExceptionHandler;
        final ThreadFactory backingThreadFactory =
                (builder.backingThreadFactory != null)
                        ? builder.backingThreadFactory
                        : Executors.defaultThreadFactory();
        final AtomicLong count = (nameFormat != null) ? new AtomicLong(0) : null;
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = backingThreadFactory.newThread(runnable);
                if (nameFormat != null) {
                    thread.setName(format(nameFormat, count.getAndIncrement()));
                }
                if (daemon != null) {
                    thread.setDaemon(daemon);
                }
                if (priority != null) {
                    thread.setPriority(priority);
                }
                if (uncaughtExceptionHandler != null) {
                    thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
                }
                return thread;
            }
        };
    }

    private static String format(String format, Object... args) {
        return String.format(Locale.ROOT, format, args);
    }
}
