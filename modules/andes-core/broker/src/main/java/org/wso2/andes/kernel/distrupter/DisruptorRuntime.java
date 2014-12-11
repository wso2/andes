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

package org.wso2.andes.kernel.distrupter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class DisruptorRuntime<T> {

    private Disruptor<T> disruptor;
    private RingBuffer<T> ringBuffer;


    private DisruptorRuntime(Builder<T> builder) {
        disruptor = builder.disruptor;
    }

    private void start() {
        ringBuffer = disruptor.start();
    }

    public RingBuffer<T> getRingBuffer() {
        return ringBuffer;
    }

    /**
     * Builder class for the Disruptor Runtime object
     * @param <T> DisruptorRuntime<T>
     */
    public static class Builder<T> {

        private static ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat
                ("DisruptorBasedExecutor-%d").build();
        private final Disruptor<T> disruptor;

        public Builder(EventFactory<T> eventFactory, EventHandler<T>[] handlers) {

            ExecutorService executorPool = Executors.newCachedThreadPool(namedThreadFactory);
            disruptor = new Disruptor<T>(eventFactory, executorPool,
                    new MultiThreadedClaimStrategy(65536), // this is for multiple publishers)
                    new BlockingWaitStrategy());
            // Using Blocking wait strategy over Sleeping wait strategy to over come CPU load issue happening when
            // starting up the sever. With this it will to resolve the issue https://wso2.org/jira/browse/MB-372
            // new SleepingWaitStrategy());

            disruptor.handleEventsWith(handlers);
            disruptor.handleExceptionsWith(new IgnoreExceptionHandler());
        }

        /**
         * Set handlers to consume from ring buffer
         * @param handlers EventHandler<T>[] array
         */
        public Builder<T> setHandlers(EventHandler<T>[] handlers) {
            disruptor.handleEventsWith(handlers);
            return this;
        }

        /**
         * Make handlers run After a given set of handlers. For Instance make handlers A run before handlers B.
         * @param dependsOn handlers that run run first (A)
         * @param handleAfter handlers that runs after (B)
         */
        public Builder<T> setDependantHandlers( EventHandler<T>[] dependsOn,  EventHandler<T>[] handleAfter) {
            disruptor.after(dependsOn).handleEventsWith(handleAfter);
            return this;
        }

        /**
         * Build the disruptor runtime object and starts the disruptor
         * @return Return the built DisruptorRuntime object
         */
        public DisruptorRuntime<T> build() {

            DisruptorRuntime<T> disruptorRuntime = new DisruptorRuntime<T>(this);
            disruptorRuntime.start();
            return disruptorRuntime;
        }
    }
}
