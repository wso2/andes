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

package org.wso2.andes.kernel;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FlowControlManager {

    /**
     * Class logger
     */
    private static Log log = LogFactory.getLog(AndesChannel.class);

    private final int globalLowLimit;
    private final int globalHighLimit;
    private final ArrayList<AndesChannel> channels;
    private final ScheduledExecutorService executor;
    private AtomicInteger messagesOnBuffer;
    private boolean flowControlEnabled;
    private Runnable flowControlTimeoutTask = new FlowControlTimeoutTask();
    private ScheduledFuture<?> scheduledFlowControlTimeoutFuture;


    public FlowControlManager() {
        globalLowLimit = AndesConfigurationManager
                .readValue(AndesConfiguration.FLOW_CONTROL_GLOBAL_LOW_LIMIT);
        globalHighLimit = AndesConfigurationManager
                .readValue(AndesConfiguration.FLOW_CONTROL_GLOBAL_HIGH_LIMIT);
        messagesOnBuffer = new AtomicInteger(0);
        flowControlEnabled = false;
        channels = new ArrayList<AndesChannel>();

        // Initialize executor service for state validity checking
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("AndesScheduledTaskManager")
                                                                     .build();
        executor = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
    }

    public synchronized AndesChannel createChannel(FlowControlListener listener) {
        AndesChannel channel = new AndesChannel(this, listener, flowControlEnabled);
        channels.add(channel);
        return channel;
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return executor;
    }

    /**
     * This method should be called when a message is put into the buffer
     *
     * @param size
     *         Number of items added to buffer
     */
    public void notifyAddition(int size) {
        int count = messagesOnBuffer.addAndGet(size);

        if (!flowControlEnabled && count >= globalHighLimit) {
            blockListeners();
        }
    }

    private synchronized void blockListeners() {
        if (!flowControlEnabled) {
            flowControlEnabled = true;

            for (AndesChannel channel : channels) {
                channel.notifyGlobalFlowControlActivation();
            }

            scheduledFlowControlTimeoutFuture = executor.schedule(flowControlTimeoutTask, 1, TimeUnit.MINUTES);
            log.info("Global flow control enabled.");
        }
    }

    /**
     * This method should be called after a message is processed and no longer required in the buffer.
     *
     * @param size
     *         Number of items removed from buffer
     */
    public void notifyRemoval(int size) {
        int count = messagesOnBuffer.addAndGet(-size);

        if (flowControlEnabled && count <= globalLowLimit) {
            unblockListeners();
        }
    }

    private synchronized void unblockListeners() {
        if (flowControlEnabled) {
            scheduledFlowControlTimeoutFuture.cancel(false);
            flowControlEnabled = false;

            for (AndesChannel channel : channels) {
                channel.notifyGlobalFlowControlDeactivation();
            }

            log.info("Global flow control disabled.");
        }
    }

    public synchronized void removeChannel(AndesChannel channel) {
        channels.remove(channel);
    }

    private class FlowControlTimeoutTask implements Runnable {
        @Override
        public void run() {
            if (flowControlEnabled && messagesOnBuffer.get() <= globalLowLimit) {
                unblockListeners();
            }
        }
    }
}
