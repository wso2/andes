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

    /**
     * Global high limit that trigger flow control globally
     */
    private final int globalLowLimit;

    /**
     * Global low limit that disables trigger flow control disable
     */
    private final int globalHighLimit;

    /**
     * List of active channels
     */
    private final ArrayList<AndesChannel> channels;

    /**
     * Executor used for flow control timeout tasks
     */
    private final ScheduledExecutorService executor;

    /**
     * Configured flow control high limit for local channel
     */
    private final int channelHighLimit;

    /**
     * Configured flow control low limit for local channel
     */
    private final int channelLowLimit;

    /**
     * Track total number of unprocessed messages
     */
    private AtomicInteger messagesOnGlobalBuffer;

    /**
     * Indicate if the flow control is enabled globally
     */
    private boolean globalFlowControlEnabled;

    /**
     * Global flow control time out task
     */
    private Runnable flowControlTimeoutTask = new FlowControlTimeoutTask();

    /**
     * Used to close the flow control timeout task if not required
     */
    private ScheduledFuture<?> scheduledFlowControlTimeoutFuture;


    public FlowControlManager() {
        // Read configured limits
        globalLowLimit = (Integer) AndesConfigurationManager
                .readValue(AndesConfiguration.FLOW_CONTROL_GLOBAL_LOW_LIMIT);
        globalHighLimit = (Integer) AndesConfigurationManager
                .readValue(AndesConfiguration.FLOW_CONTROL_GLOBAL_HIGH_LIMIT);
        channelLowLimit = ((Integer) AndesConfigurationManager
                .readValue(AndesConfiguration.FLOW_CONTROL_BUFFER_BASED_LOW_LIMIT));
        channelHighLimit = ((Integer) AndesConfigurationManager
                .readValue(AndesConfiguration.FLOW_CONTROL_BUFFER_BASED_HIGH_LIMIT));

        if (globalHighLimit <= globalLowLimit || channelHighLimit <= channelLowLimit) {
            throw new RuntimeException("Flow Control limits are not configured correctly.");
        }

        messagesOnGlobalBuffer = new AtomicInteger(0);
        globalFlowControlEnabled = false;
        channels = new ArrayList<AndesChannel>();

        // Initialize executor service for state validity checking
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("AndesScheduledTaskManager")
                                                                     .build();
        executor = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
    }

    /**
     * Create a new Andes channel for a new local channel.
     *
     * @param listener
     *         Local flow control listener
     * @return AndesChannel
     */
    public synchronized AndesChannel createChannel(FlowControlListener listener) {
        AndesChannel channel = new AndesChannel(this, listener, globalFlowControlEnabled);
        channels.add(channel);
        return channel;
    }

    /**
     * Get the flow control high limit for local channel
     *
     * @return Flow control high limit
     */
    public int getChannelHighLimit() {
        return channelHighLimit;
    }

    /**
     * Get the flow control low limit for local channel
     *
     * @return Flow control low limit
     */
    public int getChannelLowLimit() {
        return channelLowLimit;
    }

    /**
     * Get the scheduled executor used for flow controlling tasks
     *
     * @return Scheduled executor
     */
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
        int count = messagesOnGlobalBuffer.addAndGet(size);

        if (!globalFlowControlEnabled && count >= globalHighLimit) {
            blockListeners();
        }
    }

    /**
     * Notify all the channel to enable flow control
     */
    private synchronized void blockListeners() {
        if (!globalFlowControlEnabled) {
            globalFlowControlEnabled = true;

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
        int count = messagesOnGlobalBuffer.addAndGet(-size);

        if (globalFlowControlEnabled && count <= globalLowLimit) {
            unblockListeners();
        }
    }

    /**
     * Notify all the channels to disable flow control
     */
    private synchronized void unblockListeners() {
        if (globalFlowControlEnabled) {
            scheduledFlowControlTimeoutFuture.cancel(false);
            globalFlowControlEnabled = false;

            for (AndesChannel channel : channels) {
                channel.notifyGlobalFlowControlDeactivation();
            }

            log.info("Global flow control disabled.");
        }
    }

    /**
     * Remove channel from tracking
     *
     * @param channel
     *         Andes channel
     */
    public synchronized void deleteChannel(AndesChannel channel) {
        channels.remove(channel);
    }

    /**
     * This timeout task avoid flow control being enforced forever. This can happen if the recordAdditionToBuffer get a
     * context switch after evaluating the existing condition and during that time all the messages present in global
     * buffer get processed from the StateEventHandler.
     */
    private class FlowControlTimeoutTask implements Runnable {
        @Override
        public void run() {
            if (globalFlowControlEnabled && messagesOnGlobalBuffer.get() <= globalLowLimit) {
                unblockListeners();
            }
        }
    }
}
