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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AndesChannel keep track of the states of the local channels
 */
public class AndesChannel {
    /**
     * Class logger
     */
    private static Log log = LogFactory.getLog(AndesChannel.class);

    private static AtomicLong idGenerator = new AtomicLong(0);
    /**
     * Lister used to communicate with the local channels
     */
    private final FlowControlListener listener;

    /**
     * This is the limit used to release flow control on the channel
     */
    private final int flowControlLowLimit;

    /**
     * This is the limit used to enforce the flow control on the channel
     */
    private final int flowControlHighLimit;
    private final FlowControlManager flowControlManager;
    private final long id;
    private final ScheduledExecutorService executor;
    private Runnable flowControlTimeoutTask = new FlowControlTimeoutTask();
    /**
     * Number of messages waiting in the buffer
     */
    private AtomicInteger messagesOnBuffer;
    /**
     * Indicate if the flow control is enabled for this channel
     */
    private boolean flowControlEnabled;
    private boolean globalFlowControlEnabled;
    private ScheduledFuture<?> scheduledFlowControlTimeoutFuture;

    public AndesChannel(FlowControlManager flowControlManager, FlowControlListener listener,
                        boolean flowControlEnabled) {
        this.flowControlManager = flowControlManager;
        this.listener = listener;
        this.executor = flowControlManager.getScheduledExecutor();
        globalFlowControlEnabled = flowControlEnabled;

        this.flowControlLowLimit = ((Integer) AndesConfigurationManager.readValue(AndesConfiguration.FLOW_CONTROL_BUFFER_BASED_LOW_LIMIT));
        this.flowControlHighLimit = ((Integer) AndesConfigurationManager.readValue(AndesConfiguration.FLOW_CONTROL_BUFFER_BASED_HIGH_LIMIT));
        
        this.id = idGenerator.incrementAndGet();
        this.messagesOnBuffer = new AtomicInteger();
        this.flowControlEnabled = false;

        log.info("Channel created with ID: " + id);
    }

    public void notifyGlobalFlowControlActivation() {
        globalFlowControlEnabled = true;
    }

    public void notifyGlobalFlowControlDeactivation() {
        globalFlowControlEnabled = false;

        if (flowControlEnabled) {
            unblockLocalChannel();
        }
    }

    private synchronized void unblockLocalChannel() {
        if (flowControlEnabled) {
            scheduledFlowControlTimeoutFuture.cancel(false);
            flowControlEnabled = false;
            listener.unblock();

            log.info("Flow control disabled for channel " + id + ".");
        }
    }

    /**
     * This method should be called when a message is put into the buffer
     *
     * @param size
     *         Number of items added to buffer
     */
    public void recordAdditionToBuffer(int size) {
        flowControlManager.notifyAddition(size);

        int count = messagesOnBuffer.addAndGet(size);

        if (!flowControlEnabled && (globalFlowControlEnabled || count >= flowControlHighLimit)) {
            blockLocalChannel();
        }
    }

    private synchronized void blockLocalChannel() {
        if (!flowControlEnabled) {
            flowControlEnabled = true;
            listener.block();

            scheduledFlowControlTimeoutFuture = executor.schedule(flowControlTimeoutTask, 1, TimeUnit.MINUTES);
            log.info("Flow control enabled for channel " + id + ".");
        }
    }

    /**
     * This method should be called after a message is processed and no longer required in the buffer.
     *
     * @param size
     *         Number of items removed from buffer
     */
    public void recordRemovalFromBuffer(int size) {
        flowControlManager.notifyRemoval(size);

        int count = messagesOnBuffer.addAndGet(-size);

        if (flowControlEnabled && !globalFlowControlEnabled && count <= flowControlLowLimit) {
            unblockLocalChannel();
        }
    }

    private class FlowControlTimeoutTask implements Runnable {

        @Override
        public void run() {
            if (flowControlEnabled && !globalFlowControlEnabled && messagesOnBuffer.get() <= flowControlLowLimit) {
                unblockLocalChannel();
            }
        }
    }
}
