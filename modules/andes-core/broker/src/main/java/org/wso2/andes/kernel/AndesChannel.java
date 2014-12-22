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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * AndesChannel keep track of the states of the local channels
 */
public class AndesChannel {
    /**
     * Class logger
     */
    private static Log log = LogFactory.getLog(AndesChannel.class);

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

    /**
     * Number of messages waiting in the buffer
     */
    private AtomicInteger messagesOnBuffer;

    /**
     * Indicate if the flow control is enabled for this channel
     */
    private boolean flowControlEnabled;

    public AndesChannel(FlowControlListener listener) {
        this.listener = listener;

        this.flowControlLowLimit = ((Integer) AndesConfigurationManager.readValue(AndesConfiguration.FLOW_CONTROL_BUFFER_BASED_LOW_LIMIT));
        this.flowControlHighLimit = ((Integer) AndesConfigurationManager.readValue(AndesConfiguration.FLOW_CONTROL_BUFFER_BASED_HIGH_LIMIT));
        this.messagesOnBuffer = new AtomicInteger();
        flowControlEnabled = false;
    }

    /**
     * This method should be called when a message is put into the buffer
     *
     * @param size
     *         Number of items added to buffer
     */
    public void recordAdditionToBuffer(int size) {
        int count = messagesOnBuffer.addAndGet(size);

        if (!flowControlEnabled && count >= flowControlHighLimit) {
            flowControlEnabled = true;
            listener.block();

            log.info("Flow control enabled for channel.");
        }
    }

    /**
     * This method should be called after a message is processed and no longer required in the buffer.
     *
     * @param size
     *         Number of items removed from buffer
     */
    public void recordRemovalFromBuffer(int size) {
        int count = messagesOnBuffer.addAndGet(-size);

        if (flowControlEnabled && count <= flowControlLowLimit) {
            flowControlEnabled = false;
            listener.unblock();

            log.info("Flow control disabled for channel.");
        }
    }
}
