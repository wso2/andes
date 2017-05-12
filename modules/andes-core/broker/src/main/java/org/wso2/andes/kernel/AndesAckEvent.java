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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.tools.utils.MessageTracer;

/**
 * Wrapper class of message acknowledgment data publish to disruptor
 */
public class AndesAckEvent {

    private static Log log = LogFactory.getLog(AndesAckEvent.class);
    /**
     * Acknowledgement information form protocol
     */
    private AndesAckData ackData;

    /**
     * Holds DeliverableAndesMetadata object reference to match with acknowledged message
     */
    private DeliverableAndesMetadata metadataReference;

    /**
     * Holds if acknowledged message is ready to be removed. If all channels
     * acknowledged it becomes removable. Message reference cannot be used here
     * as we need to keep it in disruptor data holder
     */
    private boolean isBaringMessageRemovable = false;

    /**
     * Generate AndesAckEvent object. This holds acknowledge event in disruptor
     *
     * @param ackData acknowledge information from channel
     */
    public AndesAckEvent(AndesAckData ackData) {
        this.ackData = ackData;
    }

    /**
     * Look up and set reference of message acknowledged. Should be called only via a disruptor handler.
     *
     * @throws AndesException in case of message reference is not found
     */
    public void setMetadataReference() throws AndesException {
        AndesSubscription localSubscription = AndesContext.getInstance().
                getAndesSubscriptionManager().getSubscriptionByProtocolChannel(ackData.getChannelId());
        if (null == localSubscription) {
            throw new AndesException("Cannot handle acknowledgement for message ID = "
                    + ackData.getMessageId() + " as subscription is closed "
                    + "channelID= " + "" + ackData.getChannelId());
        } else {
            this.metadataReference = localSubscription.
                    getSubscriberConnection().getUnAckedMessage(ackData.getMessageId());

        }
        addTraceLog();
    }

    /**
     * Get reference to DeliverableAndesMetadata object that is acknowledged
     *
     * @return DeliverableAndesMetadata object being acknowledged
     */
    public DeliverableAndesMetadata getMetadataReference() {
        return metadataReference;
    }

    /**
     * Process the acknowledgement event. This call will update message status, return message
     * if eligible to be removed. For topics message is shared. If all acknowledgements are
     * received only we should remove message
     *
     * @return true if eligible to delete
     * @throws AndesException on an issue removing the message from store
     */
    public boolean processEvent() throws AndesException {
        // For topics message is shared. If all acknowledgements are received only we should remove message
        boolean deleteMessage = metadataReference.markAsAcknowledgedByChannel(ackData.getChannelId());

        AndesSubscription subscription = AndesContext.getInstance().getAndesSubscriptionManager()
                .getSubscriptionByProtocolChannel(ackData.getChannelId());

        subscription.onMessageAck(ackData.getMessageId());

        if (deleteMessage) {
            if (log.isDebugEnabled()) {
                log.debug("Ok to delete message id " + ackData.getMessageId());
            }
            //it is a must to set this to event container. Otherwise, multiple event handlers will see the status
            this.isBaringMessageRemovable = true;
        }

        return deleteMessage;
    }

    /**
     * Check if message being acknowledged is ready to be removed
     *
     * @return true if removable
     */
    public boolean isBaringMessageRemovable() {
        return isBaringMessageRemovable;
    }

    /**
     * Add trace log to message
     */
    private void addTraceLog() {
        //Tracing Message
        MessageTracer.trace(metadataReference.messageID,
                metadataReference.getDestination(), MessageTracer.ACK_MESSAGE_REFERENCE_SET_BY_DISRUPTOR);

        //Adding metrics meter for ack rate
//        Meter ackMeter = MetricManager.meter(MetricsConstants.ACK_RECEIVE_RATE
//                + MetricsConstants.METRICS_NAME_SEPARATOR
//                + metadataReference.getMessageRouterName() + MetricsConstants.METRICS_NAME_SEPARATOR
//                + metadataReference.getDestination(), Level.INFO);
//        ackMeter.mark();

        //Adding metrics counter for ack messages
//        Counter counter = MetricManager.counter(MetricsConstants.ACK_MESSAGES + MetricsConstants.METRICS_NAME_SEPARATOR
//                + metadataReference.getMessageRouterName() + MetricsConstants.METRICS_NAME_SEPARATOR
//                + metadataReference.getDestination(), Level.INFO);
//        counter.inc();
    }
}
