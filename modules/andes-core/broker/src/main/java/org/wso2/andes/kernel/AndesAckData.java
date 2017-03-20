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
import org.wso2.andes.metrics.MetricsConstants;
import org.wso2.andes.tools.utils.MessageTracer;
import org.wso2.carbon.metrics.manager.Counter;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.Meter;
import org.wso2.carbon.metrics.manager.MetricManager;

import java.util.UUID;

/**
 * Wrapper class of message acknowledgment data publish to disruptor
 */
public class AndesAckData {

    private static Log log = LogFactory.getLog(AndesAckData.class);
    /**
     * Id of acknowledged message
     */
    private long IdOfAcknowledgedMessage;

    /**
     * Keep Metadata object represented by IdOfAcknowledgedMessage
     */
    private DeliverableAndesMetadata metadataReference;

    /**
     * ID of the channel acknowledge is received
     */
    private UUID channelID;

    /**
     * Holds if acknowledged message is ready to be removed. If all channels
     * acknowledged it becomes removable. Message reference cannot be used here
     * as we need to keep it in disruptor data holder
     */
    private boolean isBaringMessageRemovable = false;

    /**
     * Generate AndesAckData object. This holds acknowledge event in disruptor
     *
     * @param channelID ID of the channel ack is received
     * @param messageId Id of the message being acknowledged
     */
    public AndesAckData(UUID channelID, long messageId) {
        this.channelID = channelID;
        this.IdOfAcknowledgedMessage = messageId;
    }

    /**
     * Get the reference of the message being acknowledged
     *
     * @return Metadata of the acknowledged message
     */
    public long getIdOfAcknowledgedMessage() {
        return IdOfAcknowledgedMessage;
    }

    /**
     * Get ID of the channel acknowledgement is received
     *
     * @return channel ID
     */
    public UUID getChannelID() {
        return channelID;
    }

    /**
     * Set Reference to DeliverableAndesMetadata object being acknowledged.
     * This MUST be set only within disruptor handler.
     */
    public void setMeradataReference() {
        AndesSubscription localSubscription = AndesContext.getInstance().
                getAndesSubscriptionManager().getSubscriptionByProtocolChannel(channelID);
        if (null == localSubscription) {
            log.error("Cannot handle acknowledgement for message ID = "
                    + IdOfAcknowledgedMessage + " as subscription is closed "
                    + "channelID= " + "" + channelID);
            metadataReference = null;
        } else {
            metadataReference = localSubscription.
                    getSubscriberConnection().getUnAckedMessage(IdOfAcknowledgedMessage);
            trace();
        }
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
     * Check if message being acknowledged is ready to be removed
     *
     * @return true if removable
     */
    public boolean isBaringMessageRemovable() {
        return isBaringMessageRemovable;
    }

    /**
     * Set message being acknowledged is ready to be removed. This happens
     * if acknowledgements are received from all channels
     */
    public void setBaringMessageRemovable() {
        this.isBaringMessageRemovable = true;
    }

    /**
     * Trace the acknowledgement
     */
    private void trace() {
        //Tracing Message
        MessageTracer.trace(IdOfAcknowledgedMessage,
                metadataReference.getDestination(), MessageTracer.ACK_RECEIVED_FROM_PROTOCOL);

        //Adding metrics meter for ack rate
        Meter ackMeter = MetricManager.meter(MetricsConstants.ACK_RECEIVE_RATE
                + MetricsConstants.METRICS_NAME_SEPARATOR
                + metadataReference.getMessageRouterName() + MetricsConstants.METRICS_NAME_SEPARATOR
                + metadataReference.getDestination(), Level.INFO);
        ackMeter.mark();

        //Adding metrics counter for ack messages
        Counter counter = MetricManager.counter(MetricsConstants.ACK_MESSAGES + MetricsConstants.METRICS_NAME_SEPARATOR
                + metadataReference.getMessageRouterName() + MetricsConstants.METRICS_NAME_SEPARATOR
                + metadataReference.getDestination(), Level.INFO);
        counter.inc();
    }

}
