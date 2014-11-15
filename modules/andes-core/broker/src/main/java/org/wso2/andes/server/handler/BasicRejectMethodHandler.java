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
package org.wso2.andes.server.handler;

import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.amqp.QpidAMQPBridge;
import org.wso2.andes.framing.BasicRejectBody;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.apache.log4j.Logger;

public class BasicRejectMethodHandler implements StateAwareMethodListener<BasicRejectBody>
{
    private static final Logger _logger = Logger.getLogger(BasicRejectMethodHandler.class);

    private static BasicRejectMethodHandler _instance = new BasicRejectMethodHandler();

    public static BasicRejectMethodHandler getInstance()
    {
        return _instance;
    }

    private BasicRejectMethodHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, BasicRejectBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();

        AMQChannel channel = session.getChannel(channelId);

        if (channel == null)
        {
            throw body.getChannelNotFoundException(channelId);
        }

        if (_logger.isDebugEnabled())
        {
            _logger.debug("Rejecting:" + body.getDeliveryTag() +
                          ": Requeue:" + body.getRequeue() +
                          //": Resend:" + evt.getMethod().resend +
                          " on channel:" + channel.debugIdentity());
        }

        long deliveryTag = body.getDeliveryTag();

        QueueEntry message = channel.getUnacknowledgedMessageMap().get(deliveryTag);

        if (message == null)
        {
            _logger.warn("Dropping reject request as message is null for tag:" + deliveryTag);
//            throw evt.getMethod().getChannelException(AMQConstant.NOT_FOUND, "Delivery Tag(" + deliveryTag + ")not known");
        }                 
        else
        {
            if (message.isQueueDeleted())
            {
                _logger.warn("Message's Queue as already been purged, unable to Reject. " +
                             "Dropping message should use Dead Letter Queue");
                message = channel.getUnacknowledgedMessageMap().remove(deliveryTag);
                if(message != null)
                {
                    message.discard();
                }
                return;
            }

            if (message.getMessage() == null)
            {
                _logger.warn("Message as already been purged, unable to Reject.");
                return;
            }


            if (_logger.isDebugEnabled())
            {
                _logger.debug("Rejecting: DT:" + deliveryTag + "-" + message.getMessage() +
                              ": Requeue:" + body.getRequeue() +
                              //": Resend:" + evt.getMethod().resend +
                              " on channel:" + channel.debugIdentity());
            }

            // If we haven't requested message to be resent to this consumer then reject it from ever getting it.
            //if (!evt.getMethod().resend)
            {
                /**
                 * Inform kernel that message has been rejected by AMQP transport
                 */
                try {
                    QpidAMQPBridge.getInstance().rejectMessage((AMQMessage) message.getMessage(), channel);
                } catch (AMQException e) {
                    _logger.error("Error while rejecting message by kernel" , e);
                    throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error while rejecting message by kernel", e);
                }
                message.reject();
            }

            if (body.getRequeue())
            {
                //todo: we need to honour this
                channel.requeue(deliveryTag);
            }
            else
            {
                _logger.warn("Dropping message as requeue not required and there is no dead letter queue");
                try {
                    MessagingEngine.getInstance().moveMessageToDeadLetterChannel(message.getMessage().getMessageNumber(),message.getQueue().getName());
                } catch (AndesException e) {
                    _logger.error("Error while moving message to DLC" , e);
                    throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error while moving message to DLC", e);
                }
            }
        }
    }
}
