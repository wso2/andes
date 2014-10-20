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

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQMethodBody;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.BasicConsumeBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.protocol.AMQSessionModel;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.virtualhost.VirtualHost;

public class BasicConsumeMethodHandler implements StateAwareMethodListener<BasicConsumeBody>
{
    private static final Logger _logger = Logger.getLogger(BasicConsumeMethodHandler.class);

    private static final BasicConsumeMethodHandler _instance = new BasicConsumeMethodHandler();

    public static BasicConsumeMethodHandler getInstance()
    {
        return _instance;
    }

    private BasicConsumeMethodHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, BasicConsumeBody body, int channelId) throws AMQException
    {
        AMQProtocolSession protocolConnection = stateManager.getProtocolSession();

        AMQChannel channel = protocolConnection.getChannel(channelId);

        VirtualHost vHost = protocolConnection.getVirtualHost();

        if (channel == null)
        {
            throw body.getChannelNotFoundException(channelId);
        }
        else
        {
            if (_logger.isDebugEnabled())
            {
                _logger.debug("BasicConsume: from '" + body.getQueue() +
                              "' for:" + body.getConsumerTag() +
                              " nowait:" + body.getNowait() +
                              " args:" + body.getArguments());
            }

            AMQQueue queue = body.getQueue() == null ? channel.getDefaultQueue() : vHost.getQueueRegistry().getQueue(body.getQueue().intern());

            if (queue == null)
            {
                if (_logger.isDebugEnabled())
                {
                    _logger.debug("No queue for '" + body.getQueue() + "'");
                }
                if (body.getQueue() != null)
                {
                    String msg = "No such queue, '" + body.getQueue() + "'";
                    throw body.getChannelException(AMQConstant.NOT_FOUND, msg);
                }
                else
                {
                    String msg = "No queue name provided, no default queue defined.";
                    throw body.getConnectionException(AMQConstant.NOT_ALLOWED, msg);
                }
            }
            else
            {
                final AMQShortString consumerTagName;

                if (queue.isExclusive() && !queue.isDurable())
                {
                    AMQSessionModel session = queue.getExclusiveOwningSession();
                    if (session == null || session.getConnectionModel() != protocolConnection)
                    {
                        throw body.getConnectionException(AMQConstant.NOT_ALLOWED,
                                                          "Queue " + queue.getNameShortString() + " is exclusive, but not created on this Connection.");
                    }
                }

                if (body.getConsumerTag() != null)
                {
                    consumerTagName = body.getConsumerTag().intern();
                }
                else
                {
                    consumerTagName = null;
                }

                try
                {
                    if(consumerTagName == null || channel.getSubscription(consumerTagName) == null)
                    {

                        AMQShortString consumerTag = channel.subscribeToQueue(consumerTagName, queue, !body.getNoAck(),
                                                                              body.getArguments(), body.getNoLocal(), body.getExclusive());
                        if (!body.getNowait())
                        {
                            MethodRegistry methodRegistry = protocolConnection.getMethodRegistry();
                            AMQMethodBody responseBody = methodRegistry.createBasicConsumeOkBody(consumerTag);
                            protocolConnection.writeFrame(responseBody.generateFrame(channelId));

                        }
                    }
                    else
                    {
                        AMQShortString msg = new AMQShortString("Non-unique consumer tag, '" + body.getConsumerTag() + "'");

                        MethodRegistry methodRegistry = protocolConnection.getMethodRegistry();
                        AMQMethodBody responseBody = methodRegistry.createConnectionCloseBody(AMQConstant.NOT_ALLOWED.getCode(),    // replyCode
                                                                 msg,               // replytext
                                                                 body.getClazz(),
                                                                 body.getMethod());
                        protocolConnection.writeFrame(responseBody.generateFrame(0));
                    }

                }
                catch (org.wso2.andes.AMQInvalidArgumentException ise)
                {
                    _logger.debug("Closing connection due to invalid selector");

                    MethodRegistry methodRegistry = protocolConnection.getMethodRegistry();
                    AMQMethodBody responseBody = methodRegistry.createChannelCloseBody(AMQConstant.INVALID_ARGUMENT.getCode(),
                                                                                       new AMQShortString(ise.getMessage()),
                                                                                       body.getClazz(),
                                                                                       body.getMethod());
                    protocolConnection.writeFrame(responseBody.generateFrame(channelId));


                }
                catch (AMQQueue.ExistingExclusiveSubscription e)
                {
                    throw body.getChannelException(AMQConstant.ACCESS_REFUSED,
                                                   "Cannot subscribe to queue "
                                                   + queue.getNameShortString()
                                                   + " as it already has an existing exclusive consumer");
                }
                catch (AMQQueue.ExistingSubscriptionPreventsExclusive e)
                {
                    throw body.getChannelException(AMQConstant.ACCESS_REFUSED,
                                                   "Cannot subscribe to queue "
                                                   + queue.getNameShortString()
                                                   + " exclusively as it already has a consumer");
                }

            }
        }
    }
}
