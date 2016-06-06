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
import org.wso2.andes.framing.BasicGetBody;
import org.wso2.andes.framing.BasicGetEmptyBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.flow.FlowCreditManager;
import org.wso2.andes.server.flow.MessageOnlyCreditManager;
import org.wso2.andes.server.subscription.SubscriptionImpl;
import org.wso2.andes.server.subscription.ClientDeliveryMethod;
import org.wso2.andes.server.subscription.RecordDeliveryMethod;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionFactoryImpl;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.protocol.AMQSessionModel;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.virtualhost.VirtualHost;

public class BasicGetMethodHandler implements StateAwareMethodListener<BasicGetBody>
{
    private static final Logger _log = Logger.getLogger(BasicGetMethodHandler.class);

    private static final BasicGetMethodHandler _instance = new BasicGetMethodHandler();

    public static BasicGetMethodHandler getInstance()
    {
        return _instance;
    }

    private BasicGetMethodHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, BasicGetBody body, int channelId) throws AMQException
    {
        AMQProtocolSession protocolConnection = stateManager.getProtocolSession();


        VirtualHost vHost = protocolConnection.getVirtualHost();

        AMQChannel channel = protocolConnection.getChannel(channelId);
        if (channel == null)
        {
            throw body.getChannelNotFoundException(channelId);
        }
        else
        {
            AMQQueue queue = body.getQueue() == null ? channel.getDefaultQueue() : vHost.getQueueRegistry().getQueue(body.getQueue());
            if (queue == null)
            {
                _log.warn("No queue for '" + body.getQueue() + "'");
                if(body.getQueue()!=null)
                {
                    throw body.getConnectionException(AMQConstant.NOT_FOUND,
                                                      "No such queue, '" + body.getQueue()+ "'");
                }
                else
                {
                    throw body.getConnectionException(AMQConstant.NOT_ALLOWED,
                                                      "No queue name provided, no default queue defined.");
                }
            }
            else
            {
                if (queue.isExclusive())
                {
                    AMQSessionModel session = queue.getExclusiveOwningSession();
                    if (session == null || session.getConnectionModel() != protocolConnection)
                    {
                        throw body.getConnectionException(AMQConstant.NOT_ALLOWED,
                                                          "Queue is exclusive, but not created on this Connection.");
                    }
                }

                if (!performGet(queue,protocolConnection, channel, !body.getNoAck()))
                {
                    MethodRegistry methodRegistry = protocolConnection.getMethodRegistry();
                    // TODO - set clusterId
                    BasicGetEmptyBody responseBody = methodRegistry.createBasicGetEmptyBody(null);


                    protocolConnection.writeFrame(responseBody.generateFrame(channelId));
                }
            }
        }
    }

    public static boolean performGet(final AMQQueue queue,
                                     final AMQProtocolSession session,
                                     final AMQChannel channel,
                                     final boolean acks)
            throws AMQException
    {

        final FlowCreditManager singleMessageCredit = new MessageOnlyCreditManager(1L);

        final ClientDeliveryMethod getDeliveryMethod = new ClientDeliveryMethod()
        {

            int _msg;

            public void deliverToClient(final Subscription sub, final QueueEntry entry, final long deliveryTag)
            throws AMQException
            {
                singleMessageCredit.useCreditForMessage(entry.getMessage());
                if(entry.getMessage() instanceof AMQMessage)
                {
                    session.getProtocolOutputConverter().writeGetOk(entry, channel.getChannelId(),
                                                                            deliveryTag, queue.getMessageCount());
                }
                else
                {
                    //TODO Convert AMQP 0-10 message
                    throw new AMQException(AMQConstant.NOT_IMPLEMENTED, "Not implemented conversion of 0-10 message", null);
                }

            }
        };
        final RecordDeliveryMethod getRecordMethod = new RecordDeliveryMethod()
        {

            public void recordMessageDelivery(final Subscription sub, final QueueEntry entry, final long deliveryTag)
            {
                channel.addUnacknowledgedMessage(entry, deliveryTag, null);
            }
        };

        Subscription sub;
        if(acks)
        {
            sub = SubscriptionFactoryImpl.INSTANCE.createSubscription(channel, session, null, acks, null, false, singleMessageCredit, getDeliveryMethod, getRecordMethod);
        }
        else
        {
            sub = new GetNoAckSubscription(channel,
                                                 session,
                                                 null,
                                                 null,
                                                 false,
                                                 singleMessageCredit,
                                                 getDeliveryMethod,
                                                 getRecordMethod);
        }

        queue.registerSubscription(sub,false, session.getAuthorizedSubject());
        queue.flushSubscription(sub);
        queue.unregisterSubscription(sub);
        return(!singleMessageCredit.hasCredit());


    }

    public static final class GetNoAckSubscription extends SubscriptionImpl.NoAckSubscription
    {
        public GetNoAckSubscription(AMQChannel channel, AMQProtocolSession protocolSession,
                               AMQShortString consumerTag, FieldTable filters,
                               boolean noLocal, FlowCreditManager creditManager,
                                   ClientDeliveryMethod deliveryMethod,
                                   RecordDeliveryMethod recordMethod)
            throws AMQException
        {
            super(channel, protocolSession, consumerTag, filters, noLocal, creditManager, deliveryMethod, recordMethod);
        }

        public boolean isTransient()
        {
            return true;
        }

        public boolean wouldSuspend(QueueEntry msg)
        {
            return !getCreditManager().useCreditForMessage(msg.getMessage());
        }

    }
}
