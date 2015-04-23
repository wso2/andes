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
import org.wso2.andes.amqp.QpidAMQPBridge;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.framing.QueueDeclareBody;
import org.wso2.andes.framing.QueueDeclareOkBody;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.exchange.ExchangeRegistry;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.protocol.AMQSessionModel;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.AMQQueueFactory;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.store.DurableConfigurationStore;
import org.wso2.andes.server.virtualhost.VirtualHost;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.wso2.andes.amqp.AMQPUtils.generateQueueName;

public class QueueDeclareHandler implements StateAwareMethodListener<QueueDeclareBody>
{
    private static final Logger _logger = Logger.getLogger(QueueDeclareHandler.class);

    private static final QueueDeclareHandler _instance = new QueueDeclareHandler();

    public static QueueDeclareHandler getInstance()
    {
        return _instance;
    }

    public boolean autoRegister = ApplicationRegistry.getInstance().getConfiguration().getQueueAutoRegister();

    private final AtomicInteger _counter = new AtomicInteger();

    public void methodReceived(AMQStateManager stateManager, QueueDeclareBody body, int channelId) throws AMQException
    {
        final AMQProtocolSession protocolConnection = stateManager.getProtocolSession();
        final AMQSessionModel session = protocolConnection.getChannel(channelId);
        VirtualHost virtualHost = protocolConnection.getVirtualHost();
        ExchangeRegistry exchangeRegistry = virtualHost.getExchangeRegistry();
        QueueRegistry queueRegistry = virtualHost.getQueueRegistry();
        DurableConfigurationStore store = virtualHost.getDurableConfigurationStore();

        final AMQShortString queueName;

        // if we aren't given a queue name, we create one which we return to the client
        if ((body.getQueue() == null) || (body.getQueue().length() == 0))
        {
            queueName = createName();
        }
        else
        {
            queueName = body.getQueue().intern();
        }

        AMQQueue queue;
        
        //TODO: do we need to check that the queue already exists with exactly the same "configuration"?

        synchronized (queueRegistry)
        {
            queue = queueRegistry.getQueue(queueName);

            AMQSessionModel owningSession = null;

            if (queue != null)
            {
                owningSession = queue.getExclusiveOwningSession();
            }

            if (queue == null)
            {
                if (body.getPassive())
                {
                    String msg = "Queue: " + queueName + " not found on VirtualHost(" + virtualHost + ").";
                    throw body.getChannelException(AMQConstant.NOT_FOUND, msg);
                }
                else
                {
                    queue = createQueue(queueName, body, virtualHost, protocolConnection);
                    queue.setAuthorizationHolder(protocolConnection);
                    if (queue.isDurable() && !queue.isAutoDelete())
                    {
                        store.createQueue(queue, body.getArguments());

                        //Tell Andes kernel to create queue
                        QpidAMQPBridge.getInstance().createQueue(queue, false);
                    }
                    if(body.getAutoDelete())
                    {
                        queue.setDeleteOnNoConsumers(true);
                    }
                    queueRegistry.registerQueue(queue);
                    if (body.getExclusive())
                    {
                        queue.setExclusiveOwningSession(protocolConnection.getChannel(channelId));
                        queue.setAuthorizationHolder(protocolConnection);

                        if(!body.getDurable())
                        {
                            final AMQQueue q = queue;
                            final AMQProtocolSession.Task sessionCloseTask = new AMQProtocolSession.Task()
                            {
                                public void doTask(AMQProtocolSession session) throws AMQException
                                {
                                    q.setExclusiveOwningSession(null);
                                }
                            };
                            protocolConnection.addSessionCloseTask(sessionCloseTask);
                            queue.addQueueDeleteTask(new AMQQueue.Task() {
                                public void doTask(AMQQueue queue) throws AMQException
                                {
                                    protocolConnection.removeSessionCloseTask(sessionCloseTask);
                                }
                            });
                        }
                    }
                    if (autoRegister)
                    {
                        Exchange defaultExchange = exchangeRegistry.getDefaultExchange();

                        virtualHost.getBindingFactory().addBinding(String.valueOf(queueName), queue, defaultExchange, Collections.EMPTY_MAP);
                        _logger.info("Queue " + queueName + " bound to default exchange(" + defaultExchange.getNameShortString() + ")");
                    }
                }
            }
            else if (queue.isExclusive() && !queue.isDurable() && (owningSession == null || owningSession.getConnectionModel() != protocolConnection))
            {
                throw body.getConnectionException(AMQConstant.NOT_ALLOWED,
                                                  "Queue " + queue.getNameShortString() + " is exclusive, but not created on this Connection.");
            }
            else if(!body.getPassive() && ((queue.isExclusive()) != body.getExclusive()))
            {

                throw body.getChannelException(AMQConstant.ALREADY_EXISTS,
                                                  "Cannot re-declare queue '" + queue.getNameShortString() + "' with different exclusivity (was: "
                                                    + queue.isExclusive() + " requested " + body.getExclusive() + ")");
            }
            else if (!body.getPassive() && body.getExclusive() && !(queue.isDurable() ? String.valueOf(queue.getOwner()).equals(session.getClientID()) : (owningSession == null || owningSession.getConnectionModel() == protocolConnection)))
            {
                throw body.getChannelException(AMQConstant.ALREADY_EXISTS, "Cannot declare queue('" + queueName + "'), "
                                                                           + "as exclusive queue with same name "
                                                                           + "declared on another client ID('"
                                                                           + queue.getOwner() + "') your clientID('" + session.getClientID() + "')");

            }
            else if(!body.getPassive() && queue.isAutoDelete() != body.getAutoDelete())
            {
                throw body.getChannelException(AMQConstant.ALREADY_EXISTS,
                                                  "Cannot re-declare queue '" + queue.getNameShortString() + "' with different auto-delete (was: "
                                                    + queue.isAutoDelete() + " requested " + body.getAutoDelete() + ")");
            }
            else if(!body.getPassive() && queue.isDurable() != body.getDurable())
            {
                throw body.getChannelException(AMQConstant.ALREADY_EXISTS,
                                                  "Cannot re-declare queue '" + queue.getNameShortString() + "' with different durability (was: "
                                                    + queue.isDurable() + " requested " + body.getDurable() + ")");
            }


            AMQChannel channel = protocolConnection.getChannel(channelId);

            if (channel == null)
            {
                throw body.getChannelNotFoundException(channelId);
            }

            //set this as the default queue on the channel:
            channel.setDefaultQueue(queue);
        }

        if (!body.getNowait())
        {
            MethodRegistry methodRegistry = protocolConnection.getMethodRegistry();
            QueueDeclareOkBody responseBody =
                    methodRegistry.createQueueDeclareOkBody(queueName,
                                                            queue.getMessageCount(),
                                                            queue.getConsumerCount());
            protocolConnection.writeFrame(responseBody.generateFrame(channelId));

            _logger.info("Queue " + queueName + " declared successfully");
        }
    }

    protected AMQShortString createName()
    {
        return new AMQShortString(generateQueueName());
    }

    protected AMQQueue createQueue(final AMQShortString queueName,
                                   QueueDeclareBody body,
                                   VirtualHost virtualHost,
                                   final AMQProtocolSession session)
            throws AMQException
    {
        final QueueRegistry registry = virtualHost.getQueueRegistry();
        AMQShortString owner = body.getExclusive() ? session.getContextKey() : null;

        final AMQQueue queue = AMQQueueFactory.createAMQQueueImpl(queueName, body.getDurable(), owner, body.getAutoDelete(),
                                                                  body.getExclusive(),virtualHost, body.getArguments());

        if (body.getExclusive() && !body.getDurable())
        {
            final AMQProtocolSession.Task deleteQueueTask =
                    new AMQProtocolSession.Task()
                    {
                        public void doTask(AMQProtocolSession session) throws AMQException
                        {
                            if (registry.getQueue(queueName) == queue)
                            {
                                queue.delete();
                            }
                        }
                    };

            session.addSessionCloseTask(deleteQueueTask);

            queue.addQueueDeleteTask(new AMQQueue.Task()
            {
                public void doTask(AMQQueue queue)
                {
                    session.removeSessionCloseTask(deleteQueueTask);
                }
            });
        }

        return queue;
    }
}
