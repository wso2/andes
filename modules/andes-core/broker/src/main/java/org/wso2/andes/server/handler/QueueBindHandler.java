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
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.framing.QueueBindBody;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.exchange.ExchangeRegistry;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.protocol.AMQSessionModel;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.virtualhost.VirtualHost;

import java.util.Map;

public class QueueBindHandler implements StateAwareMethodListener<QueueBindBody>
{
    private static final Logger _log = Logger.getLogger(QueueBindHandler.class);

    private static final QueueBindHandler _instance = new QueueBindHandler();

    public static QueueBindHandler getInstance()
    {
        return _instance;
    }

    private QueueBindHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, QueueBindBody body, int channelId) throws AMQException
    {
        AMQProtocolSession protocolConnection = stateManager.getProtocolSession();
        VirtualHost virtualHost = protocolConnection.getVirtualHost();
        ExchangeRegistry exchangeRegistry = virtualHost.getExchangeRegistry();
        QueueRegistry queueRegistry = virtualHost.getQueueRegistry();

        final AMQQueue queue;
        final AMQShortString routingKey;

        if (body.getQueue() == null)
        {
            AMQChannel channel = protocolConnection.getChannel(channelId);

            if (channel == null)
            {
                throw body.getChannelNotFoundException(channelId);
            }

            queue = channel.getDefaultQueue();

            if (queue == null)
            {
                throw body.getChannelException(AMQConstant.NOT_FOUND, "No default queue defined on channel and queue was null");
            }

            if (body.getRoutingKey() == null)
            {
                routingKey = queue.getNameShortString();
            }
            else
            {
                routingKey = body.getRoutingKey().intern();
            }
        }
        else
        {
            queue = queueRegistry.getQueue(body.getQueue());
            routingKey = body.getRoutingKey() == null ? AMQShortString.EMPTY_STRING : body.getRoutingKey().intern();
        }

        if (queue == null)
        {
            throw body.getChannelException(AMQConstant.NOT_FOUND, "Queue " + body.getQueue() + " does not exist.");
        }
        final Exchange exch = exchangeRegistry.getExchange(body.getExchange());
        if (exch == null)
        {
            throw body.getChannelException(AMQConstant.NOT_FOUND, "Exchange " + body.getExchange() + " does not exist.");
        }


        try
        {
            if (queue.isExclusive() && !queue.isDurable())
            {
                AMQSessionModel session = queue.getExclusiveOwningSession();
                if (session == null || session.getConnectionModel() != protocolConnection)
                {
                    throw body.getConnectionException(AMQConstant.NOT_ALLOWED,
                        "Queue " + queue.getNameShortString() + " is exclusive, but not created on this Connection.");
                }
            }

            if (!exch.isBound(routingKey, body.getArguments(), queue))
            {
                String bindingKey = String.valueOf(routingKey);
                Map<String,Object> arguments = FieldTable.convertToMap(body.getArguments());

                if(!virtualHost.getBindingFactory().addBinding(bindingKey, queue, exch, arguments))
                {
                    Binding oldBinding = virtualHost.getBindingFactory().getBinding(bindingKey, queue, exch, arguments);

                    Map<String, Object> oldArgs = oldBinding.getArguments();
                    if((oldArgs == null && !arguments.isEmpty()) || (oldArgs != null && !oldArgs.equals(arguments)))
                    {
                        virtualHost.getBindingFactory().replaceBinding(bindingKey, queue, exch, arguments);    
                    }
                }
            }
        }
        catch (AMQException e)
        {
            throw body.getChannelException(AMQConstant.CHANNEL_ERROR, e.toString());
        }

        if (_log.isInfoEnabled())
        {
            _log.info("Binding queue " + queue + " to exchange " + exch + " with routing key " + routingKey);
        }
        if (!body.getNowait())
        {
            MethodRegistry methodRegistry = protocolConnection.getMethodRegistry();
            AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
            protocolConnection.writeFrame(responseBody.generateFrame(channelId));

        }
    }
}
