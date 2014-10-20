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
import org.wso2.andes.framing.QueueUnbindBody;
import org.wso2.andes.framing.amqp_0_9.MethodRegistry_0_9;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.exchange.ExchangeRegistry;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.virtualhost.VirtualHost;

public class QueueUnbindHandler implements StateAwareMethodListener<QueueUnbindBody>
{
    private static final Logger _log = Logger.getLogger(QueueUnbindHandler.class);

    private static final QueueUnbindHandler _instance = new QueueUnbindHandler();

    public static QueueUnbindHandler getInstance()
    {
        return _instance;
    }

    private QueueUnbindHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, QueueUnbindBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();
        VirtualHost virtualHost = session.getVirtualHost();
        ExchangeRegistry exchangeRegistry = virtualHost.getExchangeRegistry();
        QueueRegistry queueRegistry = virtualHost.getQueueRegistry();


        final AMQQueue queue;
        final AMQShortString routingKey;               

        if (body.getQueue() == null)
        {
            AMQChannel channel = session.getChannel(channelId);

            if (channel == null)
            {
                throw body.getChannelNotFoundException(channelId);
            }

            queue = channel.getDefaultQueue();

            if (queue == null)
            {
                throw body.getChannelException(AMQConstant.NOT_FOUND, "No default queue defined on channel and queue was null");
            }

            routingKey = body.getRoutingKey() == null ? null : body.getRoutingKey().intern();

        }
        else
        {
            queue = queueRegistry.getQueue(body.getQueue());
            routingKey = body.getRoutingKey() == null ? null : body.getRoutingKey().intern();
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

        if(virtualHost.getBindingFactory().getBinding(String.valueOf(routingKey), queue, exch, FieldTable.convertToMap(body.getArguments())) == null)
        {
            throw body.getChannelException(AMQConstant.NOT_FOUND,"No such binding");
        }
        else
        {
            virtualHost.getBindingFactory().removeBinding(String.valueOf(routingKey), queue, exch, FieldTable.convertToMap(body.getArguments()));
        }

        
        if (_log.isInfoEnabled())
        {
            _log.info("Binding queue " + queue + " to exchange " + exch + " with routing key " + routingKey);
        }

        MethodRegistry_0_9 methodRegistry = (MethodRegistry_0_9) session.getMethodRegistry();
        AMQMethodBody responseBody = methodRegistry.createQueueUnbindOkBody();
        session.writeFrame(responseBody.generateFrame(channelId));


    }
}
