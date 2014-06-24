/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.server.handler;

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.exchange.ExchangeDefaults;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.BasicPublishBody;
import org.wso2.andes.framing.abstraction.MessagePublishInfo;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.virtualhost.VirtualHost;

public class BasicPublishMethodHandler implements StateAwareMethodListener<BasicPublishBody>
{
    private static final Logger _logger = Logger.getLogger(BasicPublishMethodHandler.class);

    private static final BasicPublishMethodHandler _instance = new BasicPublishMethodHandler();


    public static BasicPublishMethodHandler getInstance()
    {
        return _instance;
    }

    private BasicPublishMethodHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, BasicPublishBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();
        if (_logger.isDebugEnabled())
        {
            _logger.debug("Publish received on channel " + channelId);
        }

        AMQShortString exchangeName = body.getExchange();
        // TODO: check the delivery tag field details - is it unique across the broker or per subscriber?
        if (exchangeName == null)
        {
            exchangeName = ExchangeDefaults.DEFAULT_EXCHANGE_NAME;
        }

        VirtualHost vHost = session.getVirtualHost();
        Exchange exch = vHost.getExchangeRegistry().getExchange(exchangeName);
        // if the exchange does not exist we raise a channel exception
        if (exch == null)
        {
            throw body.getChannelException(AMQConstant.NOT_FOUND, "Unknown exchange name");
        }
        else
        {
            // The partially populated BasicDeliver frame plus the received route body
            // is stored in the channel. Once the final body frame has been received
            // it is routed to the exchange.
            AMQChannel channel = session.getChannel(channelId);

            if (channel == null)
            {
                throw body.getChannelNotFoundException(channelId);
            }

            MessagePublishInfo info = session.getMethodRegistry().getProtocolVersionMethodConverter().convertToInfo(body);
            info.setExchange(exchangeName);
            channel.setPublishFrame(info, exch);
        }
    }

}



