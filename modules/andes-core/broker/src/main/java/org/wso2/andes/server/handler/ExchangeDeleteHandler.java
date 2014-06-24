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

import org.wso2.andes.AMQException;
import org.wso2.andes.framing.ExchangeDeleteBody;
import org.wso2.andes.framing.ExchangeDeleteOkBody;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.exchange.ExchangeInUseException;
import org.wso2.andes.server.exchange.ExchangeRegistry;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.virtualhost.VirtualHost;

public class ExchangeDeleteHandler implements StateAwareMethodListener<ExchangeDeleteBody>
{
    private static final ExchangeDeleteHandler _instance = new ExchangeDeleteHandler();

    public static ExchangeDeleteHandler getInstance()
    {
        return _instance;
    }

    private ExchangeDeleteHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, ExchangeDeleteBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();
        VirtualHost virtualHost = session.getVirtualHost();
        ExchangeRegistry exchangeRegistry = virtualHost.getExchangeRegistry();

        try
        {
            if(exchangeRegistry.getExchange(body.getExchange()) == null)
            {
                throw body.getChannelException(AMQConstant.NOT_FOUND, "No such exchange: " + body.getExchange());
            }
            exchangeRegistry.unregisterExchange(body.getExchange(), body.getIfUnused());

            ExchangeDeleteOkBody responseBody = session.getMethodRegistry().createExchangeDeleteOkBody();
                        
            session.writeFrame(responseBody.generateFrame(channelId));
        }
        catch (ExchangeInUseException e)
        {
            throw body.getChannelException(AMQConstant.IN_USE, "Exchange in use");
            // TODO: sort out consistent channel close mechanism that does all clean up etc.
        }
    }
}
