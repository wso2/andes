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
package org.wso2.andes.server.exchange;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQSecurityException;
import org.wso2.andes.amqp.QpidAMQPBridge;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.server.queue.IncomingMessage;
import org.wso2.andes.server.store.DurableConfigurationStore;
import org.wso2.andes.server.virtualhost.VirtualHost;

public class DefaultExchangeRegistry implements ExchangeRegistry
{
    private static final Logger _log = Logger.getLogger(DefaultExchangeRegistry.class);

    /**
     * Maps from exchange name to exchange instance
     */
    private ConcurrentMap<AMQShortString, Exchange> _exchangeMap = new ConcurrentHashMap<AMQShortString, Exchange>();
    private ConcurrentMap<String, Exchange> _exchangeMapStr = new ConcurrentHashMap<String, Exchange>();

    private Exchange _defaultExchange;
    private VirtualHost _host;

    public DefaultExchangeRegistry(VirtualHost host)
    {
        //create 'standard' exchanges:
        _host = host;

    }

    public void initialise() throws AMQException
    {
        new ExchangeInitialiser().initialise(_host.getExchangeFactory(), this, getDurableConfigurationStore());
    }



    public DurableConfigurationStore getDurableConfigurationStore()
    {
        return _host.getDurableConfigurationStore();
    }

    public void registerExchange(Exchange exchange) throws AMQException
    {
        _exchangeMap.put(exchange.getNameShortString(), exchange);
        _exchangeMapStr.put(exchange.getNameShortString().toString(), exchange);
    }

    public void setDefaultExchange(Exchange exchange)
    {
        _defaultExchange = exchange;
    }

    public Exchange getDefaultExchange()
    {
        return _defaultExchange;
    }

    public Collection<AMQShortString> getExchangeNames()
    {
        return _exchangeMap.keySet();
    }

    public void unregisterExchange(AMQShortString name, boolean inUse) throws AMQException
    {
        // Check access
        if (!_host.getSecurityManager().authoriseDelete(_exchangeMap.get(name)))
        {
            throw new AMQSecurityException();
        }
        
        // TODO: check inUse argument

        Exchange e = _exchangeMap.remove(name);
        _exchangeMapStr.remove(name.toString());
        if (e != null)
        {
            if (e.isDurable())
            {
                getDurableConfigurationStore().removeExchange(e);

                //tell Andes Kernel to remove exchange
                QpidAMQPBridge.getInstance().deleteExchange(e);
            }
            e.close();
        }
        else
        {
            throw new AMQException("Unknown exchange " + name);
        }
    }

    public void unregisterExchange(String name, boolean inUse) throws AMQException
    {
        unregisterExchange(new AMQShortString(name), inUse);
    }

    public Exchange getExchange(AMQShortString name)
    {
        if ((name == null) || name.length() == 0)
        {
            return getDefaultExchange();
        }
        else
        {
            return _exchangeMap.get(name);
        }

    }

    public Exchange getExchange(String name)
    {
        if ((name == null) || name.length() == 0)
        {
            return getDefaultExchange();
        }
        else
        {
            return _exchangeMapStr.get(name);
        }
    }


    /**
     * Routes content through exchanges, delivering it to 1 or more queues.
     * @param payload
     * @throws AMQException if something goes wrong delivering data
     */
    public void routeContent(IncomingMessage payload) throws AMQException
    {
        final AMQShortString exchange = payload.getExchange();
        final Exchange exch = getExchange(exchange);
        // there is a small window of opportunity for the exchange to be deleted in between
        // the BasicPublish being received (where the exchange is validated) and the final
        // content body being received (which triggers this method)
        // TODO: check where the exchange is validated
        if (exch == null)
        {
            throw new AMQException("Exchange '" + exchange + "' does not exist");
        }
        payload.enqueue(exch.route(payload));
    }
}
