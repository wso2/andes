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

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQSecurityException;
import org.wso2.andes.AMQUnknownExchangeType;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.qmf.ManagementExchange;
import org.wso2.andes.server.configuration.VirtualHostConfiguration;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.virtualhost.VirtualHost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DefaultExchangeFactory implements ExchangeFactory
{
    private static final Logger _logger = Logger.getLogger(DefaultExchangeFactory.class);

    private Map<AMQShortString, ExchangeType<? extends Exchange>> _exchangeClassMap = new HashMap<AMQShortString, ExchangeType<? extends Exchange>>();
    private final VirtualHost _host;

    public DefaultExchangeFactory(VirtualHost host)
    {
        _host = host;
        registerExchangeType(DirectExchange.TYPE);
        registerExchangeType(TopicExchange.TYPE);
        registerExchangeType(HeadersExchange.TYPE);
        registerExchangeType(FanoutExchange.TYPE);
        registerExchangeType(ManagementExchange.TYPE);
    }

    public void registerExchangeType(ExchangeType<? extends Exchange> type)
    {
        _exchangeClassMap.put(type.getName(), type);
    }

    public Collection<ExchangeType<? extends Exchange>> getRegisteredTypes()
    {
        return _exchangeClassMap.values();
    }
    
    public Collection<ExchangeType<? extends Exchange>> getPublicCreatableTypes()
    {
        Collection<ExchangeType<? extends Exchange>> publicTypes = 
                                new ArrayList<ExchangeType<? extends Exchange>>();
        publicTypes.addAll(_exchangeClassMap.values());
        
        //Remove the ManagementExchange type if present, as these 
        //are private and cannot be created by external means
        publicTypes.remove(ManagementExchange.TYPE);
        
        return publicTypes;
    }
    
    

    public Exchange createExchange(String exchange, String type, boolean durable, boolean autoDelete)
            throws AMQException
    {
        return createExchange(new AMQShortString(exchange), new AMQShortString(type), durable, autoDelete, 0);
    }

    public Exchange createExchange(AMQShortString exchange, AMQShortString type, boolean durable, boolean autoDelete,
                                   int ticket)
            throws AMQException
    {
        // Check access
        if (!_host.getSecurityManager().authoriseCreateExchange(autoDelete, durable, exchange, null, null, null, type))
        {
            String description = "Permission denied: exchange-name '" + exchange.asString() + "'";
            throw new AMQSecurityException(description);
        }
        
        ExchangeType<? extends Exchange> exchType = _exchangeClassMap.get(type);
        if (exchType == null)
        {
            throw new AMQUnknownExchangeType("Unknown exchange type: " + type,null);
        }
        
        Exchange e = exchType.newInstance(_host, exchange, durable, ticket, autoDelete);
        return e;
    }

    public void initialise(VirtualHostConfiguration hostConfig)
    {

        if (hostConfig == null)
        {
            return;
        }

        for(Object className : hostConfig.getCustomExchanges())
        {
            try
            {
                ExchangeType<?> exchangeType = ApplicationRegistry.getInstance().getPluginManager().getExchanges().get(String.valueOf(className));
                if (exchangeType == null)
                {
                    _logger.error("No such custom exchange class found: \""+String.valueOf(className)+"\"");
                    return;
                }
                Class<? extends ExchangeType> exchangeTypeClass = exchangeType.getClass();
                ExchangeType<? extends ExchangeType> type = exchangeTypeClass.newInstance();
                registerExchangeType(type);
            }
            catch (ClassCastException classCastEx)
            {
                _logger.error("No custom exchange class: \""+String.valueOf(className)+"\" cannot be registered as it does not extend class \""+ExchangeType.class+"\"");
            }
            catch (IllegalAccessException e)
            {
                _logger.error("Cannot create custom exchange class: \""+String.valueOf(className)+"\"",e);
            }
            catch (InstantiationException e)
            {
                _logger.error("Cannot create custom exchange class: \""+String.valueOf(className)+"\"",e);
            }
        }

    }
}
