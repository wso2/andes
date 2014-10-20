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

import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.QpidAMQPBridge;
import org.wso2.andes.exchange.ExchangeDefaults;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.server.store.DurableConfigurationStore;

public class ExchangeInitialiser
{
    public void initialise(ExchangeFactory factory, ExchangeRegistry registry, DurableConfigurationStore store) throws AMQException{
        for (ExchangeType<? extends Exchange> type : factory.getRegisteredTypes())
        {
            define (registry, factory, type.getDefaultExchangeName(), type.getName(), store);
        }

        define(registry, factory, ExchangeDefaults.DEFAULT_EXCHANGE_NAME, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, store);
        registry.setDefaultExchange(registry.getExchange(ExchangeDefaults.DEFAULT_EXCHANGE_NAME));
    }

    private void define(ExchangeRegistry r, ExchangeFactory f,
                        AMQShortString name, AMQShortString type, DurableConfigurationStore store) throws AMQException
    {
        if(r.getExchange(name)== null)
        {
            Exchange exchange = f.createExchange(name, type, true, false, 0);
            r.registerExchange(exchange);

            if(exchange.isDurable())
            {
                store.createExchange(exchange);

                //tell Andes kernel to create Exchange
                QpidAMQPBridge.getInstance().createExchange(exchange);
            }
        }
    }
}
