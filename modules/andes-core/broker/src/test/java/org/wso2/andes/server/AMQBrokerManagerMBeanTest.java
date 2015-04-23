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
package org.wso2.andes.server;

import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.management.common.mbeans.ManagedBroker;
import org.wso2.andes.server.exchange.ExchangeRegistry;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.registry.IApplicationRegistry;
import org.wso2.andes.server.util.InternalBrokerBaseCase;
import org.wso2.andes.server.virtualhost.VirtualHostImpl;
import org.wso2.andes.server.virtualhost.VirtualHost;

public class AMQBrokerManagerMBeanTest extends InternalBrokerBaseCase
{
    private QueueRegistry _queueRegistry;
    private ExchangeRegistry _exchangeRegistry;
    private VirtualHost _vHost;

    public void testExchangeOperations() throws Exception
    {
        String exchange1 = "testExchange1_" + System.currentTimeMillis();
        String exchange2 = "testExchange2_" + System.currentTimeMillis();
        String exchange3 = "testExchange3_" + System.currentTimeMillis();

        assertTrue(_exchangeRegistry.getExchange(new AMQShortString(exchange1)) == null);
        assertTrue(_exchangeRegistry.getExchange(new AMQShortString(exchange2)) == null);
        assertTrue(_exchangeRegistry.getExchange(new AMQShortString(exchange3)) == null);


        ManagedBroker mbean = new AMQBrokerManagerMBean((VirtualHostImpl.VirtualHostMBean) _vHost.getManagedObject());
        mbean.createNewExchange(exchange1, "direct", false);
        mbean.createNewExchange(exchange2, "topic", false);
        mbean.createNewExchange(exchange3, "headers", false);

        assertTrue(_exchangeRegistry.getExchange(new AMQShortString(exchange1)) != null);
        assertTrue(_exchangeRegistry.getExchange(new AMQShortString(exchange2)) != null);
        assertTrue(_exchangeRegistry.getExchange(new AMQShortString(exchange3)) != null);

        mbean.unregisterExchange(exchange1);
        mbean.unregisterExchange(exchange2);
        mbean.unregisterExchange(exchange3);

        assertTrue(_exchangeRegistry.getExchange(new AMQShortString(exchange1)) == null);
        assertTrue(_exchangeRegistry.getExchange(new AMQShortString(exchange2)) == null);
        assertTrue(_exchangeRegistry.getExchange(new AMQShortString(exchange3)) == null);
    }

    public void testQueueOperations() throws Exception
    {
        String queueName = "testQueue_" + System.currentTimeMillis();

        ManagedBroker mbean = new AMQBrokerManagerMBean((VirtualHostImpl.VirtualHostMBean) _vHost.getManagedObject());

        assertTrue(_queueRegistry.getQueue(new AMQShortString(queueName)) == null);

        mbean.createNewQueue(queueName, "test", false, false);
        assertTrue(_queueRegistry.getQueue(new AMQShortString(queueName)) != null);

        mbean.deleteQueue(queueName);
        assertTrue(_queueRegistry.getQueue(new AMQShortString(queueName)) == null);
    }

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        IApplicationRegistry appRegistry = ApplicationRegistry.getInstance();
        _vHost = appRegistry.getVirtualHostRegistry().getVirtualHost("test");
        _queueRegistry = _vHost.getQueueRegistry();
        _exchangeRegistry = _vHost.getExchangeRegistry();
    }

}
