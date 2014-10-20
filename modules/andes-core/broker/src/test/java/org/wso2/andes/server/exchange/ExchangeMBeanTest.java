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
package org.wso2.andes.server.exchange;

import org.wso2.andes.management.common.mbeans.ManagedExchange;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.AMQQueueFactory;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.registry.IApplicationRegistry;
import org.wso2.andes.server.management.ManagedObject;
import org.wso2.andes.server.util.InternalBrokerBaseCase;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.exchange.ExchangeDefaults;
import org.wso2.andes.framing.AMQShortString;

import javax.management.openmbean.TabularData;
import java.util.ArrayList;

/**
 * Unit test class for testing different Exchange MBean operations
 */
public class ExchangeMBeanTest  extends InternalBrokerBaseCase
{
    private AMQQueue _queue;
    private QueueRegistry _queueRegistry;
    private VirtualHost _virtualHost;

    /**
     * Test for direct exchange mbean
     * @throws Exception
     */

    public void testDirectExchangeMBean() throws Exception
    {
        DirectExchange exchange = new DirectExchange();
        exchange.initialise(_virtualHost, ExchangeDefaults.DIRECT_EXCHANGE_NAME, false, 0, true);
        ManagedObject managedObj = exchange.getManagedObject();
        ManagedExchange mbean = (ManagedExchange)managedObj;

        mbean.createNewBinding(_queue.getNameShortString().toString(), "binding1");
        mbean.createNewBinding(_queue.getNameShortString().toString(), "binding2");

        TabularData data = mbean.bindings();
        ArrayList<Object> list = new ArrayList<Object>(data.values());
        assertTrue(list.size() == 2);

        // test general exchange properties
        assertEquals(mbean.getName(), "amq.direct");
        assertEquals(mbean.getExchangeType(), "direct");
        assertTrue(mbean.getTicketNo() == 0);
        assertTrue(!mbean.isDurable());
        assertTrue(mbean.isAutoDelete());
    }

    /**
     * Test for "topic" exchange mbean
     * @throws Exception
     */

    public void testTopicExchangeMBean() throws Exception
    {
        TopicExchange exchange = new TopicExchange();
        exchange.initialise(_virtualHost,ExchangeDefaults.TOPIC_EXCHANGE_NAME, false, 0, true);
        ManagedObject managedObj = exchange.getManagedObject();
        ManagedExchange mbean = (ManagedExchange)managedObj;

        mbean.createNewBinding(_queue.getNameShortString().toString(), "binding1");
        mbean.createNewBinding(_queue.getNameShortString().toString(), "binding2");

        TabularData data = mbean.bindings();
        ArrayList<Object> list = new ArrayList<Object>(data.values());
        assertTrue(list.size() == 2);

        // test general exchange properties
        assertEquals(mbean.getName(), "amq.topic");
        assertEquals(mbean.getExchangeType(), "topic");
        assertTrue(mbean.getTicketNo() == 0);
        assertTrue(!mbean.isDurable());
        assertTrue(mbean.isAutoDelete());
    }

    /**
     * Test for "Headers" exchange mbean
     * @throws Exception
     */

    public void testHeadersExchangeMBean() throws Exception
    {
        HeadersExchange exchange = new HeadersExchange();
        exchange.initialise(_virtualHost,ExchangeDefaults.HEADERS_EXCHANGE_NAME, false, 0, true);
        ManagedObject managedObj = exchange.getManagedObject();
        ManagedExchange mbean = (ManagedExchange)managedObj;

        mbean.createNewBinding(_queue.getNameShortString().toString(), "key1=binding1,key2=binding2");
        mbean.createNewBinding(_queue.getNameShortString().toString(), "key3=binding3");

        TabularData data = mbean.bindings();
        ArrayList<Object> list = new ArrayList<Object>(data.values());
        assertTrue(list.size() == 2);

        // test general exchange properties
        assertEquals(mbean.getName(), "amq.match");
        assertEquals(mbean.getExchangeType(), "headers");
        assertTrue(mbean.getTicketNo() == 0);
        assertTrue(!mbean.isDurable());
        assertTrue(mbean.isAutoDelete());
    }
    
    /**
     * Test adding bindings and removing them from the default exchange via JMX.
     * <p>
     * QPID-2700
     */
    public void testDefaultBindings() throws Exception
    {
        int bindings = _queue.getBindingCount();
        
        Exchange exchange = _queue.getVirtualHost().getExchangeRegistry().getDefaultExchange();
        ManagedExchange mbean = (ManagedExchange) ((AbstractExchange) exchange).getManagedObject();
        
        mbean.createNewBinding(_queue.getName(), "robot");
        mbean.createNewBinding(_queue.getName(), "kitten");

        assertEquals("Should have added two bindings", bindings + 2, _queue.getBindingCount());
        
        mbean.removeBinding(_queue.getName(), "robot");

        assertEquals("Should have one extra binding", bindings + 1, _queue.getBindingCount());
        
        mbean.removeBinding(_queue.getName(), "kitten");

        assertEquals("Should have original number of binding", bindings, _queue.getBindingCount());
    }
    
    /**
     * Test adding bindings and removing them from the topic exchange via JMX.
     * <p>
     * QPID-2700
     */
    public void testTopicBindings() throws Exception
    {
        int bindings = _queue.getBindingCount();
        
        Exchange exchange = _queue.getVirtualHost().getExchangeRegistry().getExchange(new AMQShortString("amq.topic"));
        ManagedExchange mbean = (ManagedExchange) ((AbstractExchange) exchange).getManagedObject();
        
        mbean.createNewBinding(_queue.getName(), "robot.#");
        mbean.createNewBinding(_queue.getName(), "#.kitten");

        assertEquals("Should have added two bindings", bindings + 2, _queue.getBindingCount());
        
        mbean.removeBinding(_queue.getName(), "robot.#");

        assertEquals("Should have one extra binding", bindings + 1, _queue.getBindingCount());
        
        mbean.removeBinding(_queue.getName(), "#.kitten");

        assertEquals("Should have original number of binding", bindings, _queue.getBindingCount());
    }

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        IApplicationRegistry applicationRegistry = ApplicationRegistry.getInstance();
        _virtualHost = applicationRegistry.getVirtualHostRegistry().getVirtualHost("test");
        _queueRegistry = _virtualHost.getQueueRegistry();
        _queue = AMQQueueFactory.createAMQQueueImpl(new AMQShortString("testQueue"), false, new AMQShortString("ExchangeMBeanTest"), false, false,
                                                    _virtualHost, null);
        _queueRegistry.registerQueue(_queue);
    }
}
