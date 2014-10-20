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
package org.wso2.andes.client;

import java.net.URISyntaxException;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;

import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.messaging.Address;
import org.wso2.andes.url.BindingURL;

/**
 * In order to support JMS 1.0 the Qpid implementation maps the 
 * direct exchange to JMS Queue and topic exchange to JMS Topic.
 * 
 * The JMS 1.1 spec provides a javax.Destination as an abstraction
 * to represent any type of destination. 
 * The abstract class AMQDestination has most of the functionality
 * to support any destination defined in AMQP 0-10 spec.
 */
public class AMQAnyDestination extends AMQDestination implements Queue, Topic
{    
    public AMQAnyDestination(BindingURL binding)
    {
        super(binding);
    }

    public AMQAnyDestination(String str) throws URISyntaxException
    {
        super(str);
    }
    
    public AMQAnyDestination(Address addr) throws Exception
    {
        super(addr);
    }
    
    public AMQAnyDestination(AMQShortString exchangeName,AMQShortString exchangeClass,
                             AMQShortString routingKey,boolean isExclusive, 
                             boolean isAutoDelete, AMQShortString queueName, 
                             boolean isDurable, AMQShortString[] bindingKeys)
    {
        super(exchangeName, exchangeClass, routingKey, isExclusive, isAutoDelete, queueName, isDurable, bindingKeys);
    }

    @Override
    public boolean isNameRequired()
    {
        return getAMQQueueName() == null;
    }

    public String getTopicName() throws JMSException
    {
        if (getRoutingKey() != null)
        {
            return getRoutingKey().asString();
        }
        else if (getSubject() != null)
        {
            return getSubject();
        }
        else
        {
            return null;
        }
    }
}
