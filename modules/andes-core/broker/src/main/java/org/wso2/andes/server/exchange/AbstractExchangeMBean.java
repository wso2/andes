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

import java.util.Collections;
import java.util.Map;

import org.wso2.andes.AMQException;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.server.management.ManagedObject;
import org.wso2.andes.server.management.ManagedObjectRegistry;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.binding.BindingFactory;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.actors.ManagementActor;
import org.wso2.andes.management.common.mbeans.ManagedExchange;
import org.wso2.andes.framing.AMQShortString;

import javax.management.openmbean.*;
import javax.management.MBeanException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.MalformedObjectNameException;
import javax.management.JMException;

/**
     * Abstract MBean class. This has some of the methods implemented from
 * management intrerface for exchanges. Any implementaion of an
 * Exchange MBean should extend this class.
 */
public abstract class AbstractExchangeMBean<T extends AbstractExchange> extends AMQManagedObject implements ManagedExchange
{
    // open mbean data types for representing exchange bindings
    protected OpenType[] _bindingItemTypes;
    protected CompositeType _bindingDataType;
    protected TabularType _bindinglistDataType;

    private T _exchange;

    public AbstractExchangeMBean(final T abstractExchange) throws NotCompliantMBeanException
    {
        super(ManagedExchange.class, ManagedExchange.TYPE);
        _exchange = abstractExchange;
    }

    protected void init() throws OpenDataException
    {
        _bindingItemTypes = new OpenType[2];
        _bindingItemTypes[0] = SimpleType.STRING;
        _bindingItemTypes[1] = new ArrayType(1, SimpleType.STRING);
        _bindingDataType = new CompositeType("Exchange Binding", "Binding key and Queue names",
                COMPOSITE_ITEM_NAMES.toArray(new String[COMPOSITE_ITEM_NAMES.size()]),
                COMPOSITE_ITEM_DESCRIPTIONS.toArray(new String[COMPOSITE_ITEM_DESCRIPTIONS.size()]), _bindingItemTypes);
        _bindinglistDataType = new TabularType("Exchange Bindings", "Exchange Bindings for " + getName(),
                _bindingDataType, TABULAR_UNIQUE_INDEX.toArray(new String[TABULAR_UNIQUE_INDEX.size()]));
    }

    public ManagedObject getParentObject()
    {
        return _exchange.getVirtualHost().getManagedObject();
    }

    public T getExchange()
    {
        return _exchange;
    }


    public String getObjectInstanceName()
    {
        return ObjectName.quote(_exchange.getName());
    }

    public String getName()
    {
        return _exchange.getName();
    }

    public String getExchangeType()
    {
        return _exchange.getTypeShortString().toString();
    }

    public Integer getTicketNo()
    {
        return _exchange._ticket;
    }

    public boolean isDurable()
    {
        return _exchange._durable;
    }

    public boolean isAutoDelete()
    {
        return _exchange._autoDelete;
    }

    // Added exchangetype in the object name lets maangement apps to do any customization required
    public ObjectName getObjectName() throws MalformedObjectNameException
    {
        String objNameString = super.getObjectName().toString();
        objNameString = objNameString + ",ExchangeType=" + getExchangeType();
        return new ObjectName(objNameString);
    }

    protected ManagedObjectRegistry getManagedObjectRegistry()
    {
        return ApplicationRegistry.getInstance().getManagedObjectRegistry();
    }

    public void createNewBinding(String queueName, String binding) throws JMException
    {
        VirtualHost vhost = getExchange().getVirtualHost();
        AMQQueue queue = vhost.getQueueRegistry().getQueue(new AMQShortString(queueName));
        if (queue == null)
        {
            throw new JMException("Queue \"" + queueName + "\" is not registered with the virtualhost.");
        }

        CurrentActor.set(new ManagementActor(_logActor.getRootMessageLogger()));
        try
        {
            vhost.getBindingFactory().addBinding(binding,queue,getExchange(),null);
            //this is similar to adding a subscription
        }
        catch (AMQException ex)
        {
            JMException jme = new JMException(ex.toString());
            throw new MBeanException(jme, "Error creating new binding " + binding);
        }
        CurrentActor.remove();
    }

    /**
     * Removes a queue binding from the exchange.
     *
     * @see BindingFactory#removeBinding(String, AMQQueue, Exchange, Map)
     */
    public void removeBinding(String queueName, String binding) throws JMException
    {
        VirtualHost vhost = getExchange().getVirtualHost();
        AMQQueue queue = vhost.getQueueRegistry().getQueue(new AMQShortString(queueName));
        if (queue == null)
        {
            throw new JMException("Queue \"" + queueName + "\" is not registered with the virtualhost.");
        }

        CurrentActor.set(new ManagementActor(_logActor.getRootMessageLogger()));
        try
        {
            vhost.getBindingFactory().removeBinding(binding, queue, _exchange, Collections.<String, Object>emptyMap(), true);

        }
        catch (AMQException ex)
        {
            JMException jme = new JMException(ex.toString());
            throw new MBeanException(jme, "Error removing binding " + binding);
        }
        CurrentActor.remove();
    }
}
