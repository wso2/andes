/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

package org.wso2.andes.server;

import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.QpidAndesBridge;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.management.common.mbeans.ManagedBroker;
import org.wso2.andes.management.common.mbeans.ManagedQueue;
import org.wso2.andes.management.common.mbeans.annotations.MBeanConstructor;
import org.wso2.andes.management.common.mbeans.annotations.MBeanDescription;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.exchange.ExchangeFactory;
import org.wso2.andes.server.exchange.ExchangeRegistry;
import org.wso2.andes.server.exchange.ExchangeType;
import org.wso2.andes.server.logging.LogMessage;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.actors.ManagementActor;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.server.management.ManagedObject;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.AMQQueueFactory;
import org.wso2.andes.server.queue.AMQQueueMBean;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.server.store.DurableConfigurationStore;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostImpl;

import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This MBean implements the broker management interface and exposes the
 * Broker level management features like creating and deleting exchanges and queue.
 */
@MBeanDescription("This MBean exposes the broker level management features")
public class AMQBrokerManagerMBean extends AMQManagedObject implements ManagedBroker
{
    private final QueueRegistry _queueRegistry;
    private final ExchangeRegistry _exchangeRegistry;
    private final ExchangeFactory _exchangeFactory;
    private final DurableConfigurationStore _durableConfig;

    private final VirtualHostImpl.VirtualHostMBean _virtualHostMBean;

    @MBeanConstructor("Creates the Broker Manager MBean")
    public AMQBrokerManagerMBean(VirtualHostImpl.VirtualHostMBean virtualHostMBean) throws JMException
    {
        super(ManagedBroker.class, ManagedBroker.TYPE);

        _virtualHostMBean = virtualHostMBean;
        VirtualHost virtualHost = virtualHostMBean.getVirtualHost();

        _queueRegistry = virtualHost.getQueueRegistry();
        _exchangeRegistry = virtualHost.getExchangeRegistry();
        _durableConfig = virtualHost.getDurableConfigurationStore();
        _exchangeFactory = virtualHost.getExchangeFactory();
    }

    /**
     * {@inheritDoc}
     */
    public String getObjectInstanceName()
    {
        return _virtualHostMBean.getVirtualHost().getName();
    }

    /**
     * Returns an array of the exchange types available for creation.
     * @since Qpid JMX API 1.3
     * @throws IOException
     */
    public String[] getExchangeTypes() throws IOException
    {
        ArrayList<String> exchangeTypes = new ArrayList<>();
        for(ExchangeType<? extends Exchange> ex : _exchangeFactory.getPublicCreatableTypes())
        {
            exchangeTypes.add(ex.getName().toString());
        }

        return exchangeTypes.toArray(new String[exchangeTypes.size()]);
    }

    /**
     * Returns a list containing the names of the attributes available for the Queue mbeans.
     * @since Qpid JMX API 1.3
     * @throws IOException
     */
    public List<String> retrieveQueueAttributeNames() throws IOException
    {
        return ManagedQueue.QUEUE_ATTRIBUTES;
    }

    /**
     * Returns a List of Object Lists containing the requested attribute values (in the same sequence requested) for
     * each queue in the virtualhost.
     * If a particular attribute cant be found or raises an mbean/reflection exception whilst being gathered its
     * value is substituted with the String "-".
     *
     * @throws IOException
     * @since Qpid JMX API 1.3
     */
    public List<List<Object>> retrieveQueueAttributeValues(String[] attributes) throws IOException
    {
        if(_queueRegistry.getQueues().size() == 0)
        {
            return new ArrayList<>();
        }

        List<List<Object>> queueAttributesList = new ArrayList<>(_queueRegistry.getQueues().size());

        int attributesLength = attributes.length;

        for(AMQQueue queue : _queueRegistry.getQueues())
        {
            AMQQueueMBean mbean = (AMQQueueMBean) queue.getManagedObject();

            if(mbean == null)
            {
                continue;
            }

            List<Object> attributeValues = new ArrayList<>(attributesLength);

            for (String attribute : attributes) {
                try {
                    attributeValues.add(mbean.getAttribute(attribute));
                } catch (Exception e) {
                    attributeValues.add("-");
                }
            }

            queueAttributesList.add(attributeValues);
        }

        return queueAttributesList;
    }

    /**
     * Creates new exchange and registers it with the registry.
     *
     * @param exchangeName The name for the new exchange.
     * @param type The exchange type can be 'direct', 'topic', 'header', 'fanout'.
     * @param durable Whether the exchange should be durable or not.
     * @throws JMException
     */
    public void createNewExchange(String exchangeName, String type, boolean durable) throws JMException
    {
        CurrentActor.set(new ManagementActor(_logActor.getRootMessageLogger()));
        try
        {
            synchronized (_exchangeRegistry)
            {
                Exchange exchange = _exchangeRegistry.getExchange(new AMQShortString(exchangeName));
                if (exchange == null)
                {
                    exchange = _exchangeFactory.createExchange(new AMQShortString(exchangeName), new AMQShortString(type),
                                                               durable, false, 0);
                    _exchangeRegistry.registerExchange(exchange);
                    if (durable)
                    {
                        _durableConfig.createExchange(exchange);

                        //tell Andes kernel to create Exchange
                        QpidAndesBridge.createExchange(exchange);
                    }
                }
                else
                {
                    throw new JMException("The exchange \"" + exchangeName + "\" already exists.");
                }
            }
        }
        catch (AMQException ex)
        {
            JMException jme = new JMException(ex.toString());
            throw new MBeanException(jme, "Error in creating exchange " + exchangeName);
        }
        finally
        {
            CurrentActor.remove();
        }
    }

    /**
     * Unregister the exchange from registry.
     *
     * @param exchangeName The exchange name to unregister
     * @throws JMException
     * @throws MBeanException
     */
    public void unregisterExchange(String exchangeName) throws JMException
    {
        // TODO
        // Check if the exchange is in use.
        // boolean inUse = false;
        // Check if there are queue-bindings with the exchange and unregister
        // when there are no bindings.
        CurrentActor.set(new ManagementActor(_logActor.getRootMessageLogger()));
        try
        {
            _exchangeRegistry.unregisterExchange(new AMQShortString(exchangeName), false);
        }
        catch (AMQException ex)
        {
            JMException jme = new JMException(ex.toString());
            throw new MBeanException(jme, "Error in unregistering exchange " + exchangeName);
        }
        finally
        {
            CurrentActor.remove();
        }
    }

    /**
     *
     * Creates a new queue and registers it with the registry and puts it
     * in persistance storage if durable queue.
     *
     * @param queueName The name of the queue to create.
     * @param owner The owner's username.
     * @param durable Whether the queue is durable or not.
     * @throws JMException
     */
    public void createNewQueue(String queueName, String owner, boolean durable) throws JMException
    {
        AMQQueue queue = _queueRegistry.getQueue(new AMQShortString(queueName));
        try
        {
            if (queue != null)
            {
                //ClusterResourceHolder.getInstance().getCassandraMessageStore().addQueue(queueName);
                throw new JMException("The queue \"" + queueName + "\" already exists.");
            }

            CurrentActor.set(new ManagementActor(_logActor.getRootMessageLogger()));
            AMQShortString ownerShortString = null;
            if (owner != null)
            {
                ownerShortString = new AMQShortString(owner);
            }

            queue = AMQQueueFactory.createAMQQueueImpl(new AMQShortString(queueName), durable, ownerShortString,
                                                                                false, false, getVirtualHost(), null);
            if (queue.isDurable() && !queue.isAutoDelete())
            {
                _durableConfig.createQueue(queue);

                //tell Andes kernel to create queue
                QpidAndesBridge.createQueue(queue);
            }

        }
        catch (Exception ex)
        {
            JMException jme = new JMException(ex.toString());
            throw new MBeanException(jme, "The queue \"" + queueName + "\" already exists.");
        }
        finally
        {
            CurrentActor.remove();
        }
    }

    /**
     * Gets the virtual host of andes.
     *
     * @return The virtual host.
     */
    private VirtualHost getVirtualHost() {
        return _virtualHostMBean.getVirtualHost();
    }

    /**
     * Deletes the queue from queue registry and persistant storage.
     *
     * @param queueName The name of the queue to delete.
     * @throws JMException
     * @throws MBeanException
     */
    public void deleteQueue(final String queueName) throws JMException
    {
        AMQQueue queue = _queueRegistry.getQueue(new AMQShortString(queueName));
        if (queue == null)
        {
            throw new JMException("The Queue " + queueName + " is not a registered queue.");
        }
        try
        {
            CurrentActor.set(new ManagementActor(_logActor.getRootMessageLogger()));
            boolean isQueueDeletable = ClusterResourceHolder.getInstance().
                    getVirtualHostConfigSynchronizer().checkIfQueueDeletable(queue);
            if(isQueueDeletable) {

                //tell Andes kernel to remove queue
                QpidAndesBridge.deleteQueue(queue);

                // Removes the binding and unregister the queue.
                queue.delete();
                if (queue.isDurable())
                {
                    _durableConfig.removeQueue(queue);
                }
            } else {
                _logActor.message( new LogMessage()
                {
                    String message = "Cannot Delete Queue" + queueName + " It Has Registered Subscriptions.";
                    public String toString()
                    {
                        return message;
                    }

                    public String getLogHierarchy()
                    {
                        return "amqp.queue";
                    }
                });
                throw new AMQException("Cannot Delete Queue" + queueName + " It Has Registered Subscriptions.");
            }
        }
        catch (AMQException ex)
        {
            JMException jme = new JMException(ex.toString());
            if(ex.toString().contains("not a registered queue")) {
                throw new MBeanException(jme, "The Queue " + queueName + " is not a registered queue.");
            } else if (ex.toString().contains("Has Registered Subscriptions")) {
                throw new MBeanException(jme, "Queue " +queueName +" has active subscribers. Please stop them first.");
            } else {
                throw new MBeanException(jme, "Error in deleting queue " + queueName + ":");
            }
        } catch (Exception e) {
            throw new MBeanException(e, "Error in deleting queue " + queueName + ". There was an issue with cluster " +
                                        "coordination");
        }
        finally
        {
            CurrentActor.remove();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ManagedObject getParentObject()
    {
        return _virtualHostMBean;
    }

    // This will have a single instance for a virtual host, so not having the name property in the ObjectName
    @Override
    public ObjectName getObjectName() throws MalformedObjectNameException
    {
        return getObjectNameForSingleInstanceMBean();
    }

    public void resetStatistics()
    {
        getVirtualHost().resetStatistics();
    }

    public double getPeakMessageDeliveryRate()
    {
        return getVirtualHost().getMessageDeliveryStatistics().getPeak();
    }

    public double getPeakDataDeliveryRate()
    {
        return getVirtualHost().getDataDeliveryStatistics().getPeak();
    }

    public double getMessageDeliveryRate()
    {
        return getVirtualHost().getMessageDeliveryStatistics().getRate();
    }

    public double getDataDeliveryRate()
    {
        return getVirtualHost().getDataDeliveryStatistics().getRate();
    }

    public long getTotalMessagesDelivered()
    {
        return getVirtualHost().getMessageDeliveryStatistics().getTotal();
    }

    public long getTotalDataDelivered()
    {
        return getVirtualHost().getDataDeliveryStatistics().getTotal();
    }

    public double getPeakMessageReceiptRate()
    {
        return getVirtualHost().getMessageReceiptStatistics().getPeak();
    }

    public double getPeakDataReceiptRate()
    {
        return getVirtualHost().getDataReceiptStatistics().getPeak();
    }

    public double getMessageReceiptRate()
    {
        return getVirtualHost().getMessageReceiptStatistics().getRate();
    }

    public double getDataReceiptRate()
    {
        return getVirtualHost().getDataReceiptStatistics().getRate();
    }

    public long getTotalMessagesReceived()
    {
        return getVirtualHost().getMessageReceiptStatistics().getTotal();
    }

    public long getTotalDataReceived()
    {
        return getVirtualHost().getDataReceiptStatistics().getTotal();
    }

    public boolean isStatisticsEnabled()
    {
        return getVirtualHost().isStatisticsEnabled();
    }
}
