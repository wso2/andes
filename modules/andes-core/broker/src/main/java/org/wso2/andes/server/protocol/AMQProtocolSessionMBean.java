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


package org.wso2.andes.server.protocol;

import java.util.Date;
import java.util.List;

import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.MBeanNotificationInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.Notification;
import javax.management.ObjectName;
import javax.management.monitor.MonitorNotification;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.ConnectionCloseBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.management.common.mbeans.ManagedConnection;
import org.wso2.andes.management.common.mbeans.annotations.MBeanConstructor;
import org.wso2.andes.management.common.mbeans.annotations.MBeanDescription;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.actors.ManagementActor;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.server.management.ManagedObject;

/**
 * This MBean class implements the management interface. In order to make more attributes, operations and notifications
 * available over JMX simply augment the ManagedConnection interface and add the appropriate implementation here.
 */
@MBeanDescription("Management Bean for an AMQ Broker Connection")
public class AMQProtocolSessionMBean extends AMQManagedObject implements ManagedConnection
{
    private AMQProtocolSession _protocolSession = null;
    private String _name = null;

    // openmbean data types for representing the channel attributes

    private static final OpenType[] _channelAttributeTypes =
        { SimpleType.INTEGER, SimpleType.BOOLEAN, SimpleType.STRING, SimpleType.INTEGER, SimpleType.BOOLEAN };
    private static CompositeType _channelType = null; // represents the data type for channel data
    private static TabularType _channelsType = null; // Data type for list of channels type
    private static final AMQShortString BROKER_MANAGEMENT_CONSOLE_HAS_CLOSED_THE_CONNECTION =
        new AMQShortString("Broker Management Console has closed the connection.");

    @MBeanConstructor("Creates an MBean exposing an AMQ Broker Connection")
    public AMQProtocolSessionMBean(AMQProtocolSession amqProtocolSession) throws NotCompliantMBeanException, OpenDataException
    {
        super(ManagedConnection.class, ManagedConnection.TYPE);
        _protocolSession = amqProtocolSession;
        String remote = getRemoteAddress();
        _name = "anonymous".equals(remote) ? (remote + hashCode()) : remote;
        init();
    }

    static
    {
        try
        {
            init();
        }
        catch (JMException ex)
        {
            // This is not expected to ever occur.
            throw new RuntimeException("Got JMException in static initializer.", ex);
        }
    }

    /**
     * initialises the openmbean data types
     */
    private static void init() throws OpenDataException
    {
        _channelType =
            new CompositeType("Channel", "Channel Details", COMPOSITE_ITEM_NAMES_DESC.toArray(new String[COMPOSITE_ITEM_NAMES_DESC.size()]),
                    COMPOSITE_ITEM_NAMES_DESC.toArray(new String[COMPOSITE_ITEM_NAMES_DESC.size()]), _channelAttributeTypes);
        _channelsType = new TabularType("Channels", "Channels", _channelType, TABULAR_UNIQUE_INDEX.toArray(new String[TABULAR_UNIQUE_INDEX.size()]));
    }

    public String getClientId()
    {
        return String.valueOf(_protocolSession.getContextKey());
    }

    public String getAuthorizedId()
    {
        return (_protocolSession.getAuthorizedPrincipal() != null ) ? _protocolSession.getAuthorizedPrincipal().getName() : null;
    }

    public String getVersion()
    {
        return (_protocolSession.getClientVersion() == null) ? null : _protocolSession.getClientVersion().toString();
    }

    public Date getLastIoTime()
    {
        return new Date(_protocolSession.getLastIoTime());
    }

    public String getRemoteAddress()
    {
        return _protocolSession.getRemoteAddress().toString();
    }

    public ManagedObject getParentObject()
    {
        return _protocolSession.getVirtualHost().getManagedObject();
    }

    public Long getWrittenBytes()
    {
        return _protocolSession.getWrittenBytes();
    }

    public Long getReadBytes()
    {
        return _protocolSession.getWrittenBytes();
    }

    public Long getMaximumNumberOfChannels()
    {
        return _protocolSession.getMaximumNumberOfChannels();
    }

    public void setMaximumNumberOfChannels(Long value)
    {
        _protocolSession.setMaximumNumberOfChannels(value);
    }

    public String getObjectInstanceName()
    {
        return ObjectName.quote(_name);
    }

    /**
     * commits transactions for a transactional channel
     *
     * @param channelId
     * @throws JMException if channel with given id doesn't exist or if commit fails
     */
    public void commitTransactions(int channelId) throws JMException
    {
        CurrentActor.set(new ManagementActor(_logActor.getRootMessageLogger()));
        try
        {
            AMQChannel channel = _protocolSession.getChannel(channelId);
            if (channel == null)
            {
                throw new JMException("The channel (channel Id = " + channelId + ") does not exist");
            }

            _protocolSession.commitTransactions(channel);
        }
        catch (AMQException ex)
        {
            throw new MBeanException(ex, ex.toString());
        }
        finally
        {
            CurrentActor.remove();
        }
    }

    /**
     * rollsback the transactions for a transactional channel
     *
     * @param channelId
     * @throws JMException if channel with given id doesn't exist or if rollback fails
     */
    public void rollbackTransactions(int channelId) throws JMException
    {
        CurrentActor.set(new ManagementActor(_logActor.getRootMessageLogger()));
        try
        {
            AMQChannel channel = _protocolSession.getChannel(channelId);
            if (channel == null)
            {
                throw new JMException("The channel (channel Id = " + channelId + ") does not exist");
            }

            _protocolSession.rollbackTransactions(channel);
        }
        catch (AMQException ex)
        {
            throw new MBeanException(ex, ex.toString());
        }
        finally
        {
            CurrentActor.remove();
        }
    }

    /**
     * Creates the list of channels in tabular form from the _channelMap.
     *
     * @return list of channels in tabular form.
     * @throws OpenDataException
     */
    public TabularData channels() throws OpenDataException
    {
        TabularDataSupport channelsList = new TabularDataSupport(_channelsType);
        List<AMQChannel> list = _protocolSession.getChannels();

        for (AMQChannel channel : list)
        {
            Object[] itemValues =
                {
                    channel.getChannelId(), channel.isTransactional(),
                    (channel.getDefaultQueue() != null) ? channel.getDefaultQueue().getNameShortString().asString() : null,
                    channel.getUnacknowledgedMessageMap().size(), channel.getBlocking()
                };

            CompositeData channelData = new CompositeDataSupport(_channelType, 
                    COMPOSITE_ITEM_NAMES_DESC.toArray(new String[COMPOSITE_ITEM_NAMES_DESC.size()]), itemValues);
            channelsList.put(channelData);
        }

        return channelsList;
    }

    /**
     * closes the connection. The administrator can use this management operation to close connection to free up
     * resources.
     * @throws JMException
     */
    public void closeConnection() throws JMException
    {

        MethodRegistry methodRegistry = _protocolSession.getMethodRegistry();
        ConnectionCloseBody responseBody =
                methodRegistry.createConnectionCloseBody(AMQConstant.REPLY_SUCCESS.getCode(),
                                                         // replyCode
                                                         BROKER_MANAGEMENT_CONSOLE_HAS_CLOSED_THE_CONNECTION,
                                                         // replyText,
                                                         0,
                                                         0);

        // This seems ugly but because we use closeConnection in both normal
        // broker operation and as part of the management interface it cannot
        // be avoided. The Current Actor will be null when this method is
        // called via the Management interface. This is because we allow the
        // Local API connection with JConsole. If we did not allow that option
        // then the CurrentActor could be set in our JMX Proxy object.
        // As it is we need to set the CurrentActor on all MBean methods
        // Ideally we would not have a single method that can be called from
        // two contexts.
        boolean removeActor = false;
        if (CurrentActor.get() == null)
        {
            removeActor = true;
            CurrentActor.set(new ManagementActor(_logActor.getRootMessageLogger()));
        }

        try
        {
            _protocolSession.writeFrame(responseBody.generateFrame(0));

            try
            {

                _protocolSession.closeSession();
            }
            catch (AMQException ex)
            {
                throw new MBeanException(ex, ex.toString());
            }
        }
        finally
        {
            if (removeActor)
            {
                CurrentActor.remove();
            }
        }
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo()
    {
        String[] notificationTypes = new String[] { MonitorNotification.THRESHOLD_VALUE_EXCEEDED };
        String name = MonitorNotification.class.getName();
        String description = "Channel count has reached threshold value";
        MBeanNotificationInfo info1 = new MBeanNotificationInfo(notificationTypes, name, description);

        return new MBeanNotificationInfo[] { info1 };
    }

    public void notifyClients(String notificationMsg)
    {
        Notification n =
            new Notification(MonitorNotification.THRESHOLD_VALUE_EXCEEDED, this, ++_notificationSequenceNumber,
                System.currentTimeMillis(), notificationMsg);
        _broadcaster.sendNotification(n);
    }

    public void resetStatistics() throws Exception
    {
        _protocolSession.resetStatistics();
    }

    public double getPeakMessageDeliveryRate()
    {
        return _protocolSession.getMessageDeliveryStatistics().getPeak();
    }

    public double getPeakDataDeliveryRate()
    {
        return _protocolSession.getDataDeliveryStatistics().getPeak();
    }

    public double getMessageDeliveryRate()
    {
        return _protocolSession.getMessageDeliveryStatistics().getRate();
    }

    public double getDataDeliveryRate()
    {
        return _protocolSession.getDataDeliveryStatistics().getRate();
    }

    public long getTotalMessagesDelivered()
    {
        return _protocolSession.getMessageDeliveryStatistics().getTotal();
    }

    public long getTotalDataDelivered()
    {
        return _protocolSession.getDataDeliveryStatistics().getTotal();
    }

    public double getPeakMessageReceiptRate()
    {
        return _protocolSession.getMessageReceiptStatistics().getPeak();
    }

    public double getPeakDataReceiptRate()
    {
        return _protocolSession.getDataReceiptStatistics().getPeak();
    }

    public double getMessageReceiptRate()
    {
        return _protocolSession.getMessageReceiptStatistics().getRate();
    }

    public double getDataReceiptRate()
    {
        return _protocolSession.getDataReceiptStatistics().getRate();
    }

    public long getTotalMessagesReceived()
    {
        return _protocolSession.getMessageReceiptStatistics().getTotal();
    }

    public long getTotalDataReceived()
    {
        return _protocolSession.getDataReceiptStatistics().getTotal();
    }

    public boolean isStatisticsEnabled()
    {
        return _protocolSession.isStatisticsEnabled();
    }

    public void setStatisticsEnabled(boolean enabled)
    {
        _protocolSession.setStatisticsEnabled(enabled);
    }
}
