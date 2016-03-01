/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.server.information.management;

import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.management.common.mbeans.DestinationManagementInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;
import org.wso2.andes.server.AMQBrokerManagerMBean;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.server.util.CompositeDataHelper;
import org.wso2.andes.server.virtualhost.VirtualHostImpl;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.OpenDataException;

/**
 * MBeans for managing resources related to destinations such as queues and topics.
 */
public class DestinationManagementInformationMBean extends AMQManagedObject implements
                                                                                    DestinationManagementInformation {
    /**
     * Wildcard character to include all.
     */
    private static final String ALL_WILDCARD = "*";

	/**
     * 'Direct' exchange for creating binding.
     */
    private static final String DIRECT_EXCHANGE = "amq.direct";

	/**
     * Helper class for converting destinations for {@link CompositeData}.
     */
    private CompositeDataHelper.DestinationCompositeDataHelper destinationCompositeDataHelper;

	/**
     * Virtual host for the broker
     */
    private VirtualHostImpl virtualHost;

    /**
     * Initializes the composite data helper and virtual host.
     *
     * @param vHostMBean Virtual host for the broker.
     * @throws NotCompliantMBeanException
     */
    public DestinationManagementInformationMBean(VirtualHostImpl.VirtualHostMBean vHostMBean) throws
            NotCompliantMBeanException {
        super(DestinationManagementInformation.class, DestinationManagementInformation.TYPE);
        destinationCompositeDataHelper = new CompositeDataHelper().new DestinationCompositeDataHelper();
        virtualHost = vHostMBean.getVirtualHost();
    }

	/**
     * {@inheritDoc}
     */
    @Override
    public CompositeData[] getDestinations(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "keyword", description = "Search keyword") String keyword,
    @MBeanOperationParameter(name = "offset", description = "Offset for result") int offset,
    @MBeanOperationParameter(name = "limit", description = "Limit for result") int limit) throws MBeanException {

        List<CompositeData> compositeDataList = new ArrayList<>();

        try {
            ProtocolType protocolType = new ProtocolType(protocolTypeAsString);
            DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());

            List<AndesQueue> destinations = AndesContext.getInstance().getAMQPConstructStore().getQueues(keyword)
                    .stream()
                    .skip(offset)
                    .limit(limit)
                    .collect(Collectors.toList());

            for (AndesQueue destination : destinations) {
                if (protocolType == destination.getProtocolType()
                                                            && destinationType == destination.getDestinationType()) {

                    long messageCountOfQueue = Andes.getInstance().getMessageCountOfQueue(destination.queueName);
                    CompositeDataSupport support =
                        destinationCompositeDataHelper.getDestinationAsCompositeData(destination, messageCountOfQueue);
                    compositeDataList.add(support);
                }
            }
        } catch (AndesException | OpenDataException e) {
            throw new MBeanException(e, "Error occurred when getting destinations.");
        }
        return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDestinations(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString)
            throws MBeanException {

        try {
            ProtocolType protocolType = new ProtocolType(protocolTypeAsString);
            DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());

            AMQBrokerManagerMBean brokerMBean = (AMQBrokerManagerMBean) virtualHost.getBrokerMBean();
            List<AndesQueue> destinations = AndesContext.getInstance().getAMQPConstructStore().getQueues(ALL_WILDCARD);

            for (AndesQueue destination : destinations) {
                if (protocolType == destination.getProtocolType()
                                                            && destinationType == destination.getDestinationType()) {
                    brokerMBean.deleteQueue(destination.queueName);
                }
            }
        } catch (AndesException | JMException e) {
            throw new MBeanException(e, "Error occurred in deleting destinations. Protocol:" + protocolTypeAsString +
                                        " DestinationType:" + destinationTypeAsString);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositeData getDestination(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination Name") String destinationName)
            throws MBeanException {
        try {
            AndesQueue destination = AndesContext.getInstance().getAMQPConstructStore().getQueue(destinationName);
            if (null != destination) {
                long messageCountOfQueue = Andes.getInstance().getMessageCountOfQueue(destination.queueName);
                return destinationCompositeDataHelper.getDestinationAsCompositeData(destination, messageCountOfQueue);
            } else {
                return null;
            }
        } catch (AndesException | JMException e) {
            throw new MBeanException(e, "Error occurred in deleting destinations. Protocol:" + protocolTypeAsString +
                                        " DestinationType:" + destinationTypeAsString);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositeData createDestination(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination Name") String destinationName,
    @MBeanOperationParameter(name = "currentUsername", description = "Current user's username") String currentUsername)
            throws MBeanException {

        AMQBrokerManagerMBean brokerMBean = (AMQBrokerManagerMBean) virtualHost.getBrokerMBean();
        CompositeData newDestination = null;
        try {
            ProtocolType protocolType = new ProtocolType(protocolTypeAsString);
            DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());

            if ("AMQP".equals(protocolType.getProtocolName()) && DestinationType.QUEUE == destinationType) {

                brokerMBean.createNewQueue(destinationName, currentUsername, true, protocolTypeAsString);

                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

                ObjectName bindingMBeanObjectName =
                        new ObjectName("org.wso2.andes:type=VirtualHost.Exchange,VirtualHost=\"carbon\",name=\"" +
                                       DIRECT_EXCHANGE + "\",ExchangeType=direct");
                String bindingOperationName = "createNewBinding";

                Object[] bindingParams = new Object[]{destinationName, destinationName};
                String[] bpSignatures = new String[]{String.class.getName(), String.class.getName()};

                mBeanServer.invoke(
                        bindingMBeanObjectName,
                        bindingOperationName,
                        bindingParams,
                        bpSignatures);

                AndesQueue destination = AndesContext.getInstance().getAMQPConstructStore().getQueue(destinationName);
                long messageCountOfQueue = Andes.getInstance().getMessageCountOfQueue(destination.queueName);
                newDestination =
                        destinationCompositeDataHelper.getDestinationAsCompositeData(destination, messageCountOfQueue);
            }
        } catch (AndesException | JMException e) {
            throw new MBeanException(e, "Error occurred in deleting destinations. Protocol:" + protocolTypeAsString +
                                        " DestinationType:" + destinationTypeAsString);
        }
        return newDestination;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDestination(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination Name") String destinationName)
            throws MBeanException {
        try {
            AMQBrokerManagerMBean brokerMBean = (AMQBrokerManagerMBean) virtualHost.getBrokerMBean();
            AndesQueue andesDestination = AndesContext.getInstance().getAMQPConstructStore().getQueue(destinationName);
            brokerMBean.deleteQueue(andesDestination.queueName);
        } catch (AndesException | JMException e) {
            throw new MBeanException(e, "Error occurred in deleting destinations. Protocol:" + protocolTypeAsString +
                                        " DestinationType:" + destinationTypeAsString);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getObjectInstanceName() {
        return DestinationManagementInformation.TYPE;
    }
}
