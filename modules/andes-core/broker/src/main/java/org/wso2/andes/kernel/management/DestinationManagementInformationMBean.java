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

package org.wso2.andes.kernel.management;

import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.management.mbeans.DestinationManagementInformation;
import org.wso2.andes.server.util.CompositeDataHelper;

import java.util.ArrayList;
import java.util.List;
import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.OpenDataException;

/**
 * MBeans for managing resources related to destinations such as queues and topics.
 */
public class DestinationManagementInformationMBean implements DestinationManagementInformation {

    /**
     * Helper class for converting destinations for {@link CompositeData}.
     */
    private CompositeDataHelper.DestinationCompositeDataHelper destinationCompositeDataHelper;

    /**
     * Initializes the composite data helper and virtual host.
     *
     * @throws MBeanException
     */
    public DestinationManagementInformationMBean() throws MBeanException {
        destinationCompositeDataHelper = new CompositeDataHelper().new DestinationCompositeDataHelper();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositeData[] getDestinations(String protocolTypeAsString, String destinationTypeAsString, String
            keyword, int offset, int limit) throws MBeanException {
        List<CompositeData> destinationCompositeList = new ArrayList<>();
        try {
            ProtocolType protocolType = new ProtocolType(protocolTypeAsString);
            DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());

            List<AndesQueue> destinations = Andes.getInstance().getAndesResourceManager().getDestinations
                    (protocolType, destinationType, keyword, offset, limit);

            for (AndesQueue destination : destinations) {

                long messageCountOfQueue = Andes.getInstance().getMessageCountOfQueue(destination.queueName);
                CompositeDataSupport support = destinationCompositeDataHelper.getDestinationAsCompositeData
                        (destination, messageCountOfQueue);
                destinationCompositeList.add(support);
            }

        } catch (AndesException | OpenDataException e) {
            throw new MBeanException(e, "Error occurred when getting destinations.");
        }
        return destinationCompositeList.toArray(new CompositeData[destinationCompositeList.size()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDestinations(String protocolTypeAsString, String destinationTypeAsString) throws MBeanException {
        try {
            ProtocolType protocolType = new ProtocolType(protocolTypeAsString);
            DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());
            Andes.getInstance().getAndesResourceManager().deleteDestinations(protocolType, destinationType);
        } catch (AndesException e) {
            throw new MBeanException(e, "Error occurred in deleting destinations.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositeData getDestination(String protocolTypeAsString, String destinationTypeAsString, String
            destinationName) throws MBeanException {
        try {
            ProtocolType protocolType = new ProtocolType(protocolTypeAsString);
            DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());
            AndesQueue destination = Andes.getInstance().getAndesResourceManager().getDestination(protocolType,
                    destinationType, destinationName);
            if (null != destination) {
                long messageCountOfQueue = Andes.getInstance().getMessageCountOfQueue(destination.queueName);
                return destinationCompositeDataHelper.getDestinationAsCompositeData(destination, messageCountOfQueue);
            } else {
                return null;
            }
        } catch (AndesException | JMException e) {
            throw new MBeanException(e, "Error occurred in getting destination.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositeData createDestination(String protocolTypeAsString, String destinationTypeAsString, String
            destinationName, String currentUsername) throws MBeanException {
        CompositeData newDestination;
        try {
            ProtocolType protocolType = new ProtocolType(protocolTypeAsString);
            DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());
            AndesQueue destination = Andes.getInstance().getAndesResourceManager().createDestination(protocolType,
                    destinationType, destinationName, currentUsername);


            long messageCountOfQueue = Andes.getInstance().getMessageCountOfQueue(destination.queueName);
            newDestination = destinationCompositeDataHelper.getDestinationAsCompositeData(destination,
                    messageCountOfQueue);
        } catch (AndesException | JMException e) {
            throw new MBeanException(e, "Error occurred in creating destination.");
        }
        return newDestination;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDestination(String protocolTypeAsString, String destinationTypeAsString, String
            destinationName) throws MBeanException {
        try {
            ProtocolType protocolType = new ProtocolType(protocolTypeAsString);
            DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());
            Andes.getInstance().getAndesResourceManager().deleteDestination(protocolType, destinationType,
                    destinationName);
        } catch (AndesException e) {
            throw new MBeanException(e, "Error occurred in deleting destination.");
        }
    }
}
