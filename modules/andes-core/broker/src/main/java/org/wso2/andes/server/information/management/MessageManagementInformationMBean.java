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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.disruptor.inbound.InboundQueueEvent;
import org.wso2.andes.management.common.mbeans.MessageManagementInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.server.util.CompositeDataHelper;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostImpl;

import java.util.ArrayList;
import java.util.List;
import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.NotCompliantMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

/**
 * MBeans for managing resources related to messages for protocols such as AMQP, MQTT.
 */
public class MessageManagementInformationMBean extends AMQManagedObject implements MessageManagementInformation {
	/**
     * Logger for {@link MessageManagementInformationMBean} class.
     */
    private static Log log = LogFactory.getLog(MessageManagementInformationMBean.class);

    /**
     * Helper class for converting a message for {@link CompositeData}.
     */
    CompositeDataHelper.MessagesCompositeDataHelper messagesCompositeDataHelper;

	/**
     * The Qpid's queue registry.
     */
    private final QueueRegistry queueRegistry;

    /**
     * Initializes the composite data helper and virtual host.
     *
     * @param vHostMBean Virtual host for the broker.
     * @throws NotCompliantMBeanException
     */
    public MessageManagementInformationMBean(VirtualHostImpl.VirtualHostMBean vHostMBean)
                                                                                    throws NotCompliantMBeanException {
        super(MessageManagementInformation.class, MessageManagementInformation.TYPE);
        messagesCompositeDataHelper = new CompositeDataHelper().new MessagesCompositeDataHelper();
        VirtualHost virtualHost = vHostMBean.getVirtualHost();
        queueRegistry = virtualHost.getQueueRegistry();
    }

	/**
     * {@inheritDoc}
     */
    @Override
    public String getObjectInstanceName() {
        return MessageManagementInformation.TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositeData[] browseDestinationWithMessageID(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination to browse") String destinationName,
    @MBeanOperationParameter(name = "content", description = "Get content of message") boolean content,
    @MBeanOperationParameter(name = "nextMessageID", description = "Starting message ID") long nextMessageID,
    @MBeanOperationParameter(name = "limit", description = "Maximum message count per request") int limit)
            throws MBeanException{
        List<CompositeData> compositeDataList = new ArrayList<>();
        ProtocolType protocolType = ProtocolType.valueOf(protocolTypeAsString.toUpperCase());
        DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());
        try {

            List<AndesMessageMetadata> nextNMessageMetadataFromQueue;
            if (!DLCQueueUtils.isDeadLetterQueue(destinationName)) {
                nextNMessageMetadataFromQueue = Andes.getInstance()
                        .getNextNMessageMetadataFromQueue(destinationName, nextMessageID, limit);
            } else {
                nextNMessageMetadataFromQueue = Andes.getInstance()
                        .getNextNMessageMetadataFromDLC(destinationName, 0, limit);
            }

            for (AndesMessageMetadata andesMessageMetadata : nextNMessageMetadataFromQueue) {
                compositeDataList.add(messagesCompositeDataHelper.getMessageAsCompositeData(protocolType,
                                                                                        andesMessageMetadata, content));
            }

        } catch (AndesException | OpenDataException e) {
            throw new MBeanException(e, "Error occurred in browse queue.");
        }
        return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositeData[] browseDestinationWithOffset(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination to browse") String destinationName,
    @MBeanOperationParameter(name = "content", description = "Get content of message") boolean content,
    @MBeanOperationParameter(name = "offset", description = "Offset for messages") int offset,
    @MBeanOperationParameter(name = "limit", description = "Maximum message count per request") int limit)
            throws MBeanException {
        List<CompositeData> compositeDataList = new ArrayList<>();
        ProtocolType protocolType = ProtocolType.valueOf(protocolTypeAsString.toUpperCase());
        DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());
        try {

            List<AndesMessageMetadata> nextNMessageMetadataFromQueue;
            if (!DLCQueueUtils.isDeadLetterQueue(destinationName)) {
                nextNMessageMetadataFromQueue = Andes.getInstance()
                        .getNextNMessageMetadataFromQueue(destinationName, offset, limit);
            } else {
                nextNMessageMetadataFromQueue = Andes.getInstance()
                        .getNextNMessageMetadataFromDLC(destinationName, 0, limit);
            }

            for (AndesMessageMetadata andesMessageMetadata : nextNMessageMetadataFromQueue) {
                compositeDataList.add(messagesCompositeDataHelper.getMessageAsCompositeData(protocolType,
                                                                                        andesMessageMetadata, content));
            }

        } catch (AndesException | OpenDataException e) {
            throw new MBeanException(e, "Error occurred in browse queue.");
        }

        return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositeData getMessage(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination Name") String destinationName,
    @MBeanOperationParameter(name = "andesMessageID", description = "Andes Metadata Message ID") long andesMessageID,
    @MBeanOperationParameter(name = "content", description = "Get content of message") boolean content)
            throws MBeanException {
        CompositeData message = null;
        ProtocolType protocolType = ProtocolType.valueOf(protocolTypeAsString.toUpperCase());
        DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());
        try {

            List<AndesMessageMetadata> nextNMessageMetadataFromQueue;
            if (!DLCQueueUtils.isDeadLetterQueue(destinationName)) {
                nextNMessageMetadataFromQueue = Andes.getInstance()
                        .getNextNMessageMetadataFromQueue(destinationName, andesMessageID, 1);
            } else {
                nextNMessageMetadataFromQueue = Andes.getInstance()
                        .getNextNMessageMetadataFromDLC(destinationName, 0, 1);
            }

            for (AndesMessageMetadata andesMessageMetadata : nextNMessageMetadataFromQueue) {
                message = messagesCompositeDataHelper.getMessageAsCompositeData(protocolType, andesMessageMetadata,
                                                                                                            content);
            }

        } catch (AndesException | OpenDataException e) {
            throw new MBeanException(e, "Error occurred in browse queue.");
        }
        return message;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessages(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination Name") String destinationName)
            throws MBeanException {
        ProtocolType protocolType = ProtocolType.valueOf(protocolTypeAsString.toUpperCase());
        DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());
        AMQQueue queue = queueRegistry.getQueue(new AMQShortString(destinationName));

        try {
            if (queue == null) {
                throw new JMException("The Queue '" + destinationName + "' is not a registered queue.");
            }

            queue.purge(0l); //This is to trigger the AMQChannel purge event so that the queue
            // state of qpid is updated. This method also validates the request owner and throws
            // an exception if permission is denied.

            InboundQueueEvent andesQueue = AMQPUtils.createAndesQueue(queue);
            int purgedMessageCount = Andes.getInstance().purgeQueue(andesQueue);
            log.info("Total message count purged for queue (from store) : " + destinationName + " : " +
                     purgedMessageCount + ". All in memory messages received before the purge call" +
                     " are abandoned from delivery phase. ");

        } catch (JMException jme) {
            if (jme.toString().contains("not a registered queue")) {
                throw new MBeanException(jme, "The Queue " + destinationName + " is not a registered " +
                                              "queue.");
            } else {
                throw new MBeanException(jme, "Error in purging queue : " + destinationName);
            }
        } catch (AMQException | AndesException amqex) {
            throw new MBeanException(amqex, "Error in purging queue : " + destinationName);
        }
    }
}
