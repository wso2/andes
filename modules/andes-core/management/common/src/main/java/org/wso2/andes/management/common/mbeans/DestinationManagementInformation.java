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

package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;

import javax.management.MBeanException;
import javax.management.MBeanOperationInfo;
import javax.management.openmbean.CompositeData;

/**
 * Interface for managing destinations - queues/topics.
 */
public interface DestinationManagementInformation {
    String TYPE = "DestinationManagementInformation";

    /**
     * Gets the collection of destinations(queues/topics)
     *
     * @param protocolTypeAsString    The protocol type matching for the destination type. Example : AMQP, amqp, MQTT,
     *                                mqtt.
     * @param destinationTypeAsString The destination type matching for the destination. Example : queue, topic,
     *                                durable_topic.
     * @param keyword                 Search keyword for destination name. "*" will return all destinations.
     *                                Destinations that <strong>contains</strong> the keyword will be returned.
     * @param offset                  The offset value for the collection of destination.
     * @param limit                   The number of records to return from the collection of destinations.
     * @return A {@link CompositeData} array with details of destinations.
     * @throws MBeanException
     */
    @MBeanAttribute(name = "Destinations", description = "Gets all destinations to a specific protocol and " +
                                                         "destination type")
    CompositeData[] getDestinations(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "keyword", description = "Search keyword") String keyword,
    @MBeanOperationParameter(name = "offset", description = "Offset for result") int offset,
    @MBeanOperationParameter(name = "limit", description = "Limit for result") int limit) throws MBeanException;

    /**
     * Deletes all the destinations.
     *
     * @param protocolTypeAsString    The protocol type matching for the destination type. Example : amqp, mqtt.
     * @param destinationTypeAsString The destination type matching for the destination. Example : queue, topic,
     *                                durable_topic.
     * @throws MBeanException
     */
    @MBeanOperation(name = "deleteMessages", description = "Deletes destinations for a specific protocol and " +
                                                           "destination type", impact = MBeanOperationInfo.ACTION)
    void  deleteDestinations(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString)
            throws MBeanException;

    /**
     * Gets a destination.
     *
     * @param protocolTypeAsString    The protocol type matching for the destination type. Example : amqp, mqtt.
     * @param destinationTypeAsString The destination type matching for the destination. Example : queue, topic,
     *                                durable_topic.
     * @param destinationName         The name of the destination.
     * @return A {@link CompositeData} with details of the destination.
     * @throws MBeanException
     */
    @MBeanAttribute(name = "Destination", description = "Gets a destination that belongs to a specific protocol, " +
                                                        "destination type and destination name")
    CompositeData getDestination(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination Name") String destinationName)
            throws MBeanException;

    /**
     * Creates a new destination.
     *
     * @param protocolTypeAsString    The protocol type matching for the destination type. Example : amqp, mqtt.
     * @param destinationTypeAsString The destination type matching for the destination. Example : queue, topic,
     *                                durable_topic.
     * @param destinationName         The name of the destination.
     * @param currentUsername         The username of the user who creates the destination.
     * @return A {@link CompositeData} with details of the newly created destination.
     * @throws MBeanException
     */
    @MBeanOperation(name = "createDestination", description = "Creates a destination for a specific protocol and " +
                                                              "destination type and destination name",
            impact = MBeanOperationInfo.ACTION)
    CompositeData  createDestination(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination Name") String destinationName,
    @MBeanOperationParameter(name = "currentUsername", description = "Current user's username") String currentUsername)
            throws MBeanException;

    /**
     * Deletes a destination.
     *
     * @param protocolTypeAsString    The protocol type matching for the destination type. Example : amqp, mqtt.
     * @param destinationTypeAsString The destination type matching for the destination. Example : queue, topic,
     *                                durable_topic.
     * @param destinationName         The name of the destination to be deleted.
     * @throws MBeanException
     */
    @MBeanOperation(name = "removeSubscriptions", description = "Deletes a destination for a specific protocol, " +
                                                                "destination type and destination name",
            impact = MBeanOperationInfo.ACTION)
    void deleteDestination(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination Name") String destinationName)
            throws MBeanException;
}
