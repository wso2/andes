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
 * Interface for managing subscriptions.
 */
public interface SubscriptionManagementInformation {
    String TYPE = "SubscriptionManagementInformation";

	/**
	 * Gets subscriptions belonging to a specific protocol type and destination type. The subscriptions can be filtered
	 * by subscription name, destination name and whether they are active or not.
	 *
	 * @param protocolTypeAsString    The protocol type matching for the subscription. Example : amqp, mqtt.
	 * @param destinationTypeAsString The destination type matching for the subscription. Example : queue, topic,
	 *                                durable_topic.
	 * @param subscriptionName        The name of the subscription. If "*", all subscriptions are included. Else
	 *                                subscriptions that <strong>contains</strong> the value are included.
	 * @param destinationName         The name of the destination name. If "*", all destinations are included. Else
	 *                                destinations that <strong>equals</strong> the value are included.
	 * @param active                  Filtering the subscriptions that are active or inactive.
	 * @param offset                  The starting index to return.
	 * @param limit                   The number of subscriptions to return.
	 * @return An array of {@link CompositeData} representing subscriptions.
	 * @throws MBeanException
	 */
	@MBeanAttribute(name="getSubscriptions",description = "Gets subscriptions belonging to a specific protocol type " +
	                                                      "and destination type.")
    CompositeData[] getSubscriptions(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "subscriptionName", description = "Subscription name") String subscriptionName,
    @MBeanOperationParameter(name = "destinationName", description = "Destination Name") String destinationName,
    @MBeanOperationParameter(name = "active", description = "active") boolean active,
    @MBeanOperationParameter(name = "offset", description = "offset") int offset,
    @MBeanOperationParameter(name = "limit", description = "limit") int limit)
    throws MBeanException;

	/**
	 * Close/unsubscribe subscriptions forcefully belonging to a specific protocol type, destination type.
	 *
	 * @param protocolTypeAsString     The protocol type matching for the subscription. Example : amqp, mqtt.
	 * @param subscriptionTypeAsString The subscription type matching for the subscription. Example : queue, topic,
	 *                                 durable_topic.
	 * @param destinationName          The name of the destination to close/unsubscribe. If "*", all destinations are
	 *                                 included. Else destinations that <strong>contains</strong> the value are
	 *                                 included.
	 * @throws MBeanException
	 */
	@MBeanOperation(name = "removeSubscriptions", description = "Deletes/Remove subscriptions forcefully for a " +
	                                                            "specific protocol and destination type",
	                                                            impact = MBeanOperationInfo.ACTION)
    void removeSubscriptions(
	@MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
	@MBeanOperationParameter(name = "subscriptionType", description = "Subscription Type")
																						String subscriptionTypeAsString,
	@MBeanOperationParameter(name = "destinationName", description = "Name of the destination") String destinationName)
			throws MBeanException;

	/**
	 * Close/Remove/Unsubscribe subscriptions forcefully belonging to a specific protocol type, destination type.
	 *
	 * @param protocolTypeAsString     The protocol type matching for the subscription. Example : amqp, mqtt.
	 * @param subscriptionTypeAsString The subscription type matching for the subscription. Example : queue, topic,
	 *                                 durable_topic.
	 * @param destinationName          The name of the destination to close/unsubscribe. If "*", all destinations are
	 *                                 included. Else destinations that <strong>equals</strong> the value are
	 *                                 included.
	 * @throws MBeanException
	 */
	@MBeanOperation(name = "removeSubscription", description = "Deletes/Remove subscription forcefully for a specific" +
	                                                           " protocol and destination type",
                                                                impact = MBeanOperationInfo.ACTION)
	void removeSubscription(
	@MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
	@MBeanOperationParameter(name = "subscriptionType", description = "Subscription Type")
																						String subscriptionTypeAsString,
	@MBeanOperationParameter(name = "destinationName", description = "Name of the destination") String destinationName,
	@MBeanOperationParameter(name = "subscriptionId", description = "ID of the Subscription to remove")
																								String subscriptionId)
			throws MBeanException;
}
