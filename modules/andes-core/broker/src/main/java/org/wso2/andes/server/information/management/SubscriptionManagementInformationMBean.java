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

import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.management.common.mbeans.SubscriptionManagementInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.server.util.CompositeDataHelper;
import org.wso2.andes.subscription.LocalSubscription;
import org.wso2.andes.subscription.SubscriptionEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.management.MBeanException;
import javax.management.NotCompliantMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

/**
 * MBeans for managing resources related to subscriptions for protocols such as AMQP, MQTT.
 */
public class SubscriptionManagementInformationMBean extends AMQManagedObject
																		implements SubscriptionManagementInformation {
	/**
	 * Wildcard character to include all.
	 */
	private static final String ALL_WILDCARD = "*";

	/**
	 * Helper class for converting a subscription for {@link CompositeData}.
	 */
    private CompositeDataHelper.SubscriptionCompositeDataHelper subscriptionCompositeDataHelper;

    /**
     * Subscription store used to query subscription related information
     */
    private SubscriptionEngine subscriptionEngine;

    /**
     * Instantiates the MBeans related to subscriptions.
     *
     * @throws NotCompliantMBeanException
     */
    public SubscriptionManagementInformationMBean() throws NotCompliantMBeanException {
        super(SubscriptionManagementInformation.class, SubscriptionManagementInformation.TYPE);
        subscriptionEngine = AndesContext.getInstance().getSubscriptionEngine();
        subscriptionCompositeDataHelper = new CompositeDataHelper().new SubscriptionCompositeDataHelper();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getObjectInstanceName() {
        return SubscriptionManagementInformation.TYPE;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public CompositeData[] getSubscriptions(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "subscriptionName", description = "Subscription name") String subscriptionName,
    @MBeanOperationParameter(name = "destinationName", description = "Destination Name") String destinationName,
    @MBeanOperationParameter(name = "active", description = "active") boolean active,
    @MBeanOperationParameter(name = "offset", description = "offset") int offset,
    @MBeanOperationParameter(name = "limit", description = "limit") int limit)
		    throws MBeanException {
        List<CompositeData> compositeDataList = new ArrayList<>();
        ProtocolType protocolType = ProtocolType.valueOf(protocolTypeAsString.toUpperCase());
        DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());

        try {
            Set<AndesSubscription> subscriptions = AndesContext.getInstance().getSubscriptionEngine()
                    .getAllClusterSubscriptionsForDestinationType(protocolType, destinationType);

            Set<AndesSubscription> filteredSubscriptions = subscriptions.stream()
            .filter(s -> s.getProtocolType() == protocolType)
            .filter(s -> s.isDurable() == ((destinationType == DestinationType.QUEUE)
                                           || (destinationType == DestinationType.DURABLE_TOPIC)))
            .filter(s -> s.hasExternalSubscriptions() == active)
            .filter(s -> null != subscriptionName && !ALL_WILDCARD.equals(subscriptionName) && s.getSubscriptionID()
			        .contains(subscriptionName))
            .filter(s -> null != destinationName && !ALL_WILDCARD.equals(destinationName) && s.getSubscribedDestination
			        ().equals(destinationName))
            .collect(Collectors.toSet());

            if (DestinationType.TOPIC == destinationType) {
                filteredSubscriptions = filterTopicSubscriptions(filteredSubscriptions);
            }

            List<AndesSubscription> andesSubscriptions = filteredSubscriptions
		            .stream()
		            .skip(offset)
		            .limit(limit)
		            .collect(Collectors.toList());

            for (AndesSubscription subscription : andesSubscriptions) {
                Long pendingMessageCount =
	                        MessagingEngine.getInstance().getMessageCountOfQueue(subscription.getStorageQueueName());
                compositeDataList.add(
	                subscriptionCompositeDataHelper.getSubscriptionAsCompositeData(subscription, pendingMessageCount));

            }
        } catch (AndesException | OpenDataException e) {
            throw new MBeanException(e, "Error getting subscriptions");
        }
        return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void removeSubscriptions(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "subscriptionType", description = "Subscription Type")
    String subscriptionTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Name of the destination") String destinationName)
		    throws MBeanException {
        ProtocolType protocolType = ProtocolType.valueOf(protocolTypeAsString.toUpperCase());
        DestinationType subscriptionType = DestinationType.valueOf(subscriptionTypeAsString.toUpperCase());
	    try {
		    Set<AndesSubscription> activeLocalSubscribersForNode = subscriptionEngine.getActiveLocalSubscribersForNode();

		    List<LocalSubscription> subscriptions = activeLocalSubscribersForNode
				    .stream()
				    .filter(s -> s.getProtocolType() == protocolType)
				    .filter(s -> s.getDestinationType() == subscriptionType)
				    .filter(s -> null != destinationName && !ALL_WILDCARD.equals(destinationName)
				                 && s.getSubscribedDestination().contains(destinationName))
				    .map(s -> (LocalSubscription) s)
				    .collect(Collectors.toList());
		    for (LocalSubscription subscription : subscriptions) {
			    subscription.forcefullyDisconnect();
		    }
	    } catch (AndesException e) {
		    throw new MBeanException(e, "Error in removing subscriptions");
	    }
    }

	/**
     * {@inheritDoc}
     */
    @Override
    public void removeSubscription(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "subscriptionType", description = "Subscription Type")
                                                                                        String subscriptionTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Name of the destination") String destinationName,
    @MBeanOperationParameter(name = "subscriptionId", description = "ID of the Subscription to remove")
                                                                                                String subscriptionId)
            throws MBeanException {
        ProtocolType protocolType = ProtocolType.valueOf(protocolTypeAsString.toUpperCase());
        DestinationType subscriptionType = DestinationType.valueOf(subscriptionTypeAsString.toUpperCase());
        try {
            Set<LocalSubscription> allSubscribersForDestination
                    = subscriptionEngine.getActiveLocalSubscribers(destinationName, protocolType, subscriptionType);

	        LocalSubscription localSubscription = allSubscribersForDestination
			        .stream()
			        .filter(s -> s.getSubscriptionID().equals(subscriptionId))
			        .findFirst()
			        .get();
	        if (null != localSubscription) {
		        localSubscription.forcefullyDisconnect();
	        } else {
		        throw new NullPointerException("Matching subscription could not be found to disconnect.");
	        }
        } catch (AndesException e) {
	        throw new MBeanException(e, "Error in removing a subscription.");
        }
    }

	/**
	 * Filters out topic subscriptions to support shared subscriptions.
	 *
	 * @param subscriptions A set of {@link AndesSubscription}.
	 * @return Filtered out set of {@link AndesSubscription}.
	 */
	private Set<AndesSubscription> filterTopicSubscriptions(Set<AndesSubscription> subscriptions) {
		Set<AndesSubscription> subscriptionsToDisplay = new HashSet<>();

		Map<String, AndesSubscription> inactiveSubscriptions = new HashMap<>();
		Set<String> uniqueSubscriptionIDs = new HashSet<>();

		for (AndesSubscription subscription : subscriptions) {

			if (subscription.isDurable()) {
				if (subscription.hasExternalSubscriptions()) {
					uniqueSubscriptionIDs.add(subscription.getTargetQueue());
				} else {
					// Since only one inactive shared subscription should be shown
					// we replace the existing value if any
					inactiveSubscriptions.put(subscription.getTargetQueue(), subscription);
					// Inactive subscriptions will be added later considering shared subscriptions
					continue;
				}
			}

			subscriptionsToDisplay.add(subscription);
		}

		// In UI only one inactive shared subscription should be shown if there are no active subscriptions.
		// If there are active subscriptions with same target queue, we skip adding inactive subscriptions
		subscriptionsToDisplay.addAll(inactiveSubscriptions.entrySet()
				.stream()
				.filter(inactiveEntry -> !(uniqueSubscriptionIDs.contains(inactiveEntry.getKey())))
				.map(Map.Entry::getValue)
				.collect(Collectors.toList()));

		return subscriptionsToDisplay;
	}
}
