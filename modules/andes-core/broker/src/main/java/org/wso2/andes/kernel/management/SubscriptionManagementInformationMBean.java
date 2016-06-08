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
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.management.mbeans.SubscriptionManagementInformation;
import org.wso2.andes.server.util.CompositeDataHelper;

import java.util.ArrayList;
import java.util.List;
import javax.management.MBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

/**
 * MBeans for managing resources related to subscriptions.
 */
public class SubscriptionManagementInformationMBean implements SubscriptionManagementInformation {

    /**
     * Helper class for converting a subscription for {@link CompositeData}.
     */
    private CompositeDataHelper.SubscriptionCompositeDataHelper subscriptionCompositeDataHelper;

    /**
     * Instantiates the MBeans related to subscriptions.
     */
    public SubscriptionManagementInformationMBean() {
        subscriptionCompositeDataHelper = new CompositeDataHelper().new SubscriptionCompositeDataHelper();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositeData[] getSubscriptions(String protocolTypeAsString, String destinationTypeAsString, String
            subscriptionName, String destinationName, boolean active, int offset, int limit) throws MBeanException {
        List<CompositeData> compositeDataList = new ArrayList<>();

        try {
            ProtocolType protocolType = new ProtocolType(protocolTypeAsString);
            DestinationType destinationType = DestinationType.valueOf(destinationTypeAsString.toUpperCase());

            List<AndesSubscription> andesSubscriptions = Andes.getInstance().getAndesResourceManager()
                    .getSubscriptions(protocolType, destinationType, subscriptionName, destinationName, active,
                            offset, limit);

            for (AndesSubscription subscription : andesSubscriptions) {
                Long pendingMessageCount = MessagingEngine.getInstance().getMessageCountOfQueue(subscription
                        .getStorageQueueName());
                compositeDataList.add(subscriptionCompositeDataHelper.getSubscriptionAsCompositeData(subscription,
                        pendingMessageCount));

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
    public void removeSubscriptions(String protocolTypeAsString, String subscriptionTypeAsString, String
            destinationName) throws MBeanException {
        try {
            ProtocolType protocolType = new ProtocolType(protocolTypeAsString);
            DestinationType subscriptionType = DestinationType.valueOf(subscriptionTypeAsString.toUpperCase());
            Andes.getInstance().getAndesResourceManager().removeSubscriptions(protocolType, subscriptionType,
                    destinationName);
        } catch (AndesException e) {
            throw new MBeanException(e, "Error in removing subscriptions");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeSubscription(String protocolTypeAsString, String subscriptionTypeAsString, String
            destinationName, String subscriptionId) throws MBeanException {
        try {
            ProtocolType protocolType = new ProtocolType(protocolTypeAsString);
            DestinationType subscriptionType = DestinationType.valueOf(subscriptionTypeAsString.toUpperCase());
            Andes.getInstance().getAndesResourceManager().removeSubscription(protocolType, subscriptionType,
                    destinationName, subscriptionId);
        } catch (AndesException e) {
            throw new MBeanException(e, "Error in removing a subscription.");
        }
    }
}
