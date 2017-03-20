/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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

package org.wso2.andes.kernel.subscription;

import org.wso2.andes.kernel.*;

import java.util.UUID;

/**
 * This class represents a durable topic subscriber. It is treated special type
 * as it logically behaves like a queue subscription. It has an inactive state as well until
 * un-subscription happens.
 */
public class DurableTopicSubscriber extends AndesSubscription {

    /**
     * subscription ID of the subscriber
     */
    private String clientID;

    /**
     * Create a DurableTopicSubscriber instance
     *
     * @param subscriptionID ID of the subscriber (unique cluster-wide)
     * @param clientID       client ID (subscription ID)
     * @param storageQueue   queue to which subscriber is bound
     * @param protocol       protocol of the subscription
     * @param connectionInfo underlying connection information
     */
    public DurableTopicSubscriber(String subscriptionID, String clientID, StorageQueue storageQueue, ProtocolType
            protocol, SubscriberConnection connectionInfo) {
        super(subscriptionID, storageQueue, protocol, connectionInfo);
        this.isActive = true;
        this.clientID = clientID;
    }

    /**
     * Upon closing subscription underlying connection is removed
     */
    public void closeConnection(UUID channelID, String nodeID) throws SubscriptionException {
        if (this.isActive) {
            this.isActive = false;
        } else {
            throw new SubscriptionException("Cannot close inactive subscription id= " + getSubscriptionId()
                    + " channelID= " + channelID);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void forcefullyDisconnectConnections() throws AndesException {
        subscriberConnection.forcefullyDisconnect();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isDurable() {
        return true;
    }

    /**
     * Get client ID (durable topic subscription identifier)
     *
     * @return durable topic subscription identifier
     */
    public String getClientID() {
        return clientID;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isActive() {
        return isActive;
    }
}
