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


import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.ProtocolType;

import java.util.UUID;

/**
 * This class represents a mock subscription representing a inactive subscriber
 * (for durable topic)
 */
public class InactiveSubscriber extends DurableTopicSubscriber{


    /**
     * Create a DurableTopicSubscriber instance
     *
     * @param subscriptionID ID of the subscriber (unique cluster-wide)
     * @param clientID       client ID (subscription ID)
     * @param storageQueue   queue to which subscriber is bound
     * @param protocol       protocol of the subscription
     */
    public InactiveSubscriber(String subscriptionID, String clientID, StorageQueue storageQueue, ProtocolType
            protocol) {
        super(subscriptionID, clientID, storageQueue, protocol, null);
        isActive = false;
    }


    /**
     * Update the connection information of the subscriber. For mock subscriber
     * this method is not supported
     *
     * @param connectionInfo new connection information
     */
    public void addConnection(SubscriberConnection connectionInfo) throws SubscriptionException {
        throw new UnsupportedOperationException("This operation is not supported for inactive subscriber");
    }


    /**
     * Upon closing subscription underlying connection is removed. For mock
     * subscriber there is no underlying connection
     */
    public void closeConnection(UUID channelID, String nodeID) throws SubscriptionException {
        throw new UnsupportedOperationException("This operation is not supported for inactive subscriber");
    }

    /**
     * {@inheritDoc}
     */
    public void forcefullyDisconnectConnections() throws AndesException {
        throw new UnsupportedOperationException("This operation is not supported for inactive subscriber");
    }
}
