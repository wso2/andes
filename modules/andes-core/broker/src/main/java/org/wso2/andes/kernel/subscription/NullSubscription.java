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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContent;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.ProtocolMessage;

import java.util.UUID;

/**
 * This class represents an empty subscription with no
 * subscription connection associated. Calling methods of this
 * subscription will have no effect.
 */
public class NullSubscription implements OutboundSubscription {

    private static Log log = LogFactory.getLog(NullSubscription.class);

    @Override
    public void forcefullyDisconnect() throws AndesException {
        throw new UnsupportedOperationException("Invalid operation for forcefully disconnect.");
    }

    @Override
    public boolean isMessageAcceptedBySelector(AndesMessageMetadata messageMetadata) throws AndesException {
        throw new UnsupportedOperationException("Invalid operation for check a message is accepted by the selector.");
    }

    @Override
    public boolean sendMessageToSubscriber(ProtocolMessage messageMetadata, AndesContent content) throws
            AndesException {
        throw new UnsupportedOperationException("Invalid operation for send messages to the subscriber.");
    }

    @Override
    public boolean isOutboundConnectionLive() {
        throw new UnsupportedOperationException("Invalid operation for check outbound connection is live.");
    }

    @Override
    public UUID getChannelID() {
        throw new UnsupportedOperationException("Invalid operation for retrieve channel id.");
    }

    @Override
    public long getSubscribeTime() {
        throw new UnsupportedOperationException("Invalid operation for retrieve subscribes time.");
    }

    @Override
    public String getProtocolQueueName() {
        throw new UnsupportedOperationException("Invalid operation for retrieve protocol name.");
    }

}
