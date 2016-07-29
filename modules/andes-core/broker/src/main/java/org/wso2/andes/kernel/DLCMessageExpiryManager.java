/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel;

import java.util.List;

/**
 * DLCMessageExpiryManager is responsible for update the message status in metadata table and the,
 * expiry table since this is associated with the configuration DLC expiry check is set
 * to true
 */
public class DLCMessageExpiryManager implements MessageExpiryManager {

    /**
     * Reference to MessageStore. This holds the messages received by andes
     */
    private MessageStore messageStore;

    public DLCMessageExpiryManager(MessageStore messageStore){
        this.messageStore = messageStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToDLC(List<AndesMessageMetadata> messages, String dlcQueueName) throws AndesException {
        messageStore.moveMetadataToDLCWhenExpiryCheckEnabled(messages,dlcQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToDLC(long messageId, String dlcQueueName) throws AndesException {
        messageStore.moveMetadataToDLCWhenExpiryCheckEnabled(messageId,dlcQueueName);
    }
}
