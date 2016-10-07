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
 * Message expiry manager is responsible for keep track of expiry related data in appropriate database tables.
 */
public class MessageExpiryManager {

    /**
     * Reference to MessageStore. This holds the messages received by Andes.
     */
    private MessageStore messageStore;

    public MessageExpiryManager(MessageStore messageStore){
        this.messageStore = messageStore;
    }

    /**
     * Move the meta data to DLC and update the status of messages.
     *
     * @param messages list of message metadata
     * @param dlcQueueName DLC queue name
     */
    public void moveMetadataToDLC(List<AndesMessageMetadata> messages, String dlcQueueName)
            throws AndesException{
        messageStore.moveMetadataToDLC(messages, dlcQueueName);
    }

    /**
     * Move the meta data to DLC and update the status of messages.
     *
     * @param messageId  message ID
     * @param dlcQueueName DLC Queue name for which the message is routed to
     * @throws AndesException
     */
    public void moveMetadataToDLC(long messageId, String dlcQueueName) throws AndesException{
        messageStore.moveMetadataToDLC(messageId, dlcQueueName);
    }
}
