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
 * Message expiry manager ir responsible for keep track of expiry related data in
 * appropriate database tables
 */
public interface MessageExpiryManager {

    /**
     * Move the meta data to DLC and update the status of messages
     * @param messages Lis of message metadata
     * @param dlcQueueName DLC queue name
     */
    public void moveMetadataToDLC(List<AndesMessageMetadata> messages, String dlcQueueName)
            throws AndesException;

    /**
     * Move the meta data to DLC and update the status of messages
     * @param messageId  Message ID
     * @param dlcQueueName DLC Queue name for which the message is routed to
     * @throws AndesException
     */
    public void moveMetadataToDLC(long messageId, String dlcQueueName) throws AndesException;
}
