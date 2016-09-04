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

package org.wso2.andes.kernel.router;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.subscription.StorageQueue;

import java.util.Set;

/**
 * This message router implementation will not route the message.
 * Message will be lost
 */
public class DiscardMessageRouter extends AndesMessageRouter {

    /**
     * Create a message router discarding messages. It will not route any
     * message to any queue.
     *
     * @param name       name of the router
     * @param type       type of the router
     * @param autoDelete true if router should be removed when all queues are removed.
     */
    public DiscardMessageRouter(String name, String type, boolean autoDelete) {
        super(name, type, autoDelete);
    }

    /**
     * Create a DiscardMessageRouter with encoded string information
     *
     * @param encodedRouterInfo encoded string information
     */
    public DiscardMessageRouter(String encodedRouterInfo) {
        super(encodedRouterInfo);
    }

    @Override
    public Set<StorageQueue> getMatchingStorageQueues(AndesMessage incomingMessage) {
        return null;
    }

    @Override
    public void onBindingQueue(StorageQueue queue) throws AndesException {

    }

    @Override
    public void onUnbindingQueue(StorageQueue queue) {

    }
}
