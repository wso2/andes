/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.andes.kernel.storemanager;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.MBConfiguration;
import org.wso2.andes.configuration.VirtualHostsConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.MessageStoreManager;

/**
 * Creates a MessageStoreManager according to the server mode (cluster or single node ) and
 * configurations for message storing.
 */
public class MessageStoreManagerFactory {

    private static final Logger log = Logger.getLogger(MessageStoreManagerFactory.class);

    /**
     * Creates a MessageStoreManager according to the configurations for message storing
     *
     * @param messageStore
     *         messageStore
     * @return MessageStoreManager implementation
     * @throws AndesException
     */
    public static MessageStoreManager create(MessageStore messageStore) throws
                                                                               AndesException {

        boolean isAsyncStoring = MBConfiguration.isAsyncStoringEnabled();
        MessageStoreManager messageStoreManager;
        if (isAsyncStoring) {
            // clustered setup with asynchronous message storing
            messageStoreManager = new AsyncStoringManager();
            messageStoreManager.initialise(messageStore);
            log.info("Message Storing strategy: Asynchronous message storing.");
            return messageStoreManager;
        } else {
            // clustered setup with direct message storing
            messageStoreManager = new DirectStoringManager();
            messageStoreManager.initialise(messageStore);
            log.info("Message Storing strategy: direct message storing.");
            return messageStoreManager;
        }
    }

    public static MessageStoreManager createDirectMessageStoreManager(MessageStore messageStore)
            throws AndesException {
        MessageStoreManager messageStoreManager;
        messageStoreManager = new DirectStoringManager();
        messageStoreManager.initialise(messageStore);
        log.info("Message Storing strategy: direct message storing.");
        return messageStoreManager;
    }
}
