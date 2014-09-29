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
     * @param durableMessageStore
     *         durableMessageStore
     * @return MessageStoreManager implementation
     * @throws AndesException
     */
    public static MessageStoreManager create(MessageStore durableMessageStore) throws
                                                                               AndesException {

        MessageStoreManager messageStoreManager;
        if (AndesContext.getInstance().isClusteringEnabled()) { // clustered mode.
            // NOTE: No in memory stores
            if (isAsyncStoringEnabled()) {
                // clustered setup with asynchronous message storing
                messageStoreManager = new DurableAsyncStoringManager();
                messageStoreManager.initialise(durableMessageStore);
                log.info("Message Storing strategy: Asynchronous message storing.");
                return messageStoreManager;
            } else {
                // clustered setup with direct message storing
                messageStoreManager = new DurableDirectStoringManager();
                messageStoreManager.initialise(durableMessageStore);
                log.info("Message Storing strategy: direct message storing.");
                return messageStoreManager;
            }
        } else {
            // single node deployment
            // todo: add proper in memory included MessageStoreManager with async and direct
            if(!isAsyncStoringEnabled()) {
                messageStoreManager = new DurableDirectStoringManager();
                messageStoreManager.initialise(durableMessageStore);
                log.info("Message Storing strategy: direct message storing.");
                return messageStoreManager;
            }
            // todo: in single node mode use else block with in-memory message store for non
            // persistent messages (This is not total in-memory mode)
        }

        // setup default strategy and return
        messageStoreManager = new DurableAsyncStoringManager();
        messageStoreManager.initialise(durableMessageStore);
        log.info("Message Storing strategy: (Default) Asynchronous message storing.");
        return messageStoreManager;
    }

    /**
     * read from configurations determine is asynchronous storing is enabled
     *
     * @return true if asynchronous storing is enabled and false otherwise
     */
    private static boolean isAsyncStoringEnabled() {
        String asyncStoringProperty = "asyncStoring";
        VirtualHostsConfiguration virtualHostsConfiguration = AndesContext.getInstance()
                                                                          .getVirtualHostsConfiguration();
        String isAsyncString = virtualHostsConfiguration.getMessageStoreProperties()
                                                        .getProperty(asyncStoringProperty);
        if (isAsyncString.isEmpty()) { // if value is not set
            isAsyncString = "true"; // default to true
        }
        return Boolean.parseBoolean(isAsyncString);
    }
}
