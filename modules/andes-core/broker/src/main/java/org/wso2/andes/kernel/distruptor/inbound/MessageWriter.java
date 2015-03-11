/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
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

package org.wso2.andes.kernel.distruptor.inbound;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.distruptor.BatchEventHandler;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;

import com.google.common.util.concurrent.SettableFuture;

/**
 * Writes messages in Disruptor ring buffer to message store in batches.
 */
public class MessageWriter implements BatchEventHandler, StoreHealthListener {

    private static Log log = LogFactory.getLog(MessageWriter.class);
    
    /**
     * List of messages to write to message store.
     */
    private final List<AndesMessage> messageList;

    /**
     * Indicates and provides a barrier if messages stores become offline.
     * marked as volatile since this value could be set from a different thread
     * (other than those of disrupter)
     */
    private volatile SettableFuture<Boolean> messageStoresUnavailable;
    
    /**
     * Reference to messaging engine. This is used to store messages
     */
    private final MessagingEngine messagingEngine;

    public MessageWriter(MessagingEngine messagingEngine, int messageBatchSize) {
        this.messagingEngine = messagingEngine;
        // For topics the size may be more than messageBatchSize since inbound event might contain more than one message
        // But this is valid for queues.
        messageList = new ArrayList<AndesMessage>(messageBatchSize);
        messageStoresUnavailable = null;
        FailureObservingStoreManager.registerStoreHealthListener(this);
    }

    @Override
    public void onEvent(final List<InboundEventContainer> eventList) throws Exception {

        // For topics there may be multiple messages in one event.
        for (InboundEventContainer event : eventList) {
            messageList.addAll(event.messageList);
        }

        if ( messageStoresUnavailable != null){
            log.info("message store has become unavailable therefore waiting until store becomes available");
            messageStoresUnavailable.get();// stop processing until message stores are available.
            log.info("message store has become available therefore resuming work");
            messageStoresUnavailable = null;
        }
        
        
        try {
            messagingEngine.messagesReceived(messageList);

            if (log.isDebugEnabled()) {
                log.debug(messageList.size() + " messages received from disruptor.");
            }

            if (log.isTraceEnabled()) {
                StringBuilder messageIDsString = new StringBuilder();
                for (AndesMessage message : messageList) {
                    messageIDsString.append(message.getMetadata().getMessageID()).append(" , ");
                }
                log.trace(messageList.size() + " messages written : " + messageIDsString);
            }

            // clear the messages
            messageList.clear();

        } catch (Exception ex) {
            log.warn("unable to store messages, probably due to errors in message stores. messages count : " + messageList.size());
            throw ex;
        }
    }

    /**
     * {@inheritDoc}
     * <p> Creates a {@link SettableFuture} indicating message store became offline.
     */
    @Override
    public void storeInoperational(HealthAwareStore store, Exception ex) {
        log.info(String.format("messagestore became inoperational. messages to store : %d",messageList.size()));
        messageStoresUnavailable = SettableFuture.create();
    }

    /**
     * {@inheritDoc}
     * <p> Sets a value for {@link SettableFuture} indicating message store became online.
     */
    @Override
    public void storeOperational(HealthAwareStore store) {
        log.info(String.format("messagestore became operational. messages to store : %d",messageList.size()));
        messageStoresUnavailable.set(false);
    }
}
