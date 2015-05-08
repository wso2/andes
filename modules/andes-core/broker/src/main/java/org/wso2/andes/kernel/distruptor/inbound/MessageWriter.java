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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.distruptor.BatchEventHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Writes messages in Disruptor ring buffer to message store in batches.
 */
public class MessageWriter implements BatchEventHandler {

    private static Log log = LogFactory.getLog(MessageWriter.class);
    private final List<AndesMessage> messageList;

    /**
     * Temporary storage for retain messages
     */
    private final List<AndesMessage> retainList;

    /**
     * Reference to messaging engine. This is used to store messages
     */
    private final MessagingEngine messagingEngine;

    public MessageWriter(MessagingEngine messagingEngine, int messageBatchSize) {
        this.messagingEngine = messagingEngine;
        // For topics the size may be more than messageBatchSize since inbound event might contain more than one message
        // But this is valid for queues.
        messageList = new ArrayList<AndesMessage>(messageBatchSize);
        retainList = new ArrayList<AndesMessage>(messageBatchSize);

    }

    @Override
    public void onEvent(final List<InboundEventContainer> eventList) throws Exception {

        // For topics there may be multiple messages in one event.
        for (InboundEventContainer event : eventList) {
            messageList.addAll(event.messageList);

            if (null != event.retainMessage) {
                retainList.add(event.retainMessage);
            }
        }

        messagingEngine.messagesReceived(messageList);
        if (!retainList.isEmpty()) {
            messagingEngine.storeRetainedMessages(retainList);
        }

        if(log.isDebugEnabled()) {
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
        retainList.clear();
    }
}
