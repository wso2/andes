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

package org.wso2.andes.kernel.distrupter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.MessagingEngine;

import java.util.ArrayList;
import java.util.List;

/**
 * Writes messages in Disruptor ring buffer to message store in batches.
 */
public class messageWriter implements BatchEventHandler {

    private static Log log = LogFactory.getLog(messageWriter.class);
    private final List<AndesMessage> messageList;

    public messageWriter() {
        messageList = new ArrayList<AndesMessage>();
    }

    @Override
    public void onEvent(final List<InboundEvent> eventList) throws Exception {

        messageList.clear();

        // For topics there may be multiple messages in one event.
        for (InboundEvent event : eventList) {
            messageList.addAll(event.messageList);
        }

        MessagingEngine.getInstance().messagesReceived(messageList);

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
    }
}
