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

package org.wso2.andes.kernel.disruptor.inbound;

import com.lmax.disruptor.EventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesMessage;

import java.util.List;

/**
 * State changes related to Andes for inbound events are handled through this handler
 */
public class StateEventHandler implements EventHandler<InboundEventContainer> {

    private static Log log = LogFactory.getLog(StateEventHandler.class);

    @Override
    public void onEvent(InboundEventContainer event, long sequence, boolean endOfBatch) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug("[ sequence " + sequence + " ] Event received from disruptor. Event type: "
                    + event.eventInfo());
        }

        try {
            switch (event.getEventType()) {
            case MESSAGE_EVENT:
                handlePublisherAcknowledgment(event);
                // Since this is the final handler to be executed, message list needs to be cleared on message event.
                event.clearMessageList(event.getChannel());
                break;
            case TRANSACTION_COMMIT_EVENT:
                event.updateState();
                event.clearMessageList(event.getChannel());
                break;
            default:
                event.updateState();
                break;
            }

        } finally {
            // This is the final handler that visits the slot in ring buffer. Hence after processing is done clear the
            // slot so that in next iteration of the first event handler over the same slot won't find garbage from
            // previous iterations.
            event.clear();
        }
    }

    /**
     * Update slot message counters and queue counters
     *
     * @param eventContainer InboundEventContainer
     */
    public void handlePublisherAcknowledgment(InboundEventContainer eventContainer) {

        List<AndesMessage> messageList = eventContainer.getMessageList();
        //We need to ack only once since, one publisher - multiple topics
        //Event container holds messages relevant to one message published
        //i.e retain messages the ack will be handled during the pre processing stage, therefore we need to ensure that
        // there are messages on the list
        if (messageList.size() > 0) {
            if (log.isDebugEnabled()) {
                log.debug("Acknowledging to the publisher " + eventContainer.getChannel());
            }
            eventContainer.pubAckHandler.ack(messageList.get(0).getMetadata());
        }

    }
}
