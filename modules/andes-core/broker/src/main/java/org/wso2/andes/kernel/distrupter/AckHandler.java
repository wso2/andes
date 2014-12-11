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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.stats.PerformanceCounter;

/**
 * Acknowledgement Handler for the Disruptor based inbound event handling.
 * This handler processes acknowledgements received from clients and updates Andes.
 */
public class AckHandler implements BatchEventHandler {

    private static Log log = LogFactory.getLog(AckHandler.class);

    @Override
    public void onEvent(final List<InboundEvent> eventList) throws Exception {
        if(log.isDebugEnabled()){
            log.debug(eventList.size() + " acknowledgements received from disruptor.");
        }
        ackReceived(eventList);
    }

    /**
     * Updates the state of Andes and deletes relevant messages. (For topics message deletion will happen only when
     * all the clients acknowledges)
     * @param eventList inboundEvent list
     * @throws AndesException
     */
    public void ackReceived(final List<InboundEvent> eventList) throws AndesException {
        List<AndesRemovableMetadata> removableMetadata = new ArrayList<AndesRemovableMetadata>();
        for (InboundEvent event : eventList) {

            AndesAckData ack = event.ackData;
            // For topics message is shared. If all acknowledgements are received only we should remove message
            boolean isOkToDeleteMessage = OnflightMessageTracker.getInstance()
                    .handleAckReceived(ack.getChannelID(), ack.getMessageID());
            if (isOkToDeleteMessage) {
                if (log.isDebugEnabled()) {
                    log.debug("Ok to delete message id " + ack.getMessageID());
                }
                removableMetadata.add(new AndesRemovableMetadata(ack.getMessageID(), ack.getDestination(),
                        ack.getMsgStorageDestination()));
            }

            OnflightMessageTracker.getInstance().decrementNonAckedMessageCount(ack.getChannelID());
            //record ack received

            PerformanceCounter.recordMessageRemovedAfterAck();
        }

        // Depending on the async or direct deletion strategy used in MessagingEngine messages will be deleted
        MessagingEngine.getInstance().deleteMessages(removableMetadata, false);
    }


}
