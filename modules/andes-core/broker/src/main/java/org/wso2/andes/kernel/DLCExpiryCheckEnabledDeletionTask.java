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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * DLCExpiryCheckEnabledDeletionTask responsible for deleting the expired messages from unassigned slots,
 * and dead letter channel.
 */
public class DLCExpiryCheckEnabledDeletionTask extends PeriodicExpiryMessageDeletionTask {

    private static Log log = LogFactory.getLog(DLCExpiryCheckEnabledDeletionTask.class);

    /**
     * Get the messages form the DLC which are expired and delete those from DB.
     */
    public void deleteExpiredMessagesFromDLC() {

        try {
            //This logic belongs to an MB run in stand alone mode / the coordinator node run in cluster mode
            if (!isClusteringEnabled
                    || (isClusteringEnabled && AndesContext.getInstance().getClusterAgent().isCoordinator())) {

                //Checks for the message store availability if its not available
                //Deletion task needs to await until message store becomes available
                if (null != messageStoresUnavailable) {
                    log.info("Message store has become unavailable therefore expiry message deletion task waiting until"
                            + " store becomes available");
                    //act as a barrier
                    messageStoresUnavailable.get();
                    log.info("Message store became available. Resuming expiry message deletion task");
                    messageStoresUnavailable = null; // we are passing the blockade
                    // (therefore clear the it).
                }

                //get the expired messages for that queue in the range of message ID starting form the lower bound ID
                List<Long> expiredMessages = MessagingEngine.getInstance().getExpiredMessagesFromDLC();

                if ((null != expiredMessages) && (!expiredMessages.isEmpty())) {

                    //Tracing message activity
                    if (MessageTracer.isEnabled()) {
                        for (Long messageId : expiredMessages) {
                            MessageTracer.trace(messageId, "", MessageTracer.EXPIRED_MESSAGE_DETECTED_FROM_DLC);
                        }
                    }

                    //delete message metadata, content from the meta data table, content table and expiry table
                    MessagingEngine.getInstance().deleteMessagesById(expiredMessages);

                    if (log.isDebugEnabled()) {
                        log.debug("Expired message count in DLC is :" + expiredMessages.size());
                    }
                }
            }
        } catch (AndesException e) {
            log.error("Error running Message Expiration Checker " + e.getMessage(), e);
        } catch (InterruptedException e) {
            log.error("Thread interrupted while waiting for message stores to come online", e);
        } catch (ExecutionException e) {
            log.error("Error occurred while waiting for message stores to come online", e);
        } catch (Throwable e) {
            log.error("Error occurred during the DLC expiry message deletion task", e);
        }
    }

    @Override
    public void run() {

         //When DLC expiry check is enabled delete the expired messages from DLC regardless of queue name or message id
        deleteExpiredMessagesFromDLC();

        //After delete messages from DLC then run a queue wise check for expired messages in unallocated zone
        deleteExpiredMessages();
    }
}
