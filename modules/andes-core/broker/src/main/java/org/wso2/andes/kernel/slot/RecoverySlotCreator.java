/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.slot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;

import java.sql.SQLException;

/**
 * callback for create slot map for given queue.
 */
public class RecoverySlotCreator {

    private static Log log = LogFactory.getLog(RecoverySlotCreator.class);

    /**
     * Inner callback class for recovery slot creator.
     */
    public static class CallBack {

        /**
         *
         * Initialize slot map for given queue
         *
         * @param storageQueueName storage queue name
         * @param firstMessageID first messageID
         * @param lastMessageID last messageID
         * @param messageCount message count for given slot
         * @throws SQLException
         * @throws AndesException
         */
        public void initializeSlotMapForQueue(String storageQueueName,long firstMessageID,long lastMessageID,
                                              int messageCount) throws SQLException, AndesException {

            if (AndesContext.getInstance().isClusteringEnabled()) {
                SlotManagerClusterMode.getInstance().updateMessageID(storageQueueName,
                        AndesContext.getInstance().getClusterAgent().getLocalNodeIdentifier(),
                        firstMessageID, lastMessageID, lastMessageID);
            } else {
                SlotManagerStandalone.getInstance().updateMessageID(storageQueueName, lastMessageID);
            }
            if (log.isDebugEnabled()) {
                log.debug("Created a slot with " + messageCount + " messages for queue (" + storageQueueName + ")");
            }
        }
    }
}
