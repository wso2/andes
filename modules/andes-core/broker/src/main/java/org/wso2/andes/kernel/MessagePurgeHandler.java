/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.util.AndesUtils;

/**
 * This class will handle purging of messages depending on queue behaviour in cluster
 */
public class MessagePurgeHandler implements QueueListener {
    private static Log log = LogFactory
            .getLog(MessagePurgeHandler.class);

    @Override
    public void handleClusterQueuesChanged(AndesQueue andesQueue, QueueChange changeType) throws AndesException {
        switch (changeType) {
            case Purged:
                String destinationQueueName = andesQueue.queueName;
                handleMessageRemovalFromNodeQueue(destinationQueueName);
                break;
        }
    }

    @Override
    public void handleLocalQueuesChanged(AndesQueue andesQueue, QueueChange changeType) throws AndesException {
        switch (changeType) {
            case Purged:
                String destinationQueueName = andesQueue.queueName;
                handleMessageRemovalFromNodeQueue(destinationQueueName);
                handleMessageRemovalFromGlobalQueue(destinationQueueName);
                break;
        }
        //if running in standalone mode short-circuit cluster notification
        if (!AndesContext.getInstance().isClusteringEnabled()) {
            handleClusterQueuesChanged(andesQueue, changeType);
        }
    }

    /**
     * remove all messages from global queue addressed to destination queue
     *
     * @param destinationQueueName destination queue to match
     * @throws AndesException
     */
    private void handleMessageRemovalFromGlobalQueue(String destinationQueueName) throws AndesException {
        int numberOfMessagesRemoved = 0;
        //there can be messages still in global queue which needs to be removed
        String globalQueueName = AndesUtils.getGlobalQueueNameForDestinationQueue(destinationQueueName);
        QueueAddress globalQueueAddress = new QueueAddress(QueueAddress.QueueType.GLOBAL_QUEUE, globalQueueName);
        numberOfMessagesRemoved += MessagingEngine.getInstance().removeMessagesOfQueue(globalQueueAddress, destinationQueueName);
        log.info("Removed " + numberOfMessagesRemoved + " Messages From Global Queue Addressed to Queue " + destinationQueueName);
    }

    /**
     * remove all messages from node queue addressed to destination queue
     *
     * @param destinationQueueName destination queue to match
     * @throws AndesException
     */
    private void handleMessageRemovalFromNodeQueue(String destinationQueueName) throws AndesException {
        int numberOfMessagesRemoved = 0;

        //remove any in-memory messages accumulated for the queue
        MessagingEngine.getInstance().removeInMemoryMessagesAccumulated(destinationQueueName);

        //there can be non-acked messages in the node queue addressed to the destination queue
        String nodeQueueName = MessagingEngine.getMyNodeQueueName();
        QueueAddress nodeQueueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE, nodeQueueName);
        numberOfMessagesRemoved += MessagingEngine.getInstance().removeMessagesOfQueue(nodeQueueAddress, destinationQueueName);

        log.info("Removed " + numberOfMessagesRemoved + " Messages From Node Queue Addressed to Queue " + destinationQueueName);

        //remove messages from global queue also
        handleMessageRemovalFromGlobalQueue(destinationQueueName);
    }
}
