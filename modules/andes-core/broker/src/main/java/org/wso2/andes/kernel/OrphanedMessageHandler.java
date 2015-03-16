/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.slot.ConnectionException;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.kernel.slot.SlotDeliveryWorker;
import org.wso2.andes.kernel.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.thrift.MBThriftClient;

/**
 * This class will handle removing messages depending on subscription behaviour
 */
public class OrphanedMessageHandler implements SubscriptionListener {
    private static Log log = LogFactory.getLog(OrphanedMessageHandler.class);
    AndesSubscriptionManager subscriptionManager = ClusterResourceHolder.getInstance().getSubscriptionManager();

    /**
     * Handle subscription changes in cluster. This will perform
     * what needs to be done to the messages addressed to the subscriber
     * @param subscription subscription changed
     * @param changeType type of change happened
     * @throws AndesException
     */
    @Override
    public void handleClusterSubscriptionsChanged(AndesSubscription subscription, SubscriptionChange changeType) throws AndesException {

    }

    /**
     * Handle local subscription changes. This will perform
     * what needs to be done to the messages addressed to the subscriber
     * @param localSubscription subscription changed
     * @param changeType type of change happened
     * @throws AndesException
     */
    @Override
    public void handleLocalSubscriptionsChanged(LocalSubscription localSubscription,
                                                SubscriptionChange changeType)
            throws AndesException {

        switch (changeType) {
            case ADDED:
                break;
            /**
             * When a normal topic subscription closes, check if there is any other subscription
             * available for subscribed destination (considering hierarchical case). If there is
             * none purge all messages addressed to the storage queue belonging to this node
             */
            case DISCONNECTED:
                if (localSubscription.getTargetQueueBoundExchangeName()
                                     .equals(AMQPUtils.TOPIC_EXCHANGE_NAME) && !localSubscription
                                      .isDurable()) {
                    String subscribedDestination = localSubscription.getSubscribedDestination();
                    if(!subscriptionManager.checkIfActiveNonDurableLocalSubscriptionExistsForTopic
                            (subscribedDestination)) {
                        if(log.isDebugEnabled()) {
                            log.debug("Purging messages of this node persisted under " + subscribedDestination);
                        }
                        log.info("Purging messages of this node persisted under " + subscribedDestination);
                        removeMessagesOfDestinationForNode(subscribedDestination,null,true);
                    }

                }
                break;
            case DELETED:
                /**
                 * When a normal topic subscription closes, check if there is any other subscription
                 * available for subscribed destination (considering hierarchical case). If there is
                 * none purge all messages addressed to the storage queue belonging to this node
                 */
                if (localSubscription.getTargetQueueBoundExchangeName()
                                     .equals(AMQPUtils.TOPIC_EXCHANGE_NAME) && !localSubscription
                                     .isDurable()) {
                    String subscribedDestination = localSubscription.getSubscribedDestination();
                    if(!subscriptionManager.checkIfActiveNonDurableLocalSubscriptionExistsForTopic
                            (subscribedDestination)) {
                        log.info("Purging messages of this node persisted under " + subscribedDestination);
                        removeMessagesOfDestinationForNode(subscribedDestination,null,true);
                    }
                }
                break;
        }
    }

    private void removeMessagesOfDestinationForNode(String destination,
                                                    String ownerName, boolean isTopic) throws AndesException {

        try {

            Long startMessageID = null;
            boolean clusteringEnabled = AndesContext.getInstance().isClusteringEnabled();
            //Will first retrieve the last unassigned slot id
            String nodeID = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
            //Will get the storage queue name
            String storageQueue = AndesUtils.getStorageQueueForDestination(destination, nodeID, isTopic);

            //if (clusteringEnabled) {
            //We get the relevant slot from the coordinator if running on cluster mode
            Slot unassignedSlot = MBThriftClient.getSlot(storageQueue, nodeID);
            //We need to get the starting message ID to inform the DB to start slicing the message from there
            //This step would be done in order to ensure that tombstones will not be fetched during the querying
            //operation
            startMessageID = unassignedSlot.getStartMessageId();
     /*       } else {
                //For stand-alone mode the last processed id will be kept within the slot delivery worker itself
                startMessageID = SlotDeliveryWorkerManager.getInstance().
                        getLastProcessedMessageIDForDestination(storageQueue);
                //For stand alone we need to increment by 1 to make sure the message which is processed before will
                //not overlap
                startMessageID = startMessageID + 1;
            }*/

            // This is a class used by AndesSubscriptionManager. Andes Subscription Manager is behind Disruptor layer.
            // Hence the call should be made to MessagingEngine NOT Andes.
            // Calling Andes methods from here will lead to probable deadlocks if Futures are used.
            // NOTE: purge call should be made to MessagingEngine not Andes
            if (0 < startMessageID) {
                //If the slot id is 0, which means for the given storage queue there're no unassigned slots which means
                //we don't need to purge messages in this case
                //The purpose of purge operation is to make sure that unassigned slots will be removed if no subs exists
                MessagingEngine.getInstance().purgeMessages(destination, ownerName, isTopic, startMessageID);
            }
        } catch (ConnectionException e) {
            String mesage = "Error while establishing a connection with the thrift server";
            log.error(mesage);
            throw new AndesException(mesage, e);
        }

    }
}
