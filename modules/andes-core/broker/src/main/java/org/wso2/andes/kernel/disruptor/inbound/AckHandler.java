/*
 * Copyright (c) 2014-2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.store.AndesTransactionRollbackException;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;

import java.util.ArrayList;
import java.util.List;

/**
 * Acknowledgement Handler for the Disruptor based inbound event handling.
 * This handler processes acknowledgements received from clients and updates Andes.
 */
public class AckHandler implements StoreHealthListener {

    private static Log log = LogFactory.getLog(AckHandler.class);
    
    private final MessagingEngine messagingEngine;

    private final AndesSubscriptionManager subscriptionManager;

    /**
     * Maximum number to retries to delete messages from message store
     */
    private static final int MAX_MESSAGE_DELETION_COUNT = 5;

    /**
     * Indicates and provides a barrier if messages stores become offline.
     * marked as volatile since this value could be set from a different thread
     * (other than those of disrupter)
     */
    private volatile boolean messageStoresUnavailable;
    
    /**
     * Keeps message meta-data that needs to be removed from the message store.
     */
    private final List<DeliverableAndesMetadata> messagesToRemove;
    
    AckHandler(MessagingEngine messagingEngine) {
        this.messagingEngine = messagingEngine;
        this.subscriptionManager = AndesContext.getInstance().getAndesSubscriptionManager();
        this.messageStoresUnavailable = false;
        this.messagesToRemove = new ArrayList<>();
        FailureObservingStoreManager.registerStoreHealthListener(this);
    }

    /**
     * Process the acknowledgment events and delete messages from database
     * @param ackDataList {@link List} of {@link AndesAckData}
     * @throws Exception
     */
    public void processAcknowledgements(final List<AndesAckData> ackDataList) throws Exception {
        if (log.isTraceEnabled()) {
            StringBuilder messageIDsString = new StringBuilder();
            for (AndesAckData andesAckData : ackDataList) {
                messageIDsString.append(andesAckData.getIdOfAcknowledgedMessage()).append(" , ");
            }
            log.trace(ackDataList.size() + " messages received : " + messageIDsString);
        }
        if (log.isDebugEnabled()) {
            log.debug(ackDataList.size() + " acknowledgements received from disruptor.");
        }

        try {
            ackReceived(ackDataList);
        } catch (AndesException e) {
            // Log the AndesException since there is no point in passing the exception to Disruptor
            log.error("Error occurred while processing acknowledgements ", e);
        }
    }

    /**
     * Updates the state of Andes and deletes relevant messages. (For topics
     * message deletion will happen only when
     * all the clients acknowledges)
     *
     * @param ackDataList
     *            inboundEvent list
     */
    public void ackReceived(final List<AndesAckData> ackDataList) throws AndesException {
        
        for (AndesAckData ack : ackDataList) {

            ack.setMeradataReference();

            // For topics message is shared. If all acknowledgements are received only we should remove message
            boolean deleteMessage = ack.getMetadataReference().markAsAcknowledgedByChannel(ack.getChannelID());

            AndesSubscription subscription = subscriptionManager
                    .getSubscriptionByProtocolChannel(ack.getChannelID());

            subscription.onMessageAck(ack.getIdOfAcknowledgedMessage());

            if (deleteMessage) {
                if (log.isDebugEnabled()) {
                    log.debug("Ok to delete message id " + ack.getIdOfAcknowledgedMessage());
                }
                //it is a must to set this to event container. Otherwise, multiple event handlers will see the status
                ack.setBaringMessageRemovable();
                messagesToRemove.add(ack.getMetadataReference());
            }
            
        }

        /*
         * Checks for the message store availability. Messages will be deleted only if the store is available.
         */
        if (!messageStoresUnavailable) {
            deleteMessagesFromStore(0);
        }
    }

    /**
     * Delete acknowledged messages from message store. Deletion is retried if it failed due to a
     * AndesTransactionRollbackException.
     *
     * @param numberOfRetriesBefore
     *         number of recursive calls
     * @throws AndesException
     */
    private void deleteMessagesFromStore(int numberOfRetriesBefore) throws AndesException {
        try {
            messagingEngine.deleteMessages(messagesToRemove);

            if (log.isTraceEnabled()) {
                StringBuilder messageIDsString = new StringBuilder();
                for (DeliverableAndesMetadata metadata : messagesToRemove) {
                    messageIDsString.append(metadata.getMessageID()).append(" , ");
                }
                log.trace(messagesToRemove.size() + " message ok to remove : " + messageIDsString);
            }
            messagesToRemove.clear();
        } catch (AndesTransactionRollbackException txRollback) {
            if (numberOfRetriesBefore <= MAX_MESSAGE_DELETION_COUNT) {

                log.warn("unable to delete messages (" + messagesToRemove.size()
                         + "), due to transaction roll back. Operation will be attempted again", txRollback);
                deleteMessagesFromStore(numberOfRetriesBefore + 1);
            } else {
                throw new AndesException("Unable to delete acked messages, in final attempt " + numberOfRetriesBefore
                                         + ". This might lead to message duplication.");
            }
        } catch (AndesException ex) {
            log.warn(String.format(
                    "unable to delete messages, probably due to errors in message stores.messages count : %d, "
                    + "operation will be attempted again",
                    messagesToRemove.size()));
            throw ex;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Creates a {@link SettableFuture} indicating message store became offline.
     */
    @Override
    public void storeNonOperational(HealthAwareStore store, Exception ex) {
        log.info(String.format("Message store became not operational. messages to delete : %d",
                messagesToRemove.size()));
        messageStoresUnavailable = true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Sets a value for {@link SettableFuture} indicating message store became
     * online.
     */
    @Override
    public void storeOperational(HealthAwareStore store) {
        log.info(String.format("Message store became operational. messages to delete : %d",
                messagesToRemove.size()));
        messageStoresUnavailable = false;
    }
}
