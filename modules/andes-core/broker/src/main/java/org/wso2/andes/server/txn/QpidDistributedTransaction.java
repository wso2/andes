/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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

package org.wso2.andes.server.txn;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.QpidAndesBridge;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesUtils;
import org.wso2.andes.kernel.dtx.AlreadyKnownDtxException;
import org.wso2.andes.kernel.dtx.DistributedTransaction;
import org.wso2.andes.kernel.dtx.JoinAndResumeDtxException;
import org.wso2.andes.kernel.dtx.NotAssociatedDtxException;
import org.wso2.andes.kernel.dtx.SuspendAndFailDtxException;
import org.wso2.andes.kernel.dtx.UnknownDtxBranchException;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.message.EnqueableMessage;
import org.wso2.andes.server.queue.BaseQueue;
import org.wso2.andes.server.queue.IncomingMessage;
import org.wso2.andes.server.queue.QueueEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.transaction.xa.Xid;

/**
 * Server Transaction type used to handle requests related to distributed transactions.
 */
public class QpidDistributedTransaction implements ServerTransaction {
    /**
     * Class logger
     */
    private static final Logger LOGGER = Logger.getLogger(AMQChannel.class);

    /**
     * Object used to communicate with the Andes core
     */
    private final DistributedTransaction distributedTransaction;

    public QpidDistributedTransaction(AndesChannel channel) {
        distributedTransaction = Andes.getInstance().createDistributedTransaction(channel);
    }

        /**
         * Used to keep the list of post transaction actions which are basically error messages that need to be send back
         * in commit and rollback stages
         */
        private final ConcurrentLinkedQueue<Action> postTransactionActions = new ConcurrentLinkedQueue<Action>();

    @Override
    public long getTransactionStartTime() {
        return 0;
    }

    @Override
    public void addPostTransactionAction(Action postTransactionAction) {
        postTransactionActions.add(postTransactionAction);
    }

    @Override
    public void dequeue(BaseQueue queue, EnqueableMessage message, Action postTransactionAction) {
        throw new NotImplementedException();
    }

    @Override
    public void dequeue(Collection<QueueEntry> messages, Action postTransactionAction) {
        throw new NotImplementedException();
    }

    @Override
    public void dequeue(UUID channelID, Collection<QueueEntry> messages, Action postTransactionAction)
            throws AMQException {
        List<AndesAckData> ackList = new ArrayList<>(messages.size());
        try {
            for (QueueEntry entry : messages) {
                ackList.add(AndesUtils.generateAndesAckMessage(channelID, entry.getMessage().getMessageNumber()));
            }
            distributedTransaction.dequeue(ackList);
        } catch (AndesException e) {
            throw new AMQException("Error occurred while creating a AndesAckData ", e);
        }
    }

    @Override
    public void enqueue(BaseQueue queue, EnqueableMessage message, Action postTransactionAction) {
        throw new NotImplementedException();
    }

    @Override
    public void enqueue(List<? extends BaseQueue> queues, EnqueableMessage message, Action postTransactionAction) {
        throw new NotImplementedException();
    }

    @Override
    public void commit() {
        throw new IllegalStateException("Cannot call tx.commit() on a distributed transaction");
    }

    @Override
    public void rollback() {
        throw new IllegalStateException("Cannot call tx.rollback() on a distributed transaction");
    }

    public void start(long sessionID, Xid xid, boolean join, boolean resume)
            throws JoinAndResumeDtxException, UnknownDtxBranchException, AlreadyKnownDtxException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Starting distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }
        distributedTransaction.start(sessionID, xid, join, resume);
    }

    public void end(long sessionID, Xid xid, boolean fail, boolean suspend)
            throws UnknownDtxBranchException, SuspendAndFailDtxException, NotAssociatedDtxException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Ending distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }
        distributedTransaction.end(sessionID, xid, fail, suspend);
    }

    public void prepare(Xid xid)
            throws TimeoutDtxException, UnknownDtxBranchException, IncorrectDtxStateException, AndesException,
            RollbackOnlyDtxException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Preparing distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }
        distributedTransaction.prepare(xid);
    }

    public void commit(Xid xid, Runnable callback) throws UnknownDtxBranchException,
                                                          IncorrectDtxStateException,
                                                          AndesException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Committing distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }
        try {
            distributedTransaction.commit(xid, callback);
        } finally {
            for (Action action : postTransactionActions) {
                action.postCommit();
            }
        }
    }

    public void rollback(Xid xid)
            throws UnknownDtxBranchException, AndesException, TimeoutDtxException, IncorrectDtxStateException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Rolling back distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }

        try {
            distributedTransaction.rollback(xid);
        } finally {
            for (Action action : postTransactionActions) {
                action.onRollback();
            }
        }
    }

    public void enqueueMessage(IncomingMessage incomingMessage, AndesChannel andesChannel) {
        AndesMessage andesMessage = null;

        try {
            andesMessage = QpidAndesBridge.convertToAndesMessage(incomingMessage);
        } catch (AndesException e) {
            // TODO Need to propagate error in the prepare stage.
        }

        distributedTransaction.enqueueMessage(andesMessage, andesChannel);
    }
}
