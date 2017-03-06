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
import org.wso2.andes.kernel.disruptor.DisruptorEventCallback;
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

    /**
     * Used communicate transaction creation and release
     */
    private final Andes andes;

    public QpidDistributedTransaction(AndesChannel channel, long sessionID) throws AndesException {
        andes = Andes.getInstance();
        distributedTransaction = andes.createDistributedTransaction(channel, sessionID);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback() {
        throw new IllegalStateException("Cannot call tx.rollback() on a distributed transaction");
    }

    public void start(long sessionID, Xid xid, boolean join, boolean resume)
            throws JoinAndResumeDtxException, UnknownDtxBranchException, AlreadyKnownDtxException, AndesException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Starting distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }
        distributedTransaction.start(sessionID, xid, join, resume);
    }

    public void end(long sessionID, Xid xid, boolean fail, boolean suspend)
            throws UnknownDtxBranchException, SuspendAndFailDtxException, NotAssociatedDtxException,
                   TimeoutDtxException, AndesException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Ending distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }
        distributedTransaction.end(sessionID, xid, fail, suspend);
    }

    public void prepare(Xid xid, DisruptorEventCallback callback)
            throws TimeoutDtxException, UnknownDtxBranchException, IncorrectDtxStateException, AndesException,
            RollbackOnlyDtxException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Preparing distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }
        try {
            distributedTransaction.prepare(xid, callback);
        } finally {
            for (Action action : postTransactionActions) {
                action.postCommit();
            }
        }
    }

    public void commit(Xid xid, boolean onePhase, DisruptorEventCallback callback) throws UnknownDtxBranchException,
                                                                                          IncorrectDtxStateException, AndesException, TimeoutDtxException, RollbackOnlyDtxException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Committing distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }
        distributedTransaction.commit(xid, onePhase, callback);
    }

    /**
     * Rewind the transaction state to the point where transaction was started.
     *
     * @param xid
     * @param callback
     * @throws UnknownDtxBranchException
     * @throws AndesException
     * @throws TimeoutDtxException
     * @throws IncorrectDtxStateException
     */
    public void rollback(Xid xid, DisruptorEventCallback callback)
            throws UnknownDtxBranchException, AndesException, TimeoutDtxException, IncorrectDtxStateException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Rolling back distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }

        try {
            distributedTransaction.rollback(xid, callback);
        } finally {
            for (Action action : postTransactionActions) {
                action.onRollback();
            }
        }
    }

    /**
     * Store messages published within a transaction in memory until the {@link #prepare(Xid, DisruptorEventCallback)} is invoked
     *
     * @param incomingMessage {@link IncomingMessage} to be enqueued
     * @param andesChannel {@link AndesChannel} object related to the incoming message
     */
    public void enqueueMessage(IncomingMessage incomingMessage, AndesChannel andesChannel) {
        try {
            AndesMessage andesMessage = QpidAndesBridge.convertToAndesMessage(incomingMessage);
            distributedTransaction.enqueueMessage(andesMessage, andesChannel);
        } catch (AndesException e) {
            LOGGER.warn("Converting incoming message to Andes message failed", e);
            failTransaction(e.getMessage());
        }
    }

    /**
     * Release resources allocated for distributed transaction in the core
     *
     * @param sessionId corresponding session ID
     */
    public void close(long sessionId) {
        try {
            distributedTransaction.close(sessionId);
        } finally {
            andes.releaseDistributedTransaction(sessionId);
        }
    }

    /**
     * Mark transaction as failed. We will fail the transaction at prepare stage if this was called
     *
     * @param reason reason for failure
     */
    public void failTransaction(String reason) {
        distributedTransaction.failTransaction(reason);
    }

    /**
     * Forget about a heuristically completed transaction branch.
     *
     * @param xid XID of the branch
     * @throws UnknownDtxBranchException  if the XID is unknown to dtx registry
     * @throws IncorrectDtxStateException If the branch is in a invalid state, forgetting is not possible with
     *                                    current state
     */
    public void forget(Xid xid) throws UnknownDtxBranchException, IncorrectDtxStateException, AndesException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Forgetting the distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }

        distributedTransaction.forget(xid);
    }

    /**
     * Set transaction timeout for current distributed transaction
     *
     * @param xid     XID of the dtx branch
     * @param timeout timeout value that should be set
     */
    public void setTimeout(Xid xid, long timeout) throws UnknownDtxBranchException, AndesException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Setting timeout" + timeout + "for the distributed transaction " + Arrays.toString(xid
                                                                                                                    .getGlobalTransactionId()));
        }

        distributedTransaction.setTimeout(xid, timeout);
    }
}
