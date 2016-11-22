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
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.dtx.AlreadyKnownDtxException;
import org.wso2.andes.kernel.dtx.DistributedTransaction;
import org.wso2.andes.kernel.dtx.JoinAndResumeDtxException;
import org.wso2.andes.kernel.dtx.NotAssociatedDtxException;
import org.wso2.andes.kernel.dtx.SuspendAndFailDtxException;
import org.wso2.andes.kernel.dtx.UnknownDtxBranchException;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.message.EnqueableMessage;
import org.wso2.andes.server.queue.BaseQueue;
import org.wso2.andes.server.queue.QueueEntry;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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

    public QpidDistributedTransaction() {
        distributedTransaction = Andes.getInstance().createDistributedTransaction();
    }

    @Override
    public long getTransactionStartTime() {
        return 0;
    }

    @Override
    public void addPostTransactionAction(Action postTransactionAction) {
        throw new NotImplementedException();
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

    public void prepare(Xid xid) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Preparing distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }
        // TODO
    }

    public void commit(Xid xid) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Committing distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }
        // TODO
    }

    public void rollback(Xid xid) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Committing distributed transaction " + Arrays.toString(xid.getGlobalTransactionId()));
        }
        // TODO
    }
}
