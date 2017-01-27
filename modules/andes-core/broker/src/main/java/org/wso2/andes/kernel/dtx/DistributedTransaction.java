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

package org.wso2.andes.kernel.dtx;

import org.apache.commons.lang.StringUtils;
import org.wso2.andes.amqp.QpidAndesBridge;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.server.txn.IncorrectDtxStateException;
import org.wso2.andes.server.txn.RollbackOnlyDtxException;
import org.wso2.andes.server.txn.TimeoutDtxException;

import java.util.ArrayList;
import java.util.List;
import javax.transaction.xa.Xid;

/**
 * Acts as a distributed transaction related interface for the transport layer.
 */
public class DistributedTransaction {

    private DtxRegistry dtxRegistry;
    private DtxBranch branch;
    private InboundEventManager eventManager;
    private AndesChannel channel;

    /**
     * Indicate if we should fail the transaction due to an error in enqueue dequeue stages
     */
    private volatile boolean transactionFailed = false;

    /**
     * Reasons for failing the transaction
     */
    private ArrayList<String> failedReasons = new ArrayList<>();

    public DistributedTransaction(DtxRegistry dtxRegistry, InboundEventManager eventManager, AndesChannel channel) {
        this.dtxRegistry = dtxRegistry;
        this.eventManager = eventManager;
        this.channel = channel;
    }

    public void start(long sessionID, Xid xid, boolean join, boolean resume) throws JoinAndResumeDtxException,
                                                                                    UnknownDtxBranchException,
                                                                                    AlreadyKnownDtxException {

        if (join && resume) {
            throw new JoinAndResumeDtxException(xid);
        }

        DtxBranch branch = dtxRegistry.getBranch(xid);

        if (join) {
            if (branch == null) {
                throw new UnknownDtxBranchException(xid);
            }

            this.branch = branch;
            branch.associateSession(sessionID);
        } else if (resume) {
            if (branch == null) {
                throw new UnknownDtxBranchException(xid);
            }

            this.branch = branch;
            branch.resumeSession(sessionID);
        } else {
            if (branch != null) {
                throw new AlreadyKnownDtxException(xid);
            }

            branch = new DtxBranch(sessionID, xid, dtxRegistry, eventManager);

            if (dtxRegistry.registerBranch(branch)) {
                this.branch = branch;
                 branch.associateSession(sessionID);
            } else {
                throw new AlreadyKnownDtxException(xid);
            }
        }

    }

    public void end(long sessionId, Xid xid, boolean fail, boolean suspend) throws SuspendAndFailDtxException,
                                                                                   UnknownDtxBranchException,
                                                                                   NotAssociatedDtxException {
        DtxBranch branch = dtxRegistry.getBranch(xid);
        if (suspend && fail) {
            branch.disassociateSession(sessionId);
            this.branch = null;
            throw new SuspendAndFailDtxException(xid);
        }

        if (null == branch) {
            throw new UnknownDtxBranchException(xid);
        } else if (!branch.isAssociated(sessionId)) {
            throw new NotAssociatedDtxException(xid);

            // TODO Check for transaction expiration
        } else if (suspend) {
            branch.suspendSession(sessionId);
        } else {
            if (fail) {
                branch.setState(DtxBranch.State.ROLLBACK_ONLY);
            }
            branch.disassociateSession(sessionId);
        }

        this.branch = null;
    }

    public void dequeue(List<AndesAckData> ackList) throws AndesException {
        if (isInsideStartEnd()) {
            branch.dequeueMessages(ackList);
        } else {
            for (AndesAckData ackData: ackList) {
                Andes.getInstance().ackReceived(ackData);
            }
        }
    }

    public void commit(Xid xid, boolean onePhase, Runnable callback) throws UnknownDtxBranchException,
            IncorrectDtxStateException,
            AndesException, RollbackOnlyDtxException, TimeoutDtxException {
        dtxRegistry.commit(xid, onePhase, callback, channel);
    }

    public void enqueueMessage(AndesMessage andesMessage, AndesChannel andesChannel) {
        if (isInsideStartEnd()) {
            branch.enqueueMessage(andesMessage);
        } else {
            QpidAndesBridge.messageReceived(andesMessage, andesChannel);
        }
    }

    public void prepare(Xid xid) throws TimeoutDtxException, UnknownDtxBranchException,
            IncorrectDtxStateException, AndesException, RollbackOnlyDtxException {
        if (!transactionFailed) {
            dtxRegistry.prepare(xid);
        } else {
            DtxBranch branch = dtxRegistry.getBranch(xid);
            if (branch != null) {
                branch.setState(DtxBranch.State.ROLLBACK_ONLY);
                String reason = "Transaction " + xid + " may only be rolled back due to: ";
                reason = reason + StringUtils.join(failedReasons, ",");

                throw new RollbackOnlyDtxException(reason);
            } else {
                throw new UnknownDtxBranchException(xid);
            }
        }
    }

    /**
     * Rollback the transaction session for the provided {@link Xid}
     *
     * @param xid {@link Xid} to be rollbacked
     * @throws UnknownDtxBranchException thrown when an unknown xid is provided
     * @throws AndesException thrown on internal Andes core error
     * @throws TimeoutDtxException thrown when the branch is expired
     * @throws IncorrectDtxStateException if the state of the branch is invalid. For instance the branch is not prepared
     */
    public void rollback(Xid xid)
            throws UnknownDtxBranchException, AndesException, TimeoutDtxException, IncorrectDtxStateException {
        transactionFailed = false;
        failedReasons.clear();
        dtxRegistry.rollback(xid);
    }

    /**
     * Remove the current distributed transaction related details from server. (if the transaction is not prepared)
     *
     * @param sessionId server session Id for the connected channel
     */
    public void close(long sessionId) {
        dtxRegistry.close(sessionId);
    }

    /**
     * Mark transaction as failed. We will fail the transaction at prepare stage if this was called
     *
     * @param reason reason for failure
     */
    public void failTransaction(String reason) {
        // We don't need to keep track of errors happening the transaction is not active
        if (isInsideStartEnd()) {
            transactionFailed = true;
            failedReasons.add(reason);
        }
    }

    /**
     * Indicate if we have received a dtx.start but not a dtx.end for this transaction
     *
     * @return true if inside a dtx.start and dtx.end call
     */
    private boolean isInsideStartEnd() {
        return branch != null;
    }

    /**
     * Forget about a heuristically completed transaction branch.
     *
     * @param xid XID of the branch
     * @throws UnknownDtxBranchException  if the XID is unknown to dtx registry
     * @throws IncorrectDtxStateException If the branch is in a invalid state, forgetting is not possible with
     *                                    current state
     */
    public void forget(Xid xid) throws UnknownDtxBranchException, IncorrectDtxStateException {
        dtxRegistry.forget(xid);
    }
}
