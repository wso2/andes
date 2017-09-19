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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.QpidAndesBridge;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.disruptor.DisruptorEventCallback;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.server.txn.IncorrectDtxStateException;
import org.wso2.andes.server.txn.RollbackOnlyDtxException;
import org.wso2.andes.server.txn.TimeoutDtxException;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.transaction.xa.Xid;

/**
 * Acts as a distributed transaction related interface for the transport layer.
 */
public class DistributedTransaction {

    /**
     * Class logger
     */
    private static final Log LOGGER = LogFactory.getLog(DistributedTransaction.class);

    /**
     * Max amount of memory allowed to be used for keeping message content per transaction
     */
    private final long maxTotalMessageSizeAllowed;

    /**
     * Reference to the {@link DtxRegistry} which stores details related to all the active distributed transactions in
     * the broker
     */
    private final DtxRegistry dtxRegistry;

    /**
     * Reference to distributed transaction related branch for this specific transaction
     */
    private DtxBranch branch;

    /**
     * Reference to event manager to push event to Disruptor
     */
    private final InboundEventManager eventManager;

    /**
     * Reference to the {@link AndesChannel} related to the transaction
     */
    private final AndesChannel channel;

    /**
     * Indicate if we should fail the transaction due to an error in enqueue dequeue stages
     */
    private volatile boolean transactionFailed = false;

    /**
     * Reasons for failing the transaction
     */
    private final ArrayList<String> failedReasons = new ArrayList<>();

    /**
     * Total memory consumed by content of the published messages belonging to this transaction
     */
    private long totalMessageSize = 0;

    public DistributedTransaction(DtxRegistry dtxRegistry, InboundEventManager eventManager, AndesChannel channel) {
        this.dtxRegistry = dtxRegistry;
        this.eventManager = eventManager;
        this.channel = channel;
        //noinspection ConstantConditions
        maxTotalMessageSizeAllowed =
                (Integer) AndesConfigurationManager.readValue(AndesConfiguration.MAX_TRANSACTION_BATCH_SIZE) * 1024;
    }

    /**
     * Begin a distributed transaction for the given {@link Xid} from the provided session
     *
     * @param sessionID Session id related to the transaction.
     * @param xid {@link Xid} of the {@link DtxBranch}
     * @param join True if join this transaction to a already existing {@link DtxBranch} with the same  {@link Xid}
     * @param resume Resume a suspended transaction with the given {@link Xid}
     * @throws JoinAndResumeDtxException Thrown when both join and resume flags are set
     * @throws UnknownDtxBranchException Thrown when the provided {@link Xid} is not known when the join flag is set
     * @throws AlreadyKnownDtxException Thrown when the provided {@link Xid} already exist and the join flag is not set
     */
    public void start(UUID sessionID, Xid xid, boolean join, boolean resume) throws JoinAndResumeDtxException,
                                                                                    UnknownDtxBranchException,
                                                                                    AlreadyKnownDtxException,
                                                                                    AndesException {

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

    /**
     * End a transaction to prepare/commit/rollback the transaction
     *
     * @param sessionId session related to the transaction
     * @param xid {@link Xid} of the {@link DtxBranch}
     * @param fail whether the ended transaction to be marked as failed. If set the transaction will be marked as
     *             ROLLBACK_ONLY
     * @param suspend if set the transaction will be suspended
     * @throws SuspendAndFailDtxException Thrown when both suspend and fail flags are set
     * @throws UnknownDtxBranchException Thrown when the provided {@link Xid} is not known by the broker.
     * @throws NotAssociatedDtxException Thrown when the provided {@link Xid} is not associated to the provided
     *                                   sessionId
     * @throws TimeoutDtxException Thrown when the {@link DtxBranch} has timed out
     */
    public void end(UUID sessionId, Xid xid, boolean fail, boolean suspend) throws SuspendAndFailDtxException,
                                                                                   UnknownDtxBranchException,
                                                                                   NotAssociatedDtxException,
                                                                                   TimeoutDtxException,
                                                                                   AndesException {
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
        } else if (branch.expired()) {
            branch.disassociateSession(sessionId);
            throw new TimeoutDtxException(xid);
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

    /**
     * Acknowledge message within a transaction. Acknowledged messages will be kept in memory until commit/rollback
     * is done
     *
     * @param ackList {@link List} of {@link AndesAckData}
     * @throws AndesException if internal error while calling ack received
     */
    public void dequeue(List<AndesAckData> ackList) throws AndesException {
        if (isInsideStartEnd()) {
            branch.dequeueMessages(ackList);
        } else {
            for (AndesAckData ackData: ackList) {
                Andes.getInstance().ackReceived(ackData);
            }
        }
    }

    /**
     * Commit the transaction for the provided {@link Xid}
     *
     * @param xid {@link Xid} of the transaction
     * @param onePhase True if this a one phase commit.
     * @param callback {@link DisruptorEventCallback} to be called when the commit is done from the Disruptor end
     * @throws UnknownDtxBranchException Thrown when the {@link Xid} is unknown
     * @throws IncorrectDtxStateException Thrown when the state transition of the {@link DtxBranch} is invalid
     *                                    For instance invoking commit without invoking prepare for a two phase commit
     * @throws AndesException Thrown when and internal exception occur
     * @throws RollbackOnlyDtxException Thrown when commit is invoked on a ROLLBACK_ONLY {@link DtxBranch}
     * @throws TimeoutDtxException Thrown when the respective {@link DtxBranch} relating to the {@link Xid} has expired
     */
    public void commit(Xid xid, boolean onePhase, DisruptorEventCallback callback) throws UnknownDtxBranchException,
                                                                                          IncorrectDtxStateException,
                                                                                          AndesException,
                                                                                          RollbackOnlyDtxException, TimeoutDtxException {
        totalMessageSize = 0;
        dtxRegistry.commit(xid, onePhase, callback, channel);
    }

    /**
     * Add {@link AndesMessage} to the transaction. Messages will be kept in memory until the transaction is committed
     * or rollback
     *
     * @param andesMessage {@link AndesMessage} published
     * @param andesChannel {@link AndesChannel} related to the transaction
     */
    public void enqueueMessage(AndesMessage andesMessage, AndesChannel andesChannel) {
        if (isInsideStartEnd()) {
            totalMessageSize = totalMessageSize + andesMessage.getMetadata().getMessageContentLength();

            if (totalMessageSize < maxTotalMessageSizeAllowed) {
                branch.enqueueMessage(andesMessage);
                if (MessageTracer.isEnabled()) {
                    MessageTracer.trace(andesMessage.getMetadata(), branch.getXid(),
                                        branch.getState(), MessageTracer.ENQUEUED_DTX_MESSAGE);
                }
            } else {
                if (!transactionFailed) {
                    branch.clearEnqueueList();
                    long totalMessageSizeInKB = totalMessageSize / 1024L;
                    failTransaction("Current total message size " + totalMessageSizeInKB + " KB exceeds max total "
                                            + "message size allowed per transaction");
                }
            }
        } else {
            try {
                QpidAndesBridge.messageReceived(andesMessage, andesChannel);
            } catch (AndesException e) {
                LOGGER.error("Non-transactional message publishing failed", e);
            }
        }
    }

    /**
     * Prepare the {@link DtxBranch} for a commit or rollback. This will persist all the transaction related
     * acknowledgements and published messages to a temporary database location
     *
     * @param xid {@link Xid} related to the transaction
     * @param callback {@link DisruptorEventCallback}
     * @throws TimeoutDtxException Thrown when the {@link DtxBranch} has expired
     * @throws UnknownDtxBranchException Thrown when the provided {@link Xid} is not known to the broker
     * @throws IncorrectDtxStateException Thrown when invoking prepare for the {@link DtxBranch} is invalid from
     *                                    current state of the {@link DtxBranch}
     * @throws AndesException   Thrown when an internal error occur
     * @throws RollbackOnlyDtxException Thrown when the branch is set to ROLLBACK_ONLY
     */
    public void prepare(Xid xid, DisruptorEventCallback callback) throws TimeoutDtxException, UnknownDtxBranchException,
            IncorrectDtxStateException, AndesException, RollbackOnlyDtxException {
        if (!transactionFailed) {
            dtxRegistry.prepare(xid, callback);
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
     * @param xid {@link Xid} to be rolled back
     * @param callback {@link DisruptorEventCallback}
     * @throws UnknownDtxBranchException thrown when an unknown xid is provided
     * @throws AndesException thrown on internal Andes core error
     * @throws TimeoutDtxException thrown when the branch is expired
     * @throws IncorrectDtxStateException if the state of the branch is invalid. For instance the branch is not prepared
     */
    public void rollback(Xid xid, DisruptorEventCallback callback)
            throws UnknownDtxBranchException, AndesException, TimeoutDtxException, IncorrectDtxStateException {
        totalMessageSize = 0;
        transactionFailed = false;
        failedReasons.clear();
        dtxRegistry.rollback(xid, callback);
    }

    /**
     * Remove the current distributed transaction related details from server. (if the transaction is not prepared)
     *
     * @param sessionId server session Id for the connected channel
     */
    public void close(UUID sessionId) {
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
    public void forget(Xid xid) throws UnknownDtxBranchException, IncorrectDtxStateException, AndesException {
        dtxRegistry.forget(xid);
    }

    /**
     * Set transaction timeout for current distributed transaction
     *
     * @param xid     XID of the dtx branch
     * @param timeout timeout value that should be set
     */
    public void setTimeout(Xid xid, long timeout) throws UnknownDtxBranchException, AndesException {
        dtxRegistry.setTimeout(xid, timeout);
    }
}
