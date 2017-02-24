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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.wso2.andes.dtx.XidImpl;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DtxStore;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.disruptor.DisruptorEventCallback;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.txn.IncorrectDtxStateException;
import org.wso2.andes.server.txn.RollbackOnlyDtxException;
import org.wso2.andes.server.txn.TimeoutDtxException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.transaction.xa.Xid;

/**
 * In memory registry for distributed transaction related operations
 */
public class DtxRegistry {

    /**
     * {@link Xid} to {@link DtxBranch} mapping
     */
    private final Map<Xid, DtxBranch> branches;

    /**
     * Persistence storage used by the {@link DtxRegistry}
     */
    private final DtxStore dtxStore;

    /**
     * Executor service used to schedule transaction timeout tasks
     */
    private ScheduledExecutorService timeoutTaskExecutor;

    /**
     * Reference to the core messaging service of the broker
     */
    private MessagingEngine messagingEngine;

    /**
     * Node id of the server
     */
    private String nodeId;

    /**
     * {@link InboundEventManager} that is used to process commit/rollback events
     */
    private InboundEventManager eventManager;

    /**
     * Xids of branches that are not cached in branches map but are stored in database
     */
    private Set<XidImpl> storeOnlyXidSet;

    /**
     * Default constructor
     *
     * @param dtxStore        Store used to persist data
     */
    public DtxRegistry(DtxStore dtxStore, MessagingEngine messagingEngine, InboundEventManager eventManager)
            throws AndesException {
        this.dtxStore = dtxStore;
        branches = new HashMap<>();
        this.messagingEngine = messagingEngine;
        this.eventManager = eventManager;
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("DtxTimeoutExecutor-%d").build();
        nodeId = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        storeOnlyXidSet = dtxStore.getStoredXidSet(nodeId);
        timeoutTaskExecutor = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
    }

    /**
     * Get the {@link DtxBranch} relating to the given {@link Xid}
     *
     * @param xid {@link Xid} of the {@link DtxBranch} to be retrieved
     * @return DtxBranch
     */
    synchronized DtxBranch getBranch(Xid xid) throws AndesException {
        DtxBranch dtxBranch = branches.get(xid);
        if (dtxBranch == null && storeOnlyXidSet.contains(new XidImpl(xid))) {

            dtxBranch = new DtxBranch(DtxBranch.RECOVERY_SESSION_ID, xid, this, eventManager);
            if (!dtxBranch.recoverFromStore(nodeId)) {
                dtxBranch = null;
            }
        }
        return dtxBranch;
    }

    /**
     * Register a new transaction branch
     *
     * @param branch {@link DtxBranch} to be registered
     * @return True if the registration was successful and wise versa
     */
    synchronized boolean registerBranch(DtxBranch branch) {
        if(!branches.containsKey(branch.getXid())) {
            branches.put(branch.getXid(), branch);
            return true;
        }
        return false;
    }

    /**
     * Prepare the {@link DtxBranch} for a commit or rollback. This will persist all the transaction related
     * acknowledgements and published messages to a temporary database location
     *
     * @param xid {@link Xid} related to the transaction
     * @throws TimeoutDtxException Thrown when the {@link DtxBranch} has expired
     * @throws UnknownDtxBranchException Thrown when the provided {@link Xid} is not known to the broker
     * @throws IncorrectDtxStateException Thrown when invoking prepare for the {@link DtxBranch} is invalid from
     *                                    current state of the {@link DtxBranch}
     * @throws AndesException   Thrown when an internal error occur
     * @throws RollbackOnlyDtxException Thrown when the branch is set to ROLLBACK_ONLY
     */
    public void prepare(Xid xid)
            throws UnknownDtxBranchException, IncorrectDtxStateException, TimeoutDtxException, AndesException,
            RollbackOnlyDtxException {
        DtxBranch branch;

        synchronized (this) {
            branch = getBranch(xid);

            if (branch != null) {
                if (!branch.hasAssociatedActiveSessions()) {
                    branch.clearAssociations();

                    if (branch.expired()) {
                        unregisterBranch(branch);
                        throw new TimeoutDtxException(xid);
                    } else if (branch.getState() == DtxBranch.State.ROLLBACK_ONLY) {
                        throw new RollbackOnlyDtxException(xid);
                    } else if (branch.getState() != DtxBranch.State.ACTIVE) {
                        throw new IncorrectDtxStateException("Cannot prepare a transaction in state " + branch.getState(),
                        xid);
                    } else {
                        branch.setState(DtxBranch.State.PRE_PREPARE);
                    }
                } else {
                    throw new IncorrectDtxStateException("Branch still has associated sessions", xid);
                }
            } else {
                throw new UnknownDtxBranchException(xid);
            }
        }

        // Note: Branch should only be in PREPARED state to call branch.prepare()
        // TODO: add the call to disruptor
        try {
            branch.prepare();
            branch.setState(DtxBranch.State.PREPARED);
        } catch (AndesException e) {
            branch.setState(DtxBranch.State.ROLLBACK_ONLY);
            throw e;
        }
    }

    /**
     * Un-register a branch from registry
     *
     * @param branch {@link DtxBranch} to be removed
     * @return True if successfully removed and false otherwise
     */
    private synchronized boolean unregisterBranch(DtxBranch branch) {
        return (branches.remove(branch.getXid()) != null);
    }

    /**
     * Persist Enqueue and Dequeue records in message store
     *
     * @param branch {@link DtxBranch} that needs to be stored
     * @return Internal XID of the distributed transaction
     * @throws AndesException when database error occurs
     */
    long storeRecords(DtxBranch branch)
            throws AndesException {
        List<AndesPreparedMessageMetadata> dequeueRecords =
                messagingEngine.acknowledgeOnDtxPrepare(branch.getDequeueList());
        branch.setMessagesToRestore(dequeueRecords);
        return dtxStore.storeDtxRecords(branch.getXid(), branch.getEnqueueList(), dequeueRecords);

    }

    /**
     * Rollback the transaction session for the provided {@link Xid}
     *
     * @param xid {@link Xid} of the branch to be rollbacked
     * @param callback {@link DisruptorEventCallback}
     * @throws UnknownDtxBranchException thrown when an unknown xid is provided
     * @throws AndesException thrown on internal Andes core error
     * @throws TimeoutDtxException thrown when the branch is expired
     * @throws IncorrectDtxStateException if the state of the branch is invalid. For instance the branch is not prepared
     */
    public synchronized void rollback(Xid xid, DisruptorEventCallback callback)
            throws TimeoutDtxException, IncorrectDtxStateException, UnknownDtxBranchException, AndesException {
        DtxBranch branch = getBranch(xid);
        if (branch != null) {
            if (branch.expired()) {
                unregisterBranch(branch);
                throw new TimeoutDtxException(xid);
            }

            if (!branch.hasAssociatedActiveSessions()) {
                branch.clearAssociations();
                branch.rollback(new RollbackCallback(callback, branch));
            } else {
                throw new IncorrectDtxStateException("Branch is still associates with a session", xid);
            }
        } else {
            throw new UnknownDtxBranchException(xid);
        }
    }

    /**
     * Commit the transaction for the provided {@link Xid}
     *
     * @param xid {@link Xid} of the transaction
     * @param onePhase True if this a one phase commit.
     * @param callback {@link DisruptorEventCallback} to be called when the commit is done from the Disruptor end
     * @throws UnknownDtxBranchException Thrown when the {@link Xid} is unknown
     * @throws IncorrectDtxStateException Thrown when the state transistion of the {@link DtxBranch} is invalid
     *                                    For instance invoking commit without invoking prepare for a two phase commit
     * @throws AndesException Thrown when and internal exception occur
     * @throws RollbackOnlyDtxException Thrown when commit is invoked on a ROLLBACK_ONLY {@link DtxBranch}
     * @throws TimeoutDtxException Thrown when the respective {@link DtxBranch} relating to the {@link Xid} has expired
     */
    public synchronized void commit(Xid xid, boolean onePhase, DisruptorEventCallback callback, AndesChannel channel)
            throws UnknownDtxBranchException, IncorrectDtxStateException, AndesException, TimeoutDtxException,
            RollbackOnlyDtxException {

        DtxBranch dtxBranch = getBranch(xid);
        if (null != dtxBranch) {
            if (!dtxBranch.hasAssociatedActiveSessions()) {
                // TODO: Need to revisit. What happens if the commit DB call fail?
                dtxBranch.clearAssociations();

                if (dtxBranch.expired()) {
                    unregisterBranch(dtxBranch);
                    throw new TimeoutDtxException(xid);
                } else if (dtxBranch.getState() == DtxBranch.State.ROLLBACK_ONLY) {
                    throw new RollbackOnlyDtxException(xid);
                } else if (onePhase && dtxBranch.getState() == DtxBranch.State.PREPARED) {
                    throw new IncorrectDtxStateException("Cannot call one-phase commit on a prepared branch", xid);
                } else if (!onePhase && dtxBranch.getState() != DtxBranch.State.PREPARED) {
                    throw new IncorrectDtxStateException("Cannot call two-phase commit on a non-prepared branch", xid);
                }

                dtxBranch.setState(DtxBranch.State.FORGOTTEN);

                DisruptorEventCallback wrappedCallback = new CommitCallback(callback, dtxBranch);
                dtxBranch.commit(wrappedCallback, channel);

            } else {
                throw new IncorrectDtxStateException("Branch still has associated sessions", xid);
            }
        } else {
            throw new UnknownDtxBranchException(xid);
        }
    }

    /**
     * Retrieve the underlying persistence store for dtx information
     * @return DtxStore
     */
    public DtxStore getStore() {
        return dtxStore;
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
        DtxBranch branch = getBranch(xid);
        if (branch != null) {
            synchronized (branch) {
                if (!branch.hasAssociatedActiveSessions()) {
                    if(branch.getState() != DtxBranch.State.HEUR_COM && branch.getState() != DtxBranch.State.HEUR_RB) {
                        throw new IncorrectDtxStateException("Branch should not be forgotten - "
                                                                     + "it is not heuristically complete", xid);
                    }
                    branch.setState(DtxBranch.State.FORGOTTEN);
                    unregisterBranch(branch);
                } else {
                    throw new IncorrectDtxStateException("Branch was still associated with a session", xid);
                }
            }
        } else {
            throw new UnknownDtxBranchException(xid);
        }
    }

    /**
     * Remove all the branches associated with the dtx session that not persisted
     *
     * @param sessionId sessionId of the closing transactional session
     */
    public void close(long sessionId) {

        for (Iterator<Map.Entry<Xid, DtxBranch>> iterator = branches.entrySet().iterator();
             iterator.hasNext(); ) {

            Map.Entry<Xid, DtxBranch> entry = iterator.next();
            DtxBranch branch = entry.getValue();

            if (branch.getCreatedSessionId() == sessionId &&
                    (branch.getState() == DtxBranch.State.ACTIVE || branch.getState() == DtxBranch.State.SUSPENDED)) {

                // If there are no associations delete the entry. If there are associations that is due to a join.
                // Hence only disassociating the session
                if (branch.isAssociated(sessionId)) {
                    branch.disassociateSession(sessionId);
                }
                if (!branch.hasAssociatedSessions()) {
                    branch.setState(DtxBranch.State.FORGOTTEN);
                    iterator.remove();
                }
            }
        }
    }

    /*
     * Set the transaction timeout of the matching dtx branch
     *
     * @param xid     XID of the dtx branch
     * @param timeout transaction timeout value in seconds
     * @throws UnknownDtxBranchException
     */
    public void setTimeout(Xid xid, long timeout) throws UnknownDtxBranchException, AndesException {
        DtxBranch branch = getBranch(xid);
        if (branch != null) {
            branch.setTimeout(timeout);
        } else {
            throw new UnknownDtxBranchException(xid);
        }
    }

    /**
     * Schedule a timeout tasks
     *
     * @param delay    task delay in milliseconds
     * @param runnable tasks to run after the delay
     * @return ScheduledFuture object matching the scheduled task
     */
    ScheduledFuture<?> scheduleTask(long delay, Runnable runnable) {
        return timeoutTaskExecutor.schedule(runnable, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Stop Executors started by the {@link DtxRegistry}
     */
    public void stop() {
        timeoutTaskExecutor.shutdown();
    }

    /**
     * Return list of XIDs belonging to dtx branches in prepared state
     *
     * @return list of XIDs in prepared state
     */
    public ArrayList<Xid> getPreparedTransactions() {
        ArrayList<Xid> preparedBranches = new ArrayList<>();

        for (DtxBranch dtxBranch : branches.values()) {
            if (dtxBranch.getState() == DtxBranch.State.PREPARED) {
                preparedBranches.add(dtxBranch.getXid());
            }
        }

        return preparedBranches;
    }

    /**
     * DisruptorEventCallback implementation for dtx commit event
     */
    private class CommitCallback implements DisruptorEventCallback {

        /**
         * callback given by Distributed Transaction object
         */
        private final DisruptorEventCallback callback;

        /**
         * Corresponding DTX branch
         */
        private final DtxBranch dtxBranch;

        private CommitCallback(DisruptorEventCallback callback, DtxBranch dtxBranch) {
            this.callback = callback;
            this.dtxBranch = dtxBranch;
        }

        @Override
        public void execute() {
            synchronized (DtxRegistry.this) {
                unregisterBranch(dtxBranch);
                storeOnlyXidSet.remove(new XidImpl(dtxBranch.getXid()));
            }

            callback.execute();
        }

        @Override
        public void onException(Exception exception) {
            synchronized (DtxRegistry.this) {
                dtxBranch.setState(DtxBranch.State.PREPARED);
            }

            callback.onException(exception);
        }
    }

    /**
     * DisruptorEventCallback implementation for dtx rollback event
     */
    private class RollbackCallback implements DisruptorEventCallback {

        /**
         * callback given by Distributed Transaction object
         */
        private final DisruptorEventCallback callback;

        /**
         * Corresponding DTX branch
         */
        private final DtxBranch dtxBranch;

        public RollbackCallback(DisruptorEventCallback callback, DtxBranch dtxBranch) {
            this.callback = callback;
            this.dtxBranch = dtxBranch;
        }

        @Override
        public void execute() {
            synchronized (DtxRegistry.this) {
                dtxBranch.setState(DtxBranch.State.FORGOTTEN);
                unregisterBranch(dtxBranch);
                storeOnlyXidSet.remove(new XidImpl(dtxBranch.getXid()));
            }
            callback.execute();
        }

        @Override
        public void onException(Exception exception) {
            synchronized (DtxRegistry.this) {
                dtxBranch.setState(DtxBranch.State.PREPARED);
            }

            callback.onException(exception);
        }
    }

}
