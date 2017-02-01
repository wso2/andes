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
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.DtxStore;
import org.wso2.andes.kernel.disruptor.DisruptorEventCallback;
import org.wso2.andes.server.txn.IncorrectDtxStateException;
import org.wso2.andes.server.txn.RollbackOnlyDtxException;
import org.wso2.andes.server.txn.TimeoutDtxException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    private final Map<ComparableXid, DtxBranch> branches;

    /**
     * Persistence storage used by the {@link DtxRegistry}
     */
    private final DtxStore dtxStore;

    /**
     * Executor service used to schedule transaction timeout tasks
     */
    private ScheduledExecutorService timeoutTaskExecutor;

    /**
     * Default constructor
     *
     * @param dtxStore        Store used to persist data
     */
    public DtxRegistry(DtxStore dtxStore) {
        this.dtxStore = dtxStore;
        branches = new HashMap<>();
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("DtxTimeoutExecutor-%d").build();
        timeoutTaskExecutor = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
    }

    /**
     * Get the {@link DtxBranch} relating to the given {@link Xid}
     *
     * @param xid {@link Xid} of the {@link DtxBranch} to be retrieved
     * @return DtxBranch
     */
    synchronized DtxBranch getBranch(Xid xid) {
        return branches.get(new ComparableXid(xid));
    }

    /**
     * Register a new transaction branch
     *
     * @param branch {@link DtxBranch} to be registered
     * @return True if the registration was successful and wise versa
     */
    synchronized boolean registerBranch(DtxBranch branch) {
        ComparableXid xid = new ComparableXid(branch.getXid());
        if(!branches.containsKey(xid)) {
            branches.put(xid, branch);
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
    public synchronized void prepare(Xid xid)
            throws UnknownDtxBranchException, IncorrectDtxStateException, TimeoutDtxException, AndesException,
            RollbackOnlyDtxException {
        DtxBranch branch = getBranch(xid);

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
                    branch.prepare();
                    branch.setState(DtxBranch.State.PREPARED);
                }
            } else {
                throw new IncorrectDtxStateException("Branch still has associated sessions", xid);
            }
        } else {
            throw new UnknownDtxBranchException(xid);
        }
    }

    /**
     * Un-register a branch from registry
     *
     * @param branch {@link DtxBranch} to be removed
     * @return True if successfully removed and false otherwise
     */
    private synchronized boolean unregisterBranch(DtxBranch branch) {
        return (branches.remove(new ComparableXid(branch.getXid())) != null);
    }

    /**
     * Persist Enqueue and Dequeue records in message store
     *
     * @param xid            XID of the DTX branch
     * @param enqueueRecords list of enqueue records
     * @param dequeueRecords list of dequeue records
     * @return Internal XID of the distributed transaction
     * @throws AndesException when database error occurs
     */
    long storeRecords(Xid xid, List<AndesMessage> enqueueRecords, List<AndesAckData> dequeueRecords)
            throws AndesException {
        return dtxStore.storeDtxRecords(xid, enqueueRecords, dequeueRecords);
    }

    /**
     * Rollback the transaction session for the provided {@link Xid}
     *
     * @param xid {@link Xid} of the branch to be rollbacked
     * @throws UnknownDtxBranchException thrown when an unknown xid is provided
     * @throws AndesException thrown on internal Andes core error
     * @throws TimeoutDtxException thrown when the branch is expired
     * @throws IncorrectDtxStateException if the state of the branch is invalid. For instance the branch is not prepared
     */
    public synchronized void rollback(Xid xid)
            throws TimeoutDtxException, IncorrectDtxStateException, UnknownDtxBranchException, AndesException {
        DtxBranch branch = getBranch(xid);

        if (branch != null) {
            if (branch.expired()) {
                unregisterBranch(branch);
                throw new TimeoutDtxException(xid);
            }

            if (!branch.hasAssociatedActiveSessions()) {
                branch.clearAssociations();
                branch.rollback();
                branch.setState(DtxBranch.State.FORGOTTEN);
                unregisterBranch(branch);
            } else {
                throw new IncorrectDtxStateException("Branch is still associates with a session", xid);
            }
        } else {
            throw new UnknownDtxBranchException(xid);
        }
    }

    /**
     * Remove the records for given {@link Xid} from the {@link DtxStore}
     * @param internalXid internal {@link Xid}
     * @throws AndesException Throws when there is a storage related exception
     */
    void removePreparedRecords(long internalXid) throws AndesException {
        dtxStore.removeDtxRecords(internalXid);
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
    public void commit(Xid xid, boolean onePhase, DisruptorEventCallback callback, AndesChannel channel)
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

                dtxBranch.commit(callback, channel);
                dtxBranch.setState(DtxBranch.State.FORGOTTEN);
                unregisterBranch(dtxBranch);
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
    public void forget(Xid xid) throws UnknownDtxBranchException, IncorrectDtxStateException {
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

        for (Iterator<Map.Entry<ComparableXid, DtxBranch>> iterator = branches.entrySet().iterator();
             iterator.hasNext(); ) {

            Map.Entry<ComparableXid, DtxBranch> entry = iterator.next();
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
    public void setTimeout(Xid xid, long timeout) throws UnknownDtxBranchException {
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

    private static final class ComparableXid {
        private final Xid xid;

        private ComparableXid(Xid xid) {
            this.xid = xid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ComparableXid that = (ComparableXid) o;

            return compareBytes(xid.getBranchQualifier(), that.xid.getBranchQualifier()) && compareBytes(
                    xid.getGlobalTransactionId(), that.xid.getGlobalTransactionId());
        }

        private static boolean compareBytes(byte[] a, byte[] b) {
            if (a.length != b.length) {
                return false;
            }
            for (int i = 0; i < a.length; i++) {
                if (a[i] != b[i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = 0;
            for (int i = 0; i < xid.getGlobalTransactionId().length; i++) {
                result = 31 * result + (int) xid.getGlobalTransactionId()[i];
            }
            for (int i = 0; i < xid.getBranchQualifier().length; i++) {
                result = 31 * result + (int) xid.getBranchQualifier()[i];
            }

            return result;
        }
    }
}
