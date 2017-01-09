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

import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.DtxStore;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.txn.IncorrectDtxStateException;
import org.wso2.andes.server.txn.TimeoutDtxException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.transaction.xa.Xid;

public class DtxRegistry {

    private final Map<ComparableXid, DtxBranch> branches;

    private final DtxStore dtxStore;

    private final MessagingEngine messagingEngine;

    public DtxRegistry(DtxStore dtxStore, MessagingEngine messagingEngine) {
        this.dtxStore = dtxStore;
        branches = new HashMap<>();
        this.messagingEngine = messagingEngine;
    }

    public synchronized DtxBranch getBranch(Xid xid) {
        return branches.get(new ComparableXid(xid));
    }

    public synchronized boolean registerBranch(DtxBranch branch) {
        ComparableXid xid = new ComparableXid(branch.getXid());
        if(!branches.containsKey(xid)) {
            branches.put(xid, branch);
            return true;
        }
        return false;
    }

    public synchronized void prepare(Xid xid)
            throws UnknownDtxBranchException, IncorrectDtxStateException, TimeoutDtxException, AndesException {
        DtxBranch branch = getBranch(xid);

        if (branch != null) {
            if (!branch.hasAssociatedActiveSessions()) {
                branch.clearAssociations();

                if (branch.expired()) {
                    unregisterBranch(branch);
                    throw new TimeoutDtxException(xid);
                } else if (branch.getState() != DtxBranch.State.ACTIVE
                        && branch.getState() != DtxBranch.State.ROLLBACK_ONLY) {
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

    private boolean unregisterBranch(DtxBranch branch) {
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
    public long storeRecords(Xid xid, List<AndesMessage> enqueueRecords, List<AndesAckData> dequeueRecords)
            throws AndesException {
        return dtxStore.storeDtxRecords(xid, enqueueRecords, dequeueRecords);
    }

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

    public void removePreparedRecords(long internalXid) throws AndesException {
        dtxStore.removeDtxRecords(internalXid);
    }

    public void commit(Xid xid, Runnable callback, AndesChannel channel) throws UnknownDtxBranchException,
                                                                                IncorrectDtxStateException,
                                                                                AndesException {
        DtxBranch dtxBranch = getBranch(xid);
        if (null != dtxBranch) {
            synchronized (dtxBranch) {
                if (!dtxBranch.hasAssociatedActiveSessions()) {
                    // TODO: Need to revisit. What happens if the commit DB call fail?
                    dtxBranch.clearAssociations();
                    dtxBranch.commit(callback, channel);
                    dtxBranch.setState(DtxBranch.State.FORGOTTEN);
                    unregisterBranch(dtxBranch);
                } else {
                    throw new IncorrectDtxStateException("Branch still has associated sessions", xid);
                }
            }
        } else {
            throw new UnknownDtxBranchException(xid);
        }
    }

    public DtxStore getStore() {
        return dtxStore;
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
