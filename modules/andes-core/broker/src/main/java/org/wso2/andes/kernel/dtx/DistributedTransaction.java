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

import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesException;

import java.util.ArrayList;
import java.util.List;
import javax.transaction.xa.Xid;

public class DistributedTransaction {


    private DtxRegistry dtxRegistry;
    private DtxBranch branch;

    private List<AndesAckData> dequeueList;

    public DistributedTransaction(DtxRegistry dtxRegistry) {
        this.dtxRegistry = dtxRegistry;
        this.dequeueList = new ArrayList<>();
    }

    public void start(long sessionID, Xid xid, boolean join, boolean resume)
            throws JoinAndResumeDtxException, UnknownDtxBranchException, AlreadyKnownDtxException {

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

            branch = new DtxBranch(xid, dtxRegistry);

            if (dtxRegistry.registerBranch(branch)) {
                this.branch = branch;
                 branch.associateSession(sessionID);
            } else {
                throw new AlreadyKnownDtxException(xid);
            }
        }

    }

    public void end(long sessionId, Xid xid, boolean fail, boolean suspend)
            throws SuspendAndFailDtxException, UnknownDtxBranchException, NotAssociatedDtxException {
        DtxBranch branch = dtxRegistry.getBranch(xid);
        if(suspend && fail)
        {
            branch.disassociateSession(sessionId);
            this.branch = null;
            throw new SuspendAndFailDtxException(xid);
        }

        if (null == branch) {
            throw new UnknownDtxBranchException(xid);
        } else if (branch.isAssociated(sessionId)) {
            throw new NotAssociatedDtxException(xid);

            // TODO Check for transaction expiration
        } else if (suspend) {
            branch.suspendSession(sessionId);
        } else {
            if (fail) {
                branch.markAsFailedSession(sessionId);
            }
            branch.disassociateSession(sessionId);
        }

        this.branch = null;
    }

    public void dequeue(List<AndesAckData> ackList) throws AndesException {
        if (null != branch) {
            dequeueList.addAll(ackList);
        } else {
            for (AndesAckData ackData: ackList) {
                Andes.getInstance().ackReceived(ackData);
            }
        }
    }
}
