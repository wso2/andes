/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.store;

import org.wso2.andes.dtx.XidImpl;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.DtxStore;
import org.wso2.andes.kernel.dtx.AndesPreparedMessageMetadata;
import org.wso2.andes.kernel.dtx.DtxBranch;

import java.util.List;
import java.util.Set;
import javax.transaction.xa.Xid;

/**
 * Implementation of {@link org.wso2.andes.kernel.DtxStore} which observes failures such as
 * connection errors. Any {@link org.wso2.andes.kernel.DtxStore} implementation
 */
public class FailureObservingDtxStore extends FailureObservingStore<DtxStore> implements DtxStore {

    /**
     * Create a {@link FailureObservingDtxStore} wrapper class using the underlying {@link DtxStore} implementation
     * and a {@link FailureObservingStoreManager}
     *
     * @param wrappedInstance {@link DtxStore} implementation
     * @param storeManager Reference to the {@link FailureObservingStoreManager}
     */
    public FailureObservingDtxStore(DtxStore wrappedInstance, FailureObservingStoreManager storeManager) {
        super(wrappedInstance, storeManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long storeDtxRecords(Xid xid, List<AndesMessage> enqueueRecords,
                                List<? extends AndesMessageMetadata> dequeueRecords) throws AndesException {
        try {
            return wrappedInstance.storeDtxRecords(xid, enqueueRecords, dequeueRecords);
        } catch (AndesStoreUnavailableException e) {
            notifyFailures(e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateOnCommit(long internalXid, List<AndesMessage> enqueueRecords) throws AndesException {
        try {
            wrappedInstance.updateOnCommit(internalXid, enqueueRecords);
        } catch (AndesStoreUnavailableException e) {
            notifyFailures(e);
            throw new AndesException(e);
        }
    }

    @Override
    public void updateOnOnePhaseCommit(List<AndesMessage> enqueueRecords,
            List<AndesPreparedMessageMetadata> dequeueRecordsMetadata) throws AndesException {
        try {
            wrappedInstance.updateOnOnePhaseCommit(enqueueRecords, dequeueRecordsMetadata);
        } catch (AndesStoreUnavailableException e) {
            notifyFailures(e);
            throw new AndesException(e);
        }
    }

    @Override
    public void updateOnRollback(long internalXid, List<AndesPreparedMessageMetadata> messagesToRestore) throws AndesException {
        try {
            wrappedInstance.updateOnRollback(internalXid, messagesToRestore);
        } catch (AndesStoreUnavailableException e) {
            notifyFailures(e);
            throw new AndesException(e);
        }
    }

    @Override
    public long recoverBranchData(DtxBranch branch, String nodeId) throws AndesException {
        try {
            return wrappedInstance.recoverBranchData(branch, nodeId);
        } catch (AndesStoreUnavailableException e) {
            notifyFailures(e);
            throw new AndesException(e);
        }
    }

    @Override
    public Set<XidImpl> getStoredXidSet(String nodeId) throws AndesException {
        try {
            return wrappedInstance.getStoredXidSet(nodeId);
        } catch (AndesStoreUnavailableException e) {
            notifyFailures(e);
            throw new AndesException(e);
        }
    }

}
