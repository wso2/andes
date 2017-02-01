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

import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.DtxStore;

import java.util.List;
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
    public long storeDtxRecords(Xid xid, List<AndesMessage> enqueueRecords, List<AndesAckData> dequeueRecords)
            throws AndesException {
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
    public void updateOnCommit(long internalXid, List<AndesMessage> enqueueRecords,
                               List<DeliverableAndesMetadata> dequeueRecords) throws AndesException {
        try {
            wrappedInstance.updateOnCommit(internalXid, enqueueRecords, dequeueRecords);
        } catch (AndesStoreUnavailableException e) {
            notifyFailures(e);
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDtxRecords(long internalXid) throws AndesException {
        try {
            wrappedInstance.removeDtxRecords(internalXid);
        } catch (AndesStoreUnavailableException e) {
            notifyFailures(e);
            throw new AndesException(e);
        }
    }

}
