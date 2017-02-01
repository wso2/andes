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

package org.wso2.andes.kernel;

import org.wso2.andes.store.HealthAwareStore;

import java.util.List;
import javax.transaction.xa.Xid;

/**
 * Interface for distributed transaction related database operations
 */
public interface DtxStore extends HealthAwareStore {

    /**
     * Persist enqueue and dequeue records to the database. This is normally done at the prepare stage
     *
     * @param xid            xid of the Distributed transaction branch
     * @param enqueueRecords list of enqueue records
     * @param dequeueRecords list of dequeue records
     */
    long storeDtxRecords(Xid xid, List<AndesMessage> enqueueRecords, List<AndesAckData> dequeueRecords)
            throws AndesException;

    /**
     * Update the store on a dtx.commit request with enqueued and dequeued records relevant to a specific {@link Xid}
     * This is update is done in a database transaction operation
     *
     * @param internalXid internalXid of the dtx transaction
     * @param enqueueRecords {@link AndesMessage} list to be stored
     * @param dequeueRecords {@link DeliverableAndesMetadata} list that got acknowledged within the transaction
     * @throws AndesException Throws exception on database related errors
     */
    void updateOnCommit(long internalXid, List<AndesMessage> enqueueRecords,
                        List<DeliverableAndesMetadata> dequeueRecords) throws AndesException;

    /**
     * Remove database records related to a transaction.
     *
     * @param internalXid internal {@link Xid} related to the distributed transaction
     * @throws AndesException Throws exception on database related errors
     */
    void removeDtxRecords(long internalXid) throws AndesException;

}
