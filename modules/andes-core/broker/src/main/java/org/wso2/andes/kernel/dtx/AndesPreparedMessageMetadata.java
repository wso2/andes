/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel.dtx;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;

/**
 * Andes message metadata object that is used to move acknowledged but not committed messages from distributed
 * transaction prepared datastore space to message store
 */
public class AndesPreparedMessageMetadata extends AndesMessageMetadata {

    /**
     * Previous message id of the acknowledged message
     */
    private long oldMessageId;

    public AndesPreparedMessageMetadata(AndesMessageMetadata messageMetadata) {
        super(messageMetadata.getBytes());
        oldMessageId = getMessageID();
        setStorageDestination(messageMetadata.getStorageDestination());
        setExpirationTime(messageMetadata.getExpirationTime());
    }

    /**
     * Previous message id. Acknowledgment is received for this message id
     * @return message id
     */
    public long getOldMessageId() {
        return this.oldMessageId;
    }

}
