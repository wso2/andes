/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel.distrupter;

import java.util.ArrayList;
import java.util.List;

import org.wso2.andes.kernel.AndesAckData;

import com.lmax.disruptor.EventHandler;
import org.wso2.andes.kernel.MessageStoreManager;

/**
 * We do this to make Listener take turns while running. So we can run many copies of these and control number
 * of IO threads through that.
 */

public class AckHandler implements EventHandler<AndesAckData> {
    private MessageStoreManager messageStoreManager;
    private List<AndesAckData> ackList = new ArrayList<AndesAckData>();

    public AckHandler(MessageStoreManager messageStoreManager) {
        this.messageStoreManager = messageStoreManager;
    }

    public void onEvent(final AndesAckData event, final long sequence, final boolean endOfBatch) throws Exception {
        ackList.add(event);
        if (endOfBatch || ackList.size() > 100) {
            final List<AndesAckData> tempList = ackList;
            messageStoreManager.ackReceived(tempList);
            ackList = new ArrayList<AndesAckData>();
        }
    }
}
