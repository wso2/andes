/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

package org.wso2.andes.andesContextStore;

import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesExchange;
import org.wso2.andes.kernel.DurableStoreConnection;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

public class H2BasedAndesContextStore implements AndesContextStore {

    private DataSource datasource;
    private String icEntry;

    @Override
    public void init(DurableStoreConnection connection) throws AndesException {

    }

    @Override
    public Map<String, List<String>> getAllStoredDurableSubscriptions() throws AndesException {
        return null;
    }

    @Override
    public void storeDurableSubscription(String destinationIdentifier, String subscriptionID, String subscriptionEncodeAsStr) throws AndesException {

    }

    @Override
    public void removeDurableSubscription(String destinationIdentifier, String subscriptionID) throws AndesException {

    }

    @Override
    public void storeNodeDetails(String nodeID, String data) throws AndesException {

    }

    @Override
    public Map<String, String> getAllStoredNodeData() throws AndesException {
        return null;
    }

    @Override
    public void removeNodeData(String nodeID) throws AndesException {

    }

    @Override
    public void addMessageCounterForQueue(String destinationQueueName) throws AndesException {

    }

    @Override
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {
        return 0;
    }

    @Override
    public void removeMessageCounterForQueue(String destinationQueueName) throws AndesException {

    }

    @Override
    public void storeExchangeInformation(String exchangeName, String exchangeInfo) throws AndesException {

    }

    @Override
    public List<AndesExchange> getAllExchangesStored() throws AndesException {
        return null;
    }

    @Override
    public void deleteExchangeInformation(String exchangeName) throws AndesException {

    }

    @Override
    public void close() {

    }
}
