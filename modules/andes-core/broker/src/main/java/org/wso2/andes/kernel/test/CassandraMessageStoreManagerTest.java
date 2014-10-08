/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *     WSO2 Inc. licenses this file to you under the Apache License,
 *     Version 2.0 (the "License"); you may not use this file except
 *     in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing,
 *    software distributed under the License is distributed on an
 *    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *    KIND, either express or implied.  See the License for the
 *    specific language governing permissions and limitations
 *    under the License.
 */

package org.wso2.andes.kernel.test;

import org.wso2.andes.configuration.ConfigurationProperties;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.storemanager.DurableAsyncStoringManager;
import org.wso2.andes.store.cassandra.CQLBasedMessageStoreImpl;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class CassandraMessageStoreManagerTest {

    private MessageStoreManager messageStoreManager;
    private int numberOfPublishers = 20;
    private AtomicLong messageCount = new AtomicLong();

    public CassandraMessageStoreManagerTest() {

    }

    public void initialize() {
        ConfigurationProperties connectionProperties = new ConfigurationProperties();
        connectionProperties.addProperty("dataSource", "CassandraRepo");
        MessageStore messageStore = new CQLBasedMessageStoreImpl();
        try {
            messageStore.initializeMessageStore(connectionProperties);
            messageStoreManager = new DurableAsyncStoringManager();
            messageStoreManager.initialise(messageStore);

        } catch (AndesException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public void publishMessages() {
       for(int i=0; i< numberOfPublishers; i++){
          MockPublisher mockPublisher = new MockPublisher(messageStoreManager, messageCount);
           mockPublisher.run();
       }
    }

    public void subscribeMessages() {

    }



}
