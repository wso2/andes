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

import com.datastax.driver.core.Cluster;
import org.wso2.andes.configuration.ConfigurationProperties;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.storemanager.AsyncStoringManager;
import org.wso2.andes.server.store.util.CQLDataAccessHelper;
import org.wso2.andes.server.store.util.CassandraDataAccessException;
import org.wso2.andes.store.cassandra.cql.CQLBasedMessageStoreImpl;
import org.wso2.andes.store.cassandra.cql.CQLConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class CassandraMessageStoreManagerTest {

    private MessageStoreManager messageStoreManager;
    private int numberOfPublishers = 20;
    private AtomicLong messageCount = new AtomicLong();
    private Cluster cluster;
    CQLConnection cqlConnection;

    public CassandraMessageStoreManagerTest() {
        initialize();
    }


    private void initializeCluster() {
        try {
            List<String> hosts = new ArrayList<String>();
            hosts.add("127.0.0.1");
            CQLDataAccessHelper.ClusterConfiguration clusterConfig = new CQLDataAccessHelper
                    .ClusterConfiguration("admin", "admin", "TestCluster", hosts, 9042);

            cluster = CQLDataAccessHelper.createCluster(clusterConfig);
            CQLConnection cqlConnection = new CQLConnection();
            cqlConnection.setCluster(cluster);
        } catch (CassandraDataAccessException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    public void initialize() {
        initializeCluster();
        ConfigurationProperties connectionProperties = new ConfigurationProperties();
        connectionProperties.addProperty("dataSource", "CassandraRepo");
        CQLBasedMessageStoreImpl messageStore = new CQLBasedMessageStoreImpl();
        try {
            messageStore.setCQLConnection(cqlConnection);
            messageStore.initializeMessageStore(connectionProperties);
            messageStoreManager = new AsyncStoringManager(messageStore);

        } catch (AndesException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public void publishMessages() {
        for (int i = 0; i < numberOfPublishers; i++) {
            MockPublisher mockPublisher = new MockPublisher(messageStoreManager, messageCount);
            mockPublisher.run();
        }
    }

    public void subscribeMessages() {

    }

    public static void main(String[] args) {
        CassandraMessageStoreManagerTest storeManagerTest = new CassandraMessageStoreManagerTest();
        storeManagerTest.publishMessages();
    }


}