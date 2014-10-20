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
package org.wso2.andes.server.virtualhost;

import org.wso2.andes.common.Closeable;
import org.wso2.andes.server.binding.BindingFactory;
import org.wso2.andes.server.configuration.ConfigStore;
import org.wso2.andes.server.configuration.VirtualHostConfig;
import org.wso2.andes.server.configuration.VirtualHostConfiguration;
import org.wso2.andes.server.connection.IConnectionRegistry;
import org.wso2.andes.server.exchange.ExchangeFactory;
import org.wso2.andes.server.exchange.ExchangeRegistry;
import org.wso2.andes.server.federation.BrokerLink;
import org.wso2.andes.server.management.ManagedObject;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.server.registry.IApplicationRegistry;
import org.wso2.andes.server.security.SecurityManager;
import org.wso2.andes.server.security.auth.manager.AuthenticationManager;
import org.wso2.andes.server.stats.StatisticsGatherer;
import org.wso2.andes.server.store.DurableConfigurationStore;
import org.wso2.andes.server.store.MessageStore;
import org.wso2.andes.server.store.TransactionLog;

import java.util.UUID;

public interface VirtualHost extends DurableConfigurationStore.Source, VirtualHostConfig, Closeable, StatisticsGatherer
{
    IConnectionRegistry getConnectionRegistry();

    VirtualHostConfiguration getConfiguration();

    String getName();

    QueueRegistry getQueueRegistry();

    ExchangeRegistry getExchangeRegistry();

    ExchangeFactory getExchangeFactory();

    MessageStore getMessageStore();

    TransactionLog getTransactionLog();

    DurableConfigurationStore getDurableConfigurationStore();

    AuthenticationManager getAuthenticationManager();

    SecurityManager getSecurityManager();

    void close();

    ManagedObject getManagedObject();

    UUID getBrokerId();

    void scheduleHouseKeepingTask(long period, HouseKeepingTask task);

    long getHouseKeepingTaskCount();

    public long getHouseKeepingCompletedTaskCount();

    int getHouseKeepingPoolSize();

    void setHouseKeepingPoolSize(int newSize);    

    int getHouseKeepingActiveCount();

    IApplicationRegistry getApplicationRegistry();

    BindingFactory getBindingFactory();

    void createBrokerConnection(String transport,
                                String host,
                                int port,
                                String vhost,
                                boolean durable,
                                String authMechanism, String username, String password);

    ConfigStore getConfigStore();

    void removeBrokerConnection(BrokerLink brokerLink);
}
