/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.server.registry;

import java.net.InetSocketAddress;
import java.util.UUID;

import org.wso2.andes.qmf.QMFService;
import org.wso2.andes.server.configuration.BrokerConfig;
import org.wso2.andes.server.configuration.ConfigStore;
import org.wso2.andes.server.configuration.ServerConfiguration;
import org.wso2.andes.server.configuration.VirtualHostConfiguration;
import org.wso2.andes.server.configuration.ConfigurationManager;
import org.wso2.andes.server.logging.RootMessageLogger;
import org.wso2.andes.server.management.ManagedObjectRegistry;
import org.wso2.andes.server.plugins.PluginManager;
import org.wso2.andes.server.security.SecurityManager;
import org.wso2.andes.server.security.auth.manager.AuthenticationManager;
import org.wso2.andes.server.stats.StatisticsGatherer;
import org.wso2.andes.server.transport.QpidAcceptor;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostRegistry;

public interface IApplicationRegistry extends StatisticsGatherer
{
    /**
     * Initialise the application registry. All initialisation must be done in this method so that any components
     * that need access to the application registry itself for initialisation are able to use it. Attempting to
     * initialise in the constructor will lead to failures since the registry reference will not have been set.
     */
    void initialise() throws Exception;

    /**
     * Shutdown this Registry
     */
    void close();

    /**
     * Get the low level configuration. For use cases where the configured object approach is not required
     * you can get the complete configuration information.
     * @return a Commons Configuration instance
     */
    ServerConfiguration getConfiguration();

    ManagedObjectRegistry getManagedObjectRegistry();

    AuthenticationManager getAuthenticationManager();

    VirtualHostRegistry getVirtualHostRegistry();

    SecurityManager getSecurityManager();

    PluginManager getPluginManager();

    ConfigurationManager getConfigurationManager();

    RootMessageLogger getRootMessageLogger();

    /**
     * Register any acceptors for this registry
     * @param bindAddress The address that the acceptor has been bound with
     * @param acceptor The acceptor in use
     */
    void addAcceptor(InetSocketAddress bindAddress, QpidAcceptor acceptor);

    public UUID getBrokerId();

    QMFService getQMFService();

    void setBroker(BrokerConfig broker);

    BrokerConfig getBroker();

    VirtualHost createVirtualHost(VirtualHostConfiguration vhostConfig) throws Exception;

    ConfigStore getConfigStore();

    void setConfigStore(ConfigStore store);
    
    void initialiseStatisticsReporting();
}
