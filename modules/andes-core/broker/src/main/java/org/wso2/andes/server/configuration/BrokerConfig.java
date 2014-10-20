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

package org.wso2.andes.server.configuration;


public interface BrokerConfig  extends ConfiguredObject<BrokerConfigType,BrokerConfig>
{
    void setSystem(SystemConfig system);

    SystemConfig getSystem();

    Integer getPort();

    Integer getWorkerThreads();

    Integer getMaxConnections();

    Integer getConnectionBacklogLimit();

    Long getStagingThreshold();

    Integer getManagementPublishInterval();

    String getVersion();

    String getDataDirectory();

    void addVirtualHost(VirtualHostConfig virtualHost);

    void createBrokerConnection(String transport,
                                String host,
                                int port,
                                boolean durable,
                                String authMechanism,
                                String username, String password);

    String getFederationTag();
}
