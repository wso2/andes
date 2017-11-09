/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.andes.configuration.models;

import org.wso2.andes.configuration.models.coordination.CoordinationConfiguration;
import org.wso2.andes.configuration.models.deployment.DeploymentConfiguration;
import org.wso2.andes.configuration.models.flowcontrol.FlowControlConfiguration;
import org.wso2.andes.configuration.models.management.ManagementConsoleConfiguration;
import org.wso2.andes.configuration.models.performance.PerformanceTuningConfiguration;
import org.wso2.andes.configuration.models.persistence.PersistenceConfiguration;
import org.wso2.andes.configuration.models.recovery.RecoveryConfiguration;
import org.wso2.andes.configuration.models.transaction.TransactionConfiguration;
import org.wso2.andes.configuration.models.transport.TransportConfiguration;
import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Root configuration model of the Broker.
 * This class provide access to all the configuration object representations of the broker.
 */
@Configuration(namespace = "wso2.carbon.broker",
        description = "Broker configuration parameters")
public class BrokerConfiguration {

    @Element(description = "Depending on the database type selected in deployment.yaml, you must enable the\n"
                           + "relevant Data access classes here. Currently WSO2 MB Supports RDBMS(any RDBMS store).")
    private PersistenceConfig persistance = new PersistenceConfig();

    @Element(description = "The relevant implementation class name of Authentication Service.By default, authentication"
                           + " implementation of broker will be used")
    private String authenticator = "org.wso2.carbon.business.messaging.core.internal.AuthenticationServiceImpl";

    @Element(description = "Coordination related configurations")
    private CoordinationConfiguration coordination = new CoordinationConfiguration();

    @Element(description = "Transport related configurations")
    private TransportConfiguration transports = new TransportConfiguration();

    @Element(description = "Persistence related configurations")
    private PersistenceConfiguration persistence = new PersistenceConfiguration();

    @Element(description = "Publisher transaction related configurations")
    private TransactionConfiguration transaction = new TransactionConfiguration();

    @Element(description = "This section allows you to tweak memory and processor allocations used by WSO2 MB.\n"
                           + "Broken down by critical processes so you have a clear view of which parameters to "
                           + "change in\n"
                           + "different scenarios.")
    private PerformanceTuningConfiguration performanceTuning = new PerformanceTuningConfiguration();

    @Element(description = "Message statistics view related configurations")
    private ManagementConsoleConfiguration managementConsole = new ManagementConsoleConfiguration();

    @Element(description = "Memory and resource exhaustion is something we should prevent and recover from.\n"
                           + "    This section allows you to specify the threshold at which to reduce/stop frequently"
                           + " intensive\n"
                           + "    operations within MB temporarily.")
    private FlowControlConfiguration flowControl = new FlowControlConfiguration();

    @Element(description =
            "Message broker keeps track of all messages it has received as groups. These groups are termed\n"
            + "    'Slots' (To know more information about Slots and message broker install please refer to online "
            + "wiki).\n"
            + "    Size of a slot is loosely determined by the configuration <windowSize> (and the number of\n"
            + "    parallel publishers for specific topic/queue). Message broker cluster (or in single node) keeps\n"
            + "    track of slots which constitutes for a large part of operating state before the cluster went down.\n"
            + "        When first message broker node of the cluster starts up, it will read the database to recreate\n"
            + "    the internal state to previous state.")
    private RecoveryConfiguration recovery = new RecoveryConfiguration();

    @Element(description =
            "Specifies the deployment mode for the broker node (and cluster). Possible values {standalone, clustered}"
            + ".\n"
            + "\n"
            + "        standalone - This is the simplest mode a broker can be started. The node will assume that "
            + "message store is not\n"
            + "                     shared with another node. Therefore it will not try to coordinate with other "
            + "nodes (possibly\n"
            + "                     non-existent) to provide HA.\n" + "\n"
            + "        clustered - Broker node will run in HA mode (active/passive).")
    private DeploymentConfiguration deployment = new DeploymentConfiguration();

    public String getAuthenticator() {
        return authenticator;
    }

    public CoordinationConfiguration getCoordination() {
        return coordination;
    }

    public TransportConfiguration getTransport() {
        return transports;
    }

    public PersistenceConfiguration getPersistence() {
        return persistence;
    }

    public TransactionConfiguration getTransaction() {
        return transaction;
    }

    public PerformanceTuningConfiguration getPerformanceTuning() {
        return performanceTuning;
    }

    public ManagementConsoleConfiguration getManagementConsole() {
        return managementConsole;
    }

    public FlowControlConfiguration getFlowControl() {
        return flowControl;
    }

    public RecoveryConfiguration getRecovery() {
        return recovery;
    }

    public DeploymentConfiguration getDeployment() {
        return deployment;
    }

    public PersistenceConfig getPersistance() {
        return persistance;
    }

}
