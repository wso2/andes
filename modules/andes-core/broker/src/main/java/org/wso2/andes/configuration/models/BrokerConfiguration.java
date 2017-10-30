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

    @Element(description = "Performance tuning related configurations")
    private PerformanceTuningConfiguration performanceTuning = new PerformanceTuningConfiguration();

    @Element(description = "Message statistics view related configurations")
    private ManagementConsoleConfiguration managementConsole = new ManagementConsoleConfiguration();

    @Element(description = "Flow control related configurations")
    private FlowControlConfiguration flowControl = new FlowControlConfiguration();

    @Element(description = "Message recovery related configurations")
    private RecoveryConfiguration recovery = new RecoveryConfiguration();

    @Element(description = "Deployment related configurations")
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

}
