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

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

import javax.annotation.Resource;

/**
 * Root configuration model of the Broker.
 * This class provide access to all the configuration object representations of the broker.
 */
@Configuration(namespace = "wso2.carbon.broker",
               description = "Broker configuration parameters")
public class BrokerConfiguration {

    @Element(description = "Coordination related configuration.")
    private CoordinationConfig coordination = new CoordinationConfig();

    @Element(description = "Transport related configuration")
    private TransportsConfig transports = new TransportsConfig();

    @Element(description = "Depending on the database type selected in deployment.yaml, you must enable the\n"
            + "relevant Data access classes here. Currently WSO2 MB Supports RDBMS(any RDBMS store).")
    private PersistenceConfig persistance = new PersistenceConfig();

    @Element(description = "Publisher transaction related configurations.")
    private TransactionConfig transaction = new TransactionConfig();

    @Element(description = "This section allows you to tweak memory and processor allocations used by WSO2 MB.\n"
            + "Broken down by critical processes so you have a clear view of which parameters to change in\n"
            + "different scenarios.")
    private PerformanceTuningConfig performanceTuning = new PerformanceTuningConfig();

    @Element(description = "Memory and resource exhaustion is something we should prevent and recover from.\n"
            + "    This section allows you to specify the threshold at which to reduce/stop frequently intensive\n"
            + "    operations within MB temporarily.")
    private FlowControlConfig flowControl = new FlowControlConfig();

    @Element(description = "Message broker keeps track of all messages it has received as groups. These groups are termed\n"
            + "    'Slots' (To know more information about Slots and message broker install please refer to online wiki).\n"
            + "    Size of a slot is loosely determined by the configuration <windowSize> (and the number of\n"
            + "    parallel publishers for specific topic/queue). Message broker cluster (or in single node) keeps\n"
            + "    track of slots which constitutes for a large part of operating state before the cluster went down.\n"
            + "        When first message broker node of the cluster starts up, it will read the database to recreate\n"
            + "    the internal state to previous state.")
    private RecoveryConfig recovery = new RecoveryConfig();

    @Element(description = "Specifies the deployment mode for the broker node (and cluster). Possible values {standalone, clustered}.\n"
            + "\n"
            + "        standalone - This is the simplest mode a broker can be started. The node will assume that message store is not\n"
            + "                     shared with another node. Therefore it will not try to coordinate with other nodes (possibly\n"
            + "                     non-existent) to provide HA.\n" + "\n"
            + "        clustered - Broker node will run in HA mode (active/passive).")
    private DeploymentConfig deployment = new DeploymentConfig();

    public CoordinationConfig getCoordination() {
        return coordination;
    }

    public TransportsConfig getTransports() {
        return transports;
    }

    public PersistenceConfig getPersistance() {
        return persistance;
    }

    public TransactionConfig getTransaction() {
        return transaction;
    }

    public PerformanceTuningConfig getPerformanceTuning() {
        return performanceTuning;
    }

    public FlowControlConfig getFlowControl() {
        return flowControl;
    }

    public RecoveryConfig getRecovery() {
        return recovery;
    }

    public DeploymentConfig getDeployment() {
        return deployment;
    }
}
