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
package org.wso2.andes.server.cluster;

import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.management.common.mbeans.ClusterManagementInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanConstructor;
import org.wso2.andes.server.management.AMQManagedObject;

import javax.management.JMException;
import java.util.List;

/**
 * <code>ClusterManagementInformationMBean</code> The the JMS MBean that expose cluster management information
 * Exposes the Cluster Management related information using MBeans
 */
public class ClusterManagementInformationMBean extends AMQManagedObject implements ClusterManagementInformation {

    /**
     * ClusterManager instance to get the information to expose
     */
    private ClusterManager clusterManager;

    /**
     * Public MBean Constructor.
     *
     * @param clusterManager holds the information which should be exposed
     * @throws JMException
     */
    @MBeanConstructor("Creates an MBean exposing an Cluster Manager")
    public ClusterManagementInformationMBean(ClusterManager clusterManager) throws JMException {
        super(ClusterManagementInformation.class, ClusterManagementInformation.TYPE);
        this.clusterManager = clusterManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getObjectInstanceName() {
        return ClusterManagementInformation.TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClusteringEnabled() {
        return AndesContext.getInstance().isClusteringEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMyNodeID() {
        return clusterManager.getMyNodeID();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCoordinatorNodeAddress() {
        return this.clusterManager.getCoordinatorNodeAddress();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllClusterNodeAddresses() {
        return this.clusterManager.getAllClusterNodeAddresses();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getStoreHealth() {
        return this.clusterManager.getStoreHealth();
    }
}
