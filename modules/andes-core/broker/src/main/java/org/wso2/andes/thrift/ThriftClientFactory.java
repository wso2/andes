/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.andes.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.slot.SlotCoordinationConstants;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.thrift.exception.ThriftClientException;
import org.wso2.andes.thrift.slot.gen.SlotManagementService;

/**
 * Thrift client ConnectionFactory which initialises a connection pool for thrift communication.
 */
public class ThriftClientFactory extends BasePooledObjectFactory<SlotManagementService.Client> {

    private static final Log log = LogFactory.getLog(ThriftClientFactory.class);

    /**
     * When a new connection needs to be created this method is called.
     *
     * @return A new thrift client which is connected to the server
     * @throws Exception Throws when a thrift client fails to connect to the server
     */
    @Override
    public SlotManagementService.Client create() throws Exception {
        HazelcastAgent hazelcastAgent = HazelcastAgent.getInstance();
        String thriftCoordinatorServerIP = hazelcastAgent.getThriftServerDetailsMap().get(
                SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_IP);
        String thriftCoordinatorServerPortString = hazelcastAgent.getThriftServerDetailsMap().
                get(SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_PORT);

        Integer soTimeout = AndesConfigurationManager.readValue(AndesConfiguration.COORDINATION_THRIFT_SO_TIMEOUT);

        if ((thriftCoordinatorServerIP == null) || (thriftCoordinatorServerPortString == null)) {
            throw new ThriftClientException("Thrift coordinator details are not updated in the map yet");
        } else {

            int thriftCoordinatorServerPort = Integer.parseInt(thriftCoordinatorServerPortString);
            TTransport transport = new TSocket(thriftCoordinatorServerIP,
                    thriftCoordinatorServerPort, soTimeout);
            try {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                return new SlotManagementService.Client(protocol);
            } catch (TTransportException e) {
                throw new TTransportException("Could not initialize the Thrift client.", e);
            }
        }
    }

    /**
     * When a new object needs to be added to the pool, this method is called.
     *
     * @param client The thrift client
     * @return The thrift client wrapped by the PooledObject
     */
    @Override
    public PooledObject<SlotManagementService.Client> wrap(SlotManagementService.Client client) {
        return new DefaultPooledObject<>(client);
    }

    /**
     * When an object is invalidated this method is called. We have to properly close the sockets before removing the
     * connection.
     *
     * @param client The thrift client which should be destroyed
     * @throws Exception Throws when the thrift connection could not be closed
     */
    @Override
    public void destroyObject(PooledObject<SlotManagementService.Client> client) throws Exception {
        client.getObject().getInputProtocol().getTransport().close();
        super.destroyObject(client);
    }
}
