/*
 *
 *   Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 */

package org.wso2.andes.server.slot.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.slot.thrift.gen.SlotManagementService;

import java.net.InetSocketAddress;

/**
 * This class is responsible of handling the thrift server
 */
public class MBThriftServer {

    private static Log log = LogFactory.getLog(MBThriftServer.class);

    private static MBThriftServer mbThriftServer = new MBThriftServer();

    private SlotManagementServiceImpl slotManagementServerHandler;

    private MBThriftServer() {
        this.slotManagementServerHandler = new SlotManagementServiceImpl();

    }

    /**
     * The server instance
     */
    private TServer server;

    /**
     * Start the thrift server
     *
     * @param hostName the hostname
     * @param port     thrift server port
     * @param taskName the name of the main server thread
     * @throws org.wso2.andes.kernel.AndesException
     *          throws in case of an starting error
     */
    public void start(final String hostName,
                      final int port,
                      final String taskName) throws AndesException {
        try {

            TServerSocket socket = new TServerSocket(new InetSocketAddress(hostName, port));
            SlotManagementService.Processor<SlotManagementServiceImpl> processor =
                    new SlotManagementService.Processor<SlotManagementServiceImpl>(slotManagementServerHandler);
            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
            server = new TThreadPoolServer(new TThreadPoolServer.Args(socket).
                    processor(processor).inputProtocolFactory(protocolFactory));

            log.info("Starting the Message Broker Thrift server on host '" + hostName + "' on port '" + port
                    + "'...");
            new Thread(new MBServerMainLoop(server), taskName).start();

        } catch (TTransportException e) {
            throw new AndesException("Cannot start Thrift server on port " + port + " on host " + hostName, e);
        }
    }

    /**
     * Stop the server
     */
    public void stop() {
        if (server != null) {
            server.stop();
        }
    }

    /**
     * Returns if the server is still running
     *
     * @return true if server is running
     */
    public boolean isServerAlive() {
        return server != null && server.isServing();
    }

    /**
     * @return MBThriftServer instance
     */
    public static MBThriftServer getInstance() {
        return mbThriftServer;
    }

    /**
     * The task for starting the thrift server
     */
    private static class MBServerMainLoop implements Runnable {
        private TServer server;

        private MBServerMainLoop(TServer server) {
            this.server = server;
        }

        public void run() {
            try {
                server.serve();
            } catch (Exception e) {
                throw new RuntimeException("Could not start the MBThrift server", e);
            }
        }
    }
}
