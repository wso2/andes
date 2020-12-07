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

package org.wso2.andes.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.thrift.slot.gen.SlotManagementService;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * This class takes care of starting and stopping the thrift server which is used to do slot communication.
 */
public class MBThriftServer {

    /**
     * Default thrift server request timeout. Server stop() will also wait this amount of time at max before
     * returning. The time unit is seconds.
     */
    private static final int THRIFT_SERVER_REQUEST_TIMEOUT = 20;

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
        /**
         * Stop node if 0.0.0.0 used as thrift server host in broker.xml
         */
        if ("0.0.0.0".equals(hostName)) {
            throw new AndesException("Invalid thrift server host 0.0.0.0");
        }
        try {
            int socketTimeout = AndesConfigurationManager.readValue(AndesConfiguration.COORDINATION_THRIFT_SO_TIMEOUT);
            TServerSocket socket = new TServerSocket(new InetSocketAddress(hostName, port), socketTimeout);
            SlotManagementService.Processor<SlotManagementServiceImpl> processor =
                    new SlotManagementService.Processor<SlotManagementServiceImpl>(slotManagementServerHandler);
            int maxWorkerThreads =
                    AndesConfigurationManager.readValue(AndesConfiguration.COORDINATION_THRIFT_SERVER_MAX_THREADS);
            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
            server = new TThreadPoolServer(new TThreadPoolServer.Args(socket)
                    .processor(processor)
                    .inputProtocolFactory(protocolFactory)
                    .requestTimeoutUnit(TimeUnit.SECONDS)
                    .requestTimeout(THRIFT_SERVER_REQUEST_TIMEOUT)
                    .maxWorkerThreads(maxWorkerThreads));

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

        log.info("Stopping the Message Broker Thrift server...");

        if (server != null) {
            server.stop();

            try {
                boolean terminationSuccessful = awaitTermination(THRIFT_SERVER_REQUEST_TIMEOUT, TimeUnit.SECONDS);

                if (!terminationSuccessful) {
                    log.warn("Could not stop the Message Broker Thrift server");
                }
            } catch (InterruptedException e) {
                log.warn("Thrift server shutdown was interrupted", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Blocks until server stop serving after a stop request, or the timeout occurs, or the current thread is
     * interrupted, whichever happens first.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return {@code true} if thrift server stopped and
     *         {@code false} if the timeout elapsed before server stops
     * @throws InterruptedException if interrupted while waiting
     */
    private boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        final long timeoutInMillis = unit.toMillis(timeout);
        final long shutDownStartTime = System.currentTimeMillis();
        long timeElapsed = 0;

        while (server.isServing() && timeElapsed < timeoutInMillis) {
            TimeUnit.MILLISECONDS.sleep(100);
            timeElapsed = System.currentTimeMillis() - shutDownStartTime;
        }

        return server.isServing();
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
