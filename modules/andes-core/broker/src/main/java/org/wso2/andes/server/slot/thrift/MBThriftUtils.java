/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

package org.wso2.andes.server.slot.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.slot.SlotCoordinationConstants;
import org.wso2.andes.server.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.server.slot.thrift.gen.SlotManagementService;

public class MBThriftUtils {

    private static final Log log = LogFactory.getLog(MBThriftUtils.class);

    private static MBThriftClient mbThriftClient = null;

    private static boolean reconnectingStarted = false;

    private static TTransport transport;

    /**
     * Start the reconnecting to thrift server thread thread
     */
    static {
        startServerReconnectingThread();
    }


    /**
     * Returns an instance of MB thrift service client
     *
     * @return a MB thrift service client
     */
    public static MBThriftClient getMBThriftClient() throws TTransportException {
        if (mbThriftClient == null) {
            HazelcastAgent hazelcastAgent = HazelcastAgent.getInstance();
            String thriftCoordinatorServerIP = hazelcastAgent.getThriftServerDetailsMap().get(
                    SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_IP);
            int thriftCoordinatorServerPort = Integer.parseInt(
                    hazelcastAgent.getThriftServerDetailsMap().
                            get(SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_PORT));
            transport = new TSocket(thriftCoordinatorServerIP,
                                    thriftCoordinatorServerPort);
            try {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                mbThriftClient = new MBThriftClient(new SlotManagementService.Client(protocol));
            } catch (TTransportException e) {
                log.error("Could not initialize the Thrift client. " + e.getMessage(), e);
                throw new TTransportException(
                        "Could not initialize the Thrift client. " + e.getMessage(), e);
            }

        }
        return mbThriftClient;
    }

    /**
     * set mbThriftClient to null
     */
    public static void resetMBThriftClient() {
        mbThriftClient = null;
        transport.close();
    }

    /**
     * Try to reconnect to server by taking latest values in the hazelcalst thrift server details
     * map
     *
     * @throws TTransportException
     *         when connecting to thrift server is unsuccessful
     */
    public static void reConnectToServer() throws TTransportException {
        HazelcastAgent hazelcastAgent = HazelcastAgent.getInstance();
        String thriftCoordinatorServerIP = hazelcastAgent.getThriftServerDetailsMap().get(
                SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_IP);
        int thriftCoordinatorServerPort = Integer.parseInt(
                hazelcastAgent.getThriftServerDetailsMap().
                        get(SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_PORT));
        transport = new TSocket(thriftCoordinatorServerIP, thriftCoordinatorServerPort);
        try {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            mbThriftClient = new MBThriftClient(new SlotManagementService.Client(protocol));
        } catch (TTransportException e) {
            log.error("Could not connect to the Thrift Server " + thriftCoordinatorServerIP + ":" +
                      thriftCoordinatorServerPort + e.getMessage(), e);
            throw new TTransportException(
                    "Could not connect to the Thrift Server " + thriftCoordinatorServerIP + ":" +
                    thriftCoordinatorServerPort, e);
        }
    }

    /**
     * This thread is responsible of reconnecting to the thrift server of the coordinator until it
     * gets succeeded
     */
    public static void startServerReconnectingThread() {
        new Thread() {
            public void run() {
                /**
                 * this thread will try to connect to thrift server while reconnectingStarted
                 * flag is true
                 * After successfully connecting to the server this flag will be set to true.
                 * While loop is therefore intentional.
                 */
                SlotDeliveryWorkerManager slotDeliveryWorkerManager = SlotDeliveryWorkerManager
                        .getInstance();
                while (true) {
                    if (reconnectingStarted) {

                        try {
                            MBThriftUtils.reConnectToServer();
                            /**
                             * If re connect to server is successful, following code segment will be
                             * executed
                             */
                            reconnectingStarted = false;
                            slotDeliveryWorkerManager.StartAllSlotDeliveryWorkers();
                        } catch (TTransportException e) {
                            try {
                                Thread.sleep(2000);
                            } catch (InterruptedException ignored) {
                                //silently ignore
                            }
                        }


                    }
                }
            }
        }.start();
    }

    /**
     * A flag to specify whether the reconnecting to thrift server is happening or not
     *
     * @return whether the reconnecting to thrift server is happening or not
     */
    public static boolean isReconnectingStarted() {
        return reconnectingStarted;
    }

    /**
     * Set reconnecting flag
     *
     * @param reconnectingFlag
     */
    public static void setReconnectingFlag(boolean reconnectingFlag) {
        reconnectingStarted = reconnectingFlag;
    }
}
