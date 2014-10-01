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
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.slot.ConnectionException;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.slot.SlotCoordinationConstants;
import org.wso2.andes.server.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.server.slot.thrift.gen.SlotInfo;
import org.wso2.andes.server.slot.thrift.gen.SlotManagementService;

/**
 * A wrapper client for the native thrift client
 */

public class MBThriftClient {


    private static boolean reconnectingStarted = false;

    private static TTransport transport;

    private static SlotManagementService.Client client = null;

    private static final Log log = LogFactory.getLog(MBThriftClient.class);

    /**
     * getSlot method. Returns Slot Object, when the
     * queue name is given
     *
     * @param queueName name of the queue
     * @param nodeId    of this node
     * @return slot object
     * @throws ConnectionException
     */
    public static synchronized Slot getSlot(String queueName,
                                            String nodeId) throws ConnectionException {
        SlotInfo slotInfo;
        try {
            client = getServiceClient();
            slotInfo = client.getSlotInfo(queueName, nodeId);
            return convertSlotInforToSlot(slotInfo);
        } catch (TException e) {
            try {
                //retry once
                reConnectToServer();
                slotInfo = client.getSlotInfo(queueName, nodeId);
                return convertSlotInforToSlot(slotInfo);
            } catch (TException e1) {
                handleCoordinatorChanges();
                throw new ConnectionException("Coordinator has changed", e);
            }

        }
    }

    /**
     * convert SlotInfo object to Slot object
     *
     * @param slotInfo object
     * @return slot object
     */
    private static Slot convertSlotInforToSlot(SlotInfo slotInfo) {
        Slot slot = new Slot();
        slot.setStartMessageId(slotInfo.getStartMessageId());
        slot.setEndMessageId(slotInfo.getEndMessageId());
        slot.setQueueName(slotInfo.getQueueName());
        return slot;
    }

    /**
     * updateMessageId method. This method will update the message ID list in the SlotManager
     *
     * @param queueName name of the queue
     * @param messageId a known message ID
     * @throws TException in case of an connection error
     */
    public static synchronized void updateMessageId(String queueName,
                                                    long messageId) throws ConnectionException {
        try {
            client = getServiceClient();
            client.updateMessageId(queueName, messageId);
        } catch (TException e) {
            try {
                //retry once
                reConnectToServer();
                client.updateMessageId(queueName, messageId);
            } catch (TException e1) {
                resetServiceClient();
                throw new ConnectionException("Coordinator has changed", e);
            }

        }
    }

    /**
     * delete the slot from SlotAssignmentMap
     *
     * @param queueName
     * @param slot      to be deleted
     * @throws TException
     */
    public static synchronized void deleteSlot(String queueName, Slot slot,
                                               String nodeId) throws ConnectionException {
        SlotInfo slotInfo = new SlotInfo(slot.getStartMessageId(), slot.getEndMessageId(),
                slot.getQueueName());
        try {
            client = getServiceClient();
            client.deleteSlot(queueName, slotInfo, nodeId);
        } catch (TException e) {
            try {
                //retry to connect once
                reConnectToServer();
                client.deleteSlot(queueName, slotInfo, nodeId);
            } catch (TException e1) {
                resetServiceClient();
                throw new ConnectionException("Coordinator has changed", e);
            }
        }
    }

    /**
     * re-assign the slot when the last subscriber leaves the node
     *
     * @param nodeId    of this node
     * @param queueName name of the queue
     * @throws TException
     */
    public static synchronized void reAssignSlotWhenNoSubscribers(String nodeId,
                                                                  String queueName) throws ConnectionException {
        try {
            client = getServiceClient();
            client.reAssignSlotWhenNoSubscribers(nodeId, queueName);
        } catch (TException e) {
            try {
                //retry to do the operation once
                reConnectToServer();
                client.reAssignSlotWhenNoSubscribers(nodeId, queueName);
            } catch (TException e1) {
                handleCoordinatorChanges();
                throw new ConnectionException("Coordinator has changed", e);
            }
        }
    }

    /**
     * Returns an instance of Slot Management service client
     *
     * @return a SlotManagementService client
     */
    private static SlotManagementService.Client getServiceClient() throws TTransportException {
        if (client == null) {
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
                return new SlotManagementService.Client(protocol);
            } catch (TTransportException e) {
                log.error("Could not initialize the Thrift client. " + e.getMessage(), e);
                throw new TTransportException(
                        "Could not initialize the Thrift client. " + e.getMessage(), e);
            }

        }
        return client;
    }

    /**
     * handle when coordinator of the cluster changes. start reconnecting to new server thread
     */
    private static void handleCoordinatorChanges() {
        resetServiceClient();
        if (isReconnectingStarted()) {
            setReconnectingFlag(true);
        }
        startServerReconnectingThread();
    }

    /**
     * set mbThriftClient to null
     */
    private static void resetServiceClient() {
        client = null;
        transport.close();
    }

    /**
     * Try to reconnect to server by taking latest values in the hazelcalst thrift server details
     * map
     *
     * @throws TTransportException when connecting to thrift server is unsuccessful
     */
    private static void reConnectToServer() throws TTransportException {
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
            client = new SlotManagementService.Client(protocol);
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
    private static void startServerReconnectingThread() {
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
                while (reconnectingStarted) {

                    try {
                        reConnectToServer();
                        /**
                         * If re connect to server is successful, following code segment will be
                         * executed
                         */
                        reconnectingStarted = false;
                        slotDeliveryWorkerManager.startAllSlotDeliveryWorkers();
                    } catch (TTransportException e) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ignored) {
                            //silently ignore
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
