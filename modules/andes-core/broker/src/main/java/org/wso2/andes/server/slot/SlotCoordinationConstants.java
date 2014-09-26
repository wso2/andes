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
package org.wso2.andes.server.slot;

/**
 * This class contains all constants used in slot management
 */
public final class SlotCoordinationConstants {

    /**
     * number of slot delivery worker threads running inn one MB node
     */
    public static int NUMBER_OF_SLOT_DELIVERY_WORKER_THREADS = 5;


    /**
     * timeout in milliseconds for messages in the slot. When this timeout is exceeded slot will be
     * submitted to the coordinator
     */
    public static long SLOT_SUBMIT_TIMEOUT = 1000;

    /**
     * IP of the coordinator's thrift server
     */
    public static String THRIFT_COORDINATOR_SERVER_IP = "thriftCoordinatorServerIP";

    /**
     * Port of the coordinator's thrift server
     */
    public static String THRIFT_COORDINATOR_SERVER_PORT = "thriftCoordinatorServerPort";

    /**
     * Maximum number of messages that will be read from message store each time in standalone mode
     */
    public static int STANDALONE_SLOT_THRESHOLD = 1000;

}
