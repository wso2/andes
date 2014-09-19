/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.wso2.andes.server.slot.thrift.gen.SlotManagementService;

public class MBUtils {

    private static final Log log = LogFactory.getLog(MBUtils.class);

    /**
     * Returns an instance of MB thrift service client
     *
     * @param hostName           thrift server host name
     * @param port               thrift server port client should connect to
     * @return a MB thrift service client
     */
    public static SlotManagementService.Client getCGThriftClient(final String hostName,final int port) {
        try {

            TTransport transport;
            transport = new TSocket(hostName, port);
            transport.open();
            TProtocol protocol = new  TBinaryProtocol(transport);
            SlotManagementService.Client client = new SlotManagementService.Client(protocol);
            return new SlotManagementService.Client(protocol);
        } catch (TTransportException e) {
            log.error("Could not initialize the Thrift client. " + e.getMessage(), e);
            throw new RuntimeException("Could not initialize the Thrift client. " + e.getMessage(), e);
        }
    }

}
