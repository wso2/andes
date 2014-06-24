/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.server.handler;

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQFrame;
import org.wso2.andes.framing.ConnectionCloseBody;
import org.wso2.andes.framing.ConnectionCloseOkBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.protocol.AMQMethodEvent;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;

public class ConnectionCloseMethodHandler implements StateAwareMethodListener<ConnectionCloseBody>
{
    private static final Logger _logger = Logger.getLogger(ConnectionCloseMethodHandler.class);

    private static ConnectionCloseMethodHandler _instance = new ConnectionCloseMethodHandler();

    public static ConnectionCloseMethodHandler getInstance()
    {
        return _instance;
    }

    private ConnectionCloseMethodHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, ConnectionCloseBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();

        if (_logger.isInfoEnabled())
        {
            _logger.info("ConnectionClose received with reply code/reply text " + body.getReplyCode() + "/" +
                         body.getReplyText() + " for " + session);
        }
        try
        {
            session.closeSession();
        }
        catch (Exception e)
        {
            _logger.error("Error closing protocol session: " + e, e);
        }

        MethodRegistry methodRegistry = session.getMethodRegistry();
        ConnectionCloseOkBody responseBody = methodRegistry.createConnectionCloseOkBody();
        session.writeFrame(responseBody.generateFrame(channelId));

    }
}
