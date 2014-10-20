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
package org.wso2.andes.server.handler;

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQMethodBody;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.ConnectionOpenBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQState;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.virtualhost.VirtualHost;

public class ConnectionOpenMethodHandler implements StateAwareMethodListener<ConnectionOpenBody>
{
    private static final Logger _logger = Logger.getLogger(ConnectionOpenMethodHandler.class);

    private static ConnectionOpenMethodHandler _instance = new ConnectionOpenMethodHandler();

    public static ConnectionOpenMethodHandler getInstance()
    {
        return _instance;
    }

    private ConnectionOpenMethodHandler()
    {
    }

    private static AMQShortString generateClientID()
    {
        return new AMQShortString(Long.toString(System.currentTimeMillis()));
    }

    public void methodReceived(AMQStateManager stateManager, ConnectionOpenBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();

        //ignore leading '/'
        String virtualHostName;
        if ((body.getVirtualHost() != null) && body.getVirtualHost().charAt(0) == '/')
        {
            virtualHostName = new StringBuilder(body.getVirtualHost().subSequence(1, body.getVirtualHost().length())).toString();
        }
        else
        {
            virtualHostName = body.getVirtualHost() == null ? null : String.valueOf(body.getVirtualHost());
        }

        VirtualHost virtualHost = stateManager.getVirtualHostRegistry().getVirtualHost(virtualHostName);

        if (virtualHost == null)
        {
            throw body.getConnectionException(AMQConstant.NOT_FOUND, "Unknown virtual host: '" + virtualHostName + "'");
        }
        else
        {
            // Check virtualhost access
            if (!virtualHost.getSecurityManager().accessVirtualhost(virtualHostName, session.getRemoteAddress()))
            {
                throw body.getConnectionException(AMQConstant.ACCESS_REFUSED, "Permission denied: '" + virtualHost.getName() + "'");
            }

            session.setVirtualHost(virtualHost);

            // See Spec (0.8.2). Section  3.1.2 Virtual Hosts
            if (session.getContextKey() == null)
            {
                session.setContextKey(generateClientID());
            }            

            MethodRegistry methodRegistry = session.getMethodRegistry();
            AMQMethodBody responseBody =  methodRegistry.createConnectionOpenOkBody(body.getVirtualHost());            

            stateManager.changeState(AMQState.CONNECTION_OPEN);

            session.writeFrame(responseBody.generateFrame(channelId));
        }
    }
}
