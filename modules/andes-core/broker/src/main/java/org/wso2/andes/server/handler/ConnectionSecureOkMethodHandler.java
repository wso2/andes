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


import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.ConnectionCloseBody;
import org.wso2.andes.framing.ConnectionSecureBody;
import org.wso2.andes.framing.ConnectionSecureOkBody;
import org.wso2.andes.framing.ConnectionTuneBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.security.auth.AuthenticationResult;
import org.wso2.andes.server.security.auth.manager.AuthenticationManager;
import org.wso2.andes.server.security.auth.sasl.UsernamePrincipal;
import org.wso2.andes.server.state.AMQState;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;

public class ConnectionSecureOkMethodHandler implements StateAwareMethodListener<ConnectionSecureOkBody>
{
    private static final Logger _logger = Logger.getLogger(ConnectionSecureOkMethodHandler.class);

    private static ConnectionSecureOkMethodHandler _instance = new ConnectionSecureOkMethodHandler();

    public static ConnectionSecureOkMethodHandler getInstance()
    {
        return _instance;
    }

    private ConnectionSecureOkMethodHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, ConnectionSecureOkBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();

        AuthenticationManager authMgr = ApplicationRegistry.getInstance().getAuthenticationManager();

        SaslServer ss = session.getSaslServer();
        if (ss == null)
        {
            throw new AMQException("No SASL context set up in session");
        }
        MethodRegistry methodRegistry = session.getMethodRegistry();
        AuthenticationResult authResult = authMgr.authenticate(ss, body.getResponse());
        switch (authResult.getStatus())
        {
            case ERROR:
                Exception cause = authResult.getCause();

                _logger.info("Authentication failed:" + (cause == null ? "" : cause.getMessage()));

                // This should be abstracted
                stateManager.changeState(AMQState.CONNECTION_CLOSING);

                ConnectionCloseBody connectionCloseBody =
                        methodRegistry.createConnectionCloseBody(AMQConstant.NOT_ALLOWED.getCode(),
                                                                 AMQConstant.NOT_ALLOWED.getName(),
                                                                 body.getClazz(),
                                                                 body.getMethod());

                session.writeFrame(connectionCloseBody.generateFrame(0));
                disposeSaslServer(session);
                break;
            case SUCCESS:
                if (_logger.isInfoEnabled())
                {
                    _logger.info("Connected as: " + UsernamePrincipal.getUsernamePrincipalFromSubject(authResult.getSubject()));
                }
                stateManager.changeState(AMQState.CONNECTION_NOT_TUNED);

                ConnectionTuneBody tuneBody =
                        methodRegistry.createConnectionTuneBody(ApplicationRegistry.getInstance().getConfiguration().getMaxChannelCount(),
                                                                ConnectionStartOkMethodHandler.getConfiguredFrameSize(),
                                                                ApplicationRegistry.getInstance().getConfiguration().getHeartBeatDelay());
                session.writeFrame(tuneBody.generateFrame(0));
                session.setAuthorizedSubject(authResult.getSubject());
                disposeSaslServer(session);                
                break;
            case CONTINUE:
                stateManager.changeState(AMQState.CONNECTION_NOT_AUTH);

                ConnectionSecureBody secureBody = methodRegistry.createConnectionSecureBody(authResult.getChallenge());
                session.writeFrame(secureBody.generateFrame(0));
        }
    }

    private void disposeSaslServer(AMQProtocolSession ps)
    {
        SaslServer ss = ps.getSaslServer();
        if (ss != null)
        {
            ps.setSaslServer(null);
            try
            {
                ss.dispose();
            }
            catch (SaslException e)
            {
                _logger.error("Error disposing of Sasl server: " + e);
            }
        }
    }
}
