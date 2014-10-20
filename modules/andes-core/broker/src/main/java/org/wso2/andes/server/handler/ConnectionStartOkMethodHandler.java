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

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.ConnectionCloseBody;
import org.wso2.andes.framing.ConnectionSecureBody;
import org.wso2.andes.framing.ConnectionStartOkBody;
import org.wso2.andes.framing.ConnectionTuneBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.configuration.ServerConfiguration;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.security.auth.AuthenticationResult;
import org.wso2.andes.server.security.auth.manager.AuthenticationManager;
import org.wso2.andes.server.security.auth.sasl.UsernamePrincipal;
import org.wso2.andes.server.state.AMQState;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;


public class ConnectionStartOkMethodHandler implements StateAwareMethodListener<ConnectionStartOkBody>
{
    private static final Logger _logger = Logger.getLogger(ConnectionStartOkMethodHandler.class);

    private static ConnectionStartOkMethodHandler _instance = new ConnectionStartOkMethodHandler();

    public static ConnectionStartOkMethodHandler getInstance()
    {
        return _instance;
    }

    private ConnectionStartOkMethodHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, ConnectionStartOkBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();
        
        _logger.info("SASL Mechanism selected: " + body.getMechanism());
        _logger.info("Locale selected: " + body.getLocale());

        AuthenticationManager authMgr = ApplicationRegistry.getInstance().getAuthenticationManager();
        SaslServer ss = null;
        try
        {                       
            ss = authMgr.createSaslServer(String.valueOf(body.getMechanism()), session.getLocalFQDN());

            if (ss == null)
            {
                throw body.getConnectionException(AMQConstant.RESOURCE_ERROR, "Unable to create SASL Server:" + body.getMechanism());
            }

            session.setSaslServer(ss);

            final AuthenticationResult authResult = authMgr.authenticate(ss, body.getResponse());
            //save clientProperties
            if (session.getClientProperties() == null)
            {
                session.setClientProperties(body.getClientProperties());
            }

            MethodRegistry methodRegistry = session.getMethodRegistry();

            switch (authResult.getStatus())
            {
                case ERROR:
                    Exception cause = authResult.getCause();

                    _logger.info("Authentication failed:" + (cause == null ? "" : cause.getMessage()));

                    stateManager.changeState(AMQState.CONNECTION_CLOSING);

                    ConnectionCloseBody closeBody =
                            methodRegistry.createConnectionCloseBody(AMQConstant.NOT_ALLOWED.getCode(),    // replyCode
                                                                     AMQConstant.NOT_ALLOWED.getName(),
                                                                     body.getClazz(),
                                                                     body.getMethod());

                    session.writeFrame(closeBody.generateFrame(0));
                    disposeSaslServer(session);
                    break;

                case SUCCESS:
                    if (_logger.isInfoEnabled())
                    {
                        _logger.info("Connected as: " + UsernamePrincipal.getUsernamePrincipalFromSubject(authResult.getSubject()));
                    }
                    session.setAuthorizedSubject(authResult.getSubject());

                    stateManager.changeState(AMQState.CONNECTION_NOT_TUNED);

                    ConnectionTuneBody tuneBody = methodRegistry.createConnectionTuneBody(ApplicationRegistry.getInstance().getConfiguration().getMaxChannelCount(),
                                                                                          getConfiguredFrameSize(),
                                                                                          ApplicationRegistry.getInstance().getConfiguration().getHeartBeatDelay());
                    session.writeFrame(tuneBody.generateFrame(0));
                    break;
                case CONTINUE:
                    stateManager.changeState(AMQState.CONNECTION_NOT_AUTH);

                    ConnectionSecureBody secureBody = methodRegistry.createConnectionSecureBody(authResult.getChallenge());
                    session.writeFrame(secureBody.generateFrame(0));
            }
        }
        catch (SaslException e)
        {
            disposeSaslServer(session);
            throw new AMQException("SASL error: " + e, e);
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

    static int getConfiguredFrameSize()
    {
        final ServerConfiguration config = ApplicationRegistry.getInstance().getConfiguration();
        final int framesize = config.getFrameSize();
        _logger.info("Framesize set to " + framesize);
        return framesize;
    }
}



