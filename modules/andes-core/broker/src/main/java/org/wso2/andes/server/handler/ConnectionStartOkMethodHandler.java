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

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPAuthenticationManager;
import org.wso2.andes.configuration.qpid.ServerConfiguration;
import org.wso2.andes.framing.ConnectionCloseBody;
import org.wso2.andes.framing.ConnectionStartOkBody;
import org.wso2.andes.framing.ConnectionTuneBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.registry.ApplicationRegistry;
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
        MethodRegistry methodRegistry = session.getMethodRegistry();

        try {
            Subject subject = AMQPAuthenticationManager.authenticate(body.getResponse());

            _logger.info("Connected as: " + AMQPAuthenticationManager.extractUserPrincipalFromSubject(subject)
                    .getName());
            session.setAuthorizedSubject(subject);

            stateManager.changeState(AMQState.CONNECTION_NOT_TUNED);

            ConnectionTuneBody tuneBody = methodRegistry.createConnectionTuneBody(ApplicationRegistry.getInstance().getConfiguration().getMaxChannelCount(),
                    getConfiguredFrameSize(),
                    ApplicationRegistry.getInstance().getConfiguration().getHeartBeatDelay());
            session.writeFrame(tuneBody.generateFrame(0));
        } catch (LoginException e) {

            _logger.warn("Authentication attemp failed.", e);

            stateManager.changeState(AMQState.CONNECTION_CLOSING);

            ConnectionCloseBody closeBody =
                    methodRegistry.createConnectionCloseBody(AMQConstant.NOT_ALLOWED.getCode(),    // replyCode
                            AMQConstant.NOT_ALLOWED.getName(),
                            body.getClazz(),
                            body.getMethod());

            session.writeFrame(closeBody.generateFrame(0));
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



