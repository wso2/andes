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
import org.wso2.andes.amqp.AMQPAuthenticationManager;
import org.wso2.andes.framing.ConnectionCloseBody;
import org.wso2.andes.framing.ConnectionSecureOkBody;
import org.wso2.andes.framing.ConnectionTuneBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.state.AMQState;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

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
        MethodRegistry methodRegistry = session.getMethodRegistry();
        try {
                        Subject authSubject = AMQPAuthenticationManager.authenticate(body.getResponse());

            _logger.info("Connected as: " + AMQPAuthenticationManager.extractUserPrincipalFromSubject(authSubject));
                       stateManager.changeState(AMQState.CONNECTION_NOT_TUNED);

            ConnectionTuneBody tuneBody =
                    methodRegistry.createConnectionTuneBody(ApplicationRegistry.getInstance().getConfiguration()
                                    .getMaxChannelCount(),
                            ConnectionStartOkMethodHandler.getConfiguredFrameSize(),
                            ApplicationRegistry.getInstance().getConfiguration().getHeartBeatDelay());
            session.writeFrame(tuneBody.generateFrame(0));
            session.setAuthorizedSubject(authSubject);
        } catch (LoginException e) {
            _logger.error("Authentication failure.", e);
            stateManager.changeState(AMQState.CONNECTION_CLOSING);
            ConnectionCloseBody connectionCloseBody =
                                        methodRegistry.createConnectionCloseBody(AMQConstant.NOT_ALLOWED.getCode(),
                                                       AMQConstant.NOT_ALLOWED.getName(),
                                                       body.getClazz(),
                                                       body.getMethod());





            session.writeFrame(connectionCloseBody.generateFrame(0));
        }
    }
}
