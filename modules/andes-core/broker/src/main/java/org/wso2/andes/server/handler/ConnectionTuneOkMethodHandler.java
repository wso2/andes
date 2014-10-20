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
import org.wso2.andes.framing.ConnectionTuneOkBody;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQState;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;

public class ConnectionTuneOkMethodHandler implements StateAwareMethodListener<ConnectionTuneOkBody>
{
    private static final Logger _logger = Logger.getLogger(ConnectionTuneOkMethodHandler.class);

    private static ConnectionTuneOkMethodHandler _instance = new ConnectionTuneOkMethodHandler();

    public static ConnectionTuneOkMethodHandler getInstance()
    {
        return _instance;
    }

    public void methodReceived(AMQStateManager stateManager, ConnectionTuneOkBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();

        if (_logger.isDebugEnabled())
        {
            _logger.debug(body);
        }
        stateManager.changeState(AMQState.CONNECTION_NOT_OPENED);
        session.initHeartbeats(body.getHeartbeat());
        session.setMaxFrameSize(body.getFrameMax());
        
        long maxChannelNumber = body.getChannelMax();
        //0 means no implied limit, except that forced by protocol limitations (0xFFFF)
        session.setMaximumNumberOfChannels( maxChannelNumber == 0 ? 0xFFFFL : maxChannelNumber);
    }
}
