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
import org.wso2.andes.framing.ChannelCloseBody;
import org.wso2.andes.framing.ChannelCloseOkBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.protocol.AMQMethodEvent;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.AMQChannel;

public class ChannelCloseHandler implements StateAwareMethodListener<ChannelCloseBody>
{
    private static final Logger _logger = Logger.getLogger(ChannelCloseHandler.class);

    private static ChannelCloseHandler _instance = new ChannelCloseHandler();

    public static ChannelCloseHandler getInstance()
    {
        return _instance;
    }

    private ChannelCloseHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, ChannelCloseBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();

        if (_logger.isInfoEnabled())
        {
            _logger.info("Received channel close for id " + channelId + " citing class " + body.getClassId() +
                         " and method " + body.getMethodId());
        }


        AMQChannel channel = session.getChannel(channelId);

        if (channel == null)
        {
            throw body.getConnectionException(AMQConstant.CHANNEL_ERROR, "Trying to close unknown channel");
        }

        session.closeChannel(channelId);
        // Client requested closure so we don't wait for ok we send it
        stateManager.getProtocolSession().closeChannelOk(channelId);

        MethodRegistry methodRegistry = session.getMethodRegistry();
        ChannelCloseOkBody responseBody = methodRegistry.createChannelCloseOkBody();
        session.writeFrame(responseBody.generateFrame(channelId));
    }
}
