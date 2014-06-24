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
package org.wso2.andes.client.handler;

import org.wso2.andes.AMQException;
import org.wso2.andes.client.protocol.AMQProtocolSession;
import org.wso2.andes.client.state.StateAwareMethodListener;
import org.wso2.andes.framing.ChannelCloseOkBody;
import org.wso2.andes.protocol.AMQConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelCloseOkMethodHandler implements StateAwareMethodListener<ChannelCloseOkBody>
{
    private static final Logger _logger = LoggerFactory.getLogger(ChannelCloseOkMethodHandler.class);

    private static final ChannelCloseOkMethodHandler _instance = new ChannelCloseOkMethodHandler();

    public static ChannelCloseOkMethodHandler getInstance()
    {
        return _instance;
    }

    public void methodReceived(AMQProtocolSession session,  ChannelCloseOkBody method, int channelId)
        throws AMQException
    {
        if (_logger.isDebugEnabled()) {
            _logger.debug("Received channel-close-ok for channel-id " + channelId);
        }

        session.channelClosed(channelId, AMQConstant.REPLY_SUCCESS, "Channel closed successfully");
    }
}
