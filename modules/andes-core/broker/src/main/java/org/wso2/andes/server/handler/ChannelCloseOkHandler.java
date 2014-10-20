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
import org.wso2.andes.framing.ChannelCloseOkBody;
import org.wso2.andes.protocol.AMQMethodEvent;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;

public class ChannelCloseOkHandler implements StateAwareMethodListener<ChannelCloseOkBody>
{
    private static final Logger _logger = Logger.getLogger(ChannelCloseOkHandler.class);

    private static ChannelCloseOkHandler _instance = new ChannelCloseOkHandler();

    public static ChannelCloseOkHandler getInstance()
    {
        return _instance;
    }

    private ChannelCloseOkHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, ChannelCloseOkBody body, int channelId) throws AMQException
    {

        _logger.info("Received channel-close-ok for channel-id " + channelId);
        
        // Let the Protocol Session know the channel is now closed.
        stateManager.getProtocolSession().closeChannelOk(channelId);
    }
}
