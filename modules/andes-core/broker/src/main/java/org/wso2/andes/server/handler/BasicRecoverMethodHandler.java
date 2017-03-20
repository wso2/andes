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
import org.wso2.andes.framing.BasicRecoverBody;
import org.wso2.andes.framing.ProtocolVersion;
import org.wso2.andes.framing.amqp_8_0.MethodRegistry_8_0;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;

import java.util.Map;

public class BasicRecoverMethodHandler implements StateAwareMethodListener<BasicRecoverBody>
{
    private static final Logger _logger = Logger.getLogger(BasicRecoverMethodHandler.class);

    private static final BasicRecoverMethodHandler _instance = new BasicRecoverMethodHandler();

    public static BasicRecoverMethodHandler getInstance()
    {
        return _instance;
    }

    public void methodReceived(AMQStateManager stateManager, BasicRecoverBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();

        _logger.debug("Recover received on protocol session " + session + " and channel " + channelId);
        AMQChannel channel = session.getChannel(channelId);

        if (channel == null)
        {
            throw body.getChannelNotFoundException(channelId);
        }

        // Qpid 0-8 hacks a synchronous -ok onto recover.
        // In Qpid 0-9 we create a separate sync-recover, sync-recover-ok pair to be "more" compliant
        if(session.getProtocolVersion().equals(ProtocolVersion.v8_0))
        {
            MethodRegistry_8_0 methodRegistry = (MethodRegistry_8_0) session.getMethodRegistry();
            AMQMethodBody recoverOk = methodRegistry.createBasicRecoverOkBody();
            session.writeFrame(recoverOk.generateFrame(channelId));

        }

        channel.resendRecoveredMessages();
    }
}
