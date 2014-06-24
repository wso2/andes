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

import org.wso2.andes.AMQException;
import org.wso2.andes.framing.BasicCancelBody;
import org.wso2.andes.framing.BasicCancelOkBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.apache.log4j.Logger;

public class BasicCancelMethodHandler implements StateAwareMethodListener<BasicCancelBody>
{
    private static final Logger _log = Logger.getLogger(BasicCancelMethodHandler.class);

    private static final BasicCancelMethodHandler _instance = new BasicCancelMethodHandler();

    public static BasicCancelMethodHandler getInstance()
    {
        return _instance;
    }

    private BasicCancelMethodHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, BasicCancelBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();

        final AMQChannel channel = session.getChannel(channelId);


        if (channel == null)
        {
            throw body.getChannelNotFoundException(channelId);
        }

        if (_log.isDebugEnabled())
        {
            _log.debug("BasicCancel: for:" + body.getConsumerTag() +
                       " nowait:" + body.getNowait());
        }

        channel.unsubscribeConsumer(body.getConsumerTag());
        if (!body.getNowait())
        {
            MethodRegistry methodRegistry = session.getMethodRegistry();
            BasicCancelOkBody cancelOkBody = methodRegistry.createBasicCancelOkBody(body.getConsumerTag());
            session.writeFrame(cancelOkBody.generateFrame(channelId));
        }
    }
}
