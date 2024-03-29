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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQChannelException;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.QpidAndesBridge;
import org.wso2.andes.framing.AMQMethodBody;
import org.wso2.andes.framing.ChannelFlowBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.kernel.disruptor.DisruptorEventCallback;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;

public class ChannelFlowHandler implements StateAwareMethodListener<ChannelFlowBody>
{
    private static final Log logger = LogFactory.getLog(ChannelFlowHandler.class);

    private static ChannelFlowHandler _instance = new ChannelFlowHandler();

    public static ChannelFlowHandler getInstance()
    {
        return _instance;
    }

    private ChannelFlowHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, final ChannelFlowBody body, final int channelId)
            throws AMQException {

        final AMQProtocolSession session = stateManager.getProtocolSession();
        AMQChannel channel = session.getChannel(channelId);

        if (null == channel) {
            throw body.getChannelNotFoundException(channelId);
        }

        channel.setSuspended(!body.getActive());

        QpidAndesBridge.notifyChannelFlow(channel.getId(), body.getActive(), new DisruptorEventCallback() {
            @Override
            public void execute() {
                MethodRegistry methodRegistry = session.getMethodRegistry();
                AMQMethodBody responseBody = methodRegistry.createChannelFlowOkBody(body.getActive());
                session.writeFrame(responseBody.generateFrame(channelId));
            }

            @Override
            public void onException(Exception exception) {
                logger.error("Exception occurred while processing channel flow: " + body.getActive(),
                             exception);
                AMQChannelException channelException = body.getChannelException(AMQConstant.INTERNAL_ERROR,
                                                                                exception.getMessage());
                session.writeFrame(channelException.getCloseFrame(channelId));
                try {
                    session.closeChannel(channelId);
                } catch (AMQException e) {
                    logger.error("Couldn't close channel (channel id ) " + channelId, e);
                }

            }
        });
        if (logger.isDebugEnabled()) {
            logger.debug(" Channel flow: " + body.getActive() + " set for channel: " + channel.getId());
        }
    }
}
