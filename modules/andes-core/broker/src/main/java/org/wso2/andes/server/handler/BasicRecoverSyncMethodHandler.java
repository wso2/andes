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
import org.wso2.andes.AMQChannelException;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQMethodBody;
import org.wso2.andes.framing.BasicRecoverSyncBody;
import org.wso2.andes.framing.ProtocolVersion;
import org.wso2.andes.framing.amqp_0_9.MethodRegistry_0_9;
import org.wso2.andes.framing.amqp_0_91.MethodRegistry_0_91;
import org.wso2.andes.kernel.disruptor.DisruptorEventCallback;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;


public class BasicRecoverSyncMethodHandler implements StateAwareMethodListener<BasicRecoverSyncBody>
{
    private static final Logger _logger = Logger.getLogger(BasicRecoverSyncMethodHandler.class);

    private static final BasicRecoverSyncMethodHandler _instance = new BasicRecoverSyncMethodHandler();

    public static BasicRecoverSyncMethodHandler getInstance()
    {
        return _instance;
    }

    public void methodReceived(final AMQStateManager stateManager,
                               final BasicRecoverSyncBody body, final int channelId) throws AMQException

    {
        final AMQProtocolSession session = stateManager.getProtocolSession();

        _logger.debug("Recover received on protocol session " + session + " and channel " + channelId);
        AMQChannel channel = session.getChannel(channelId);


        if (channel == null)
        {
            throw body.getChannelNotFoundException(channelId);
        }

        channel.resendRecoveredMessages(new DisruptorEventCallback() {

            @Override
            public void execute() {

                AMQChannel channel = session.getChannel(channelId);

                if (channel == null) {
                    _logger.error("Channel not found for id " + channelId);
                }

                // Qpid 0-8 hacks a synchronous -ok onto recover.
                // In Qpid 0-9 we create a separate sync-recover, sync-recover-ok pair to be "more" compliant
                if(session.getProtocolVersion().equals(ProtocolVersion.v0_9)) {
                    MethodRegistry_0_9 methodRegistry = (MethodRegistry_0_9) session.getMethodRegistry();
                    AMQMethodBody recoverOk = methodRegistry.createBasicRecoverSyncOkBody();
                    session.writeFrame(recoverOk.generateFrame(channelId));

                }
                else if(session.getProtocolVersion().equals(ProtocolVersion.v0_91)) {
                    MethodRegistry_0_91 methodRegistry = (MethodRegistry_0_91) session.getMethodRegistry();
                    AMQMethodBody recoverOk = methodRegistry.createBasicRecoverSyncOkBody();
                    session.writeFrame(recoverOk.generateFrame(channelId));

                }
            }

            @Override
            public void onException(Exception exception) {
                _logger.error("Exception occurred when recovering session ", exception);
                AMQChannelException channelException = body.getChannelException(AMQConstant.INTERNAL_ERROR,
                        exception.getMessage());
                session.writeFrame(channelException.getCloseFrame(channelId));
                try {
                    session.closeChannel(channelId);
                } catch (AMQException e) {
                    _logger.error("Couldn't close channel (channel id ) " + channelId, e);
                }

            }
        });
    }
}
