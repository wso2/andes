/*
 *  Copyright (c) 2017, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.wso2.andes.server.handler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQMethodBody;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.framing.TxRollbackWithContextBody;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;

/**
 * This will handle the custom rollback frame which includes the last dispatched message ID on the client side.
 */
public class TxRollbackWithContextHandler implements StateAwareMethodListener<TxRollbackWithContextBody> {

    private static Log log = LogFactory.getLog(TxRollbackWithContextHandler.class);

    private static TxRollbackWithContextHandler _instance = new TxRollbackWithContextHandler();

    public static TxRollbackWithContextHandler getInstance() {
        return _instance;
    }

    @Override
    public void methodReceived(AMQStateManager stateManager, TxRollbackWithContextBody body, final int channelId)
            throws AMQException {

        final AMQProtocolSession session = stateManager.getProtocolSession();

        if (log.isDebugEnabled()) {
            log.debug("Rollback frame received with lastDispatchedDeliveryTag: " + body.getLastDispatchedDeliveryTag());
        }

        try {
            AMQChannel channel = session.getChannel(channelId);

            if (channel == null) {
                throw body.getChannelNotFoundException(channelId);
            }

            QueueEntry message = channel.getUnacknowledgedMessageMap().get(body.getLastDispatchedDeliveryTag());

            channel.getSubscription(body.getConsumerTag()).setJMSRollbackInProgress(true);

            if (null != message) {
                channel.setLastRollbackedMessageId(message.getMessage().getMessageNumber());
            }

            final MethodRegistry methodRegistry = session.getMethodRegistry();
            final AMQMethodBody responseBody = methodRegistry.createTxRollbackOkBody();

            Runnable task = new Runnable() {

                public void run() {
                    session.writeFrame(responseBody.generateFrame(channelId));
                }
            };

            channel.rollback(task);

        } catch (AMQException e) {
            throw body.getChannelException(e.getErrorCode(), "Failed to rollback: " + e.getMessage());
        }
    }
}