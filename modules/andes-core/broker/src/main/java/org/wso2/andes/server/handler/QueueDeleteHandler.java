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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.QpidAMQPBridge;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.framing.QueueDeleteBody;
import org.wso2.andes.framing.QueueDeleteOkBody;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.protocol.AMQSessionModel;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.store.DurableConfigurationStore;
import org.wso2.andes.server.virtualhost.VirtualHost;

public class QueueDeleteHandler implements StateAwareMethodListener<QueueDeleteBody>
{
	
    private final static Logger _logger = LoggerFactory.getLogger(QueueDeleteHandler.class);

    private static final QueueDeleteHandler _instance = new QueueDeleteHandler();

    public static QueueDeleteHandler getInstance()
    {
        return _instance;
    }

    private final boolean _failIfNotFound;

    public QueueDeleteHandler()
    {
        this(true);
    }

    public QueueDeleteHandler(boolean failIfNotFound)
    {
        _failIfNotFound = failIfNotFound;

    }

    public void methodReceived(AMQStateManager stateManager, QueueDeleteBody body, int channelId) throws AMQException
    {
        AMQProtocolSession protocolConnection = stateManager.getProtocolSession();
        VirtualHost virtualHost = protocolConnection.getVirtualHost();
        QueueRegistry queueRegistry = virtualHost.getQueueRegistry();
        DurableConfigurationStore store = virtualHost.getDurableConfigurationStore();

        final AMQQueue queue;
        if (body.getQueue() == null)
        {
            AMQChannel channel = protocolConnection.getChannel(channelId);

            if (channel == null)
            {
                throw body.getChannelNotFoundException(channelId);
            }

            //get the default queue on the channel:
            queue = channel.getDefaultQueue();
        }
        else
        {
            queue = queueRegistry.getQueue(body.getQueue());
        }

        if (queue == null)
        {
            if (_failIfNotFound)
            {
                throw body.getChannelException(AMQConstant.NOT_FOUND, "Queue " + body.getQueue() + " does not exist.");
            }
        }
        else
        {
            if (body.getIfEmpty() && !queue.isEmpty())
            {
                throw body.getChannelException(AMQConstant.IN_USE, "Queue: " + body.getQueue() + " is not empty.");
            }
            else if (body.getIfUnused() && !queue.isUnused())
            {
                // TODO - Error code
                throw body.getChannelException(AMQConstant.IN_USE, "Queue: " + body.getQueue() + " is still used.");
            }
            else
            {
                AMQSessionModel session = queue.getExclusiveOwningSession();
                if (queue.isExclusive() && !queue.isDurable() && (session == null || session.getConnectionModel() != protocolConnection))
                {
                    throw body.getConnectionException(AMQConstant.NOT_ALLOWED,
                                                      "Queue " + queue.getNameShortString() + " is exclusive, but not created on this Connection.");
                }
                
                int purged = queue.delete();

                if (queue.isDurable())
                {
                    store.removeQueue(queue);

                    //tell Andes Kernel to remove queue
                    QpidAMQPBridge.getInstance().deleteQueue(queue);
                }

                MethodRegistry methodRegistry = protocolConnection.getMethodRegistry();
                QueueDeleteOkBody responseBody = methodRegistry.createQueueDeleteOkBody(purged);
                protocolConnection.writeFrame(responseBody.generateFrame(channelId));
            }
        }
    }
}
