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

import org.wso2.andes.AMQException;
import org.wso2.andes.framing.QueuePurgeBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.framing.AMQMethodBody;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.protocol.AMQSessionModel;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.AMQChannel;

public class QueuePurgeHandler implements StateAwareMethodListener<QueuePurgeBody>
{
    private static final QueuePurgeHandler _instance = new QueuePurgeHandler();

    public static QueuePurgeHandler getInstance()
    {
        return _instance;
    }

    private final boolean _failIfNotFound;

    public QueuePurgeHandler()
    {
        this(true);
    }

    public QueuePurgeHandler(boolean failIfNotFound)
    {
        _failIfNotFound = failIfNotFound;
    }

    public void methodReceived(AMQStateManager stateManager, QueuePurgeBody body, int channelId) throws AMQException
    {
        AMQProtocolSession protocolConnection = stateManager.getProtocolSession();
        VirtualHost virtualHost = protocolConnection.getVirtualHost();
        QueueRegistry queueRegistry = virtualHost.getQueueRegistry();

        AMQChannel channel = protocolConnection.getChannel(channelId);


        AMQQueue queue;
        if(body.getQueue() == null)
        {

           if (channel == null)
           {
               throw body.getChannelNotFoundException(channelId);
           }

           //get the default queue on the channel:
           queue = channel.getDefaultQueue();
            
            if(queue == null)
            {
                if(_failIfNotFound)
                {
                    throw body.getConnectionException(AMQConstant.NOT_ALLOWED,"No queue specified.");
                }
            }
        }
        else
        {
            queue = queueRegistry.getQueue(body.getQueue());
        }

        if(queue == null)
        {
            if(_failIfNotFound)
            {
                throw body.getChannelException(AMQConstant.NOT_FOUND, "Queue " + body.getQueue() + " does not exist.");
            }
        }
        else
        {
                AMQSessionModel session = queue.getExclusiveOwningSession();

                if (queue.isExclusive() && (session == null || session.getConnectionModel() != protocolConnection))
                {
                    throw body.getConnectionException(AMQConstant.NOT_ALLOWED,
                                                      "Queue is exclusive, but not created on this Connection.");
                }

                long purged = queue.clearQueue();


                if(!body.getNowait())
                {

                    MethodRegistry methodRegistry = protocolConnection.getMethodRegistry();
                    AMQMethodBody responseBody = methodRegistry.createQueuePurgeOkBody(purged);
                    protocolConnection.writeFrame(responseBody.generateFrame(channelId));

                }
        }
    }
}
