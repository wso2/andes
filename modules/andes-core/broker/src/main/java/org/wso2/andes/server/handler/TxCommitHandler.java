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
import org.wso2.andes.framing.TxCommitBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.framing.AMQMethodBody;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;

public class TxCommitHandler implements StateAwareMethodListener<TxCommitBody>
{
    private static final Logger _log = Logger.getLogger(TxCommitHandler.class);

    private static TxCommitHandler _instance = new TxCommitHandler();

    public static TxCommitHandler getInstance()
    {
        return _instance;
    }

    private TxCommitHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, TxCommitBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();

        try
        {
            if (_log.isDebugEnabled())
            {
                _log.debug("Commit received on channel " + channelId);
            }
            AMQChannel channel = session.getChannel(channelId);

            if (channel == null)
            {
                throw body.getChannelNotFoundException(channelId);
            }
            channel.commit();

            MethodRegistry methodRegistry = session.getMethodRegistry();
            AMQMethodBody responseBody = methodRegistry.createTxCommitOkBody();
            session.writeFrame(responseBody.generateFrame(channelId));
                        
        }
        catch (AMQException e)
        {
            throw body.getChannelException(e.getErrorCode(), "Failed to commit: " + e.getMessage());
        }
    }
}
