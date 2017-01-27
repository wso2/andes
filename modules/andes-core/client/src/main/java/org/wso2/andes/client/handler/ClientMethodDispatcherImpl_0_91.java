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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.client.protocol.AMQProtocolHandler;
import org.wso2.andes.client.protocol.AMQProtocolSession;
import org.wso2.andes.client.state.AMQMethodNotImplementedException;
import org.wso2.andes.framing.BasicRecoverOkBody;
import org.wso2.andes.framing.BasicRecoverSyncBody;
import org.wso2.andes.framing.BasicRecoverSyncOkBody;
import org.wso2.andes.framing.ChannelOkBody;
import org.wso2.andes.framing.ChannelPingBody;
import org.wso2.andes.framing.ChannelPongBody;
import org.wso2.andes.framing.ChannelResumeBody;
import org.wso2.andes.framing.DtxCommitBody;
import org.wso2.andes.framing.DtxCommitOkBody;
import org.wso2.andes.framing.DtxEndBody;
import org.wso2.andes.framing.DtxEndOkBody;
import org.wso2.andes.framing.DtxForgetBody;
import org.wso2.andes.framing.DtxForgetOkBody;
import org.wso2.andes.framing.DtxPrepareBody;
import org.wso2.andes.framing.DtxPrepareOkBody;
import org.wso2.andes.framing.DtxRollbackBody;
import org.wso2.andes.framing.DtxRollbackOkBody;
import org.wso2.andes.framing.DtxSetTimeoutBody;
import org.wso2.andes.framing.DtxSetTimeoutOkBody;
import org.wso2.andes.framing.DtxStartOkBody;
import org.wso2.andes.framing.MessageAppendBody;
import org.wso2.andes.framing.MessageCancelBody;
import org.wso2.andes.framing.MessageCheckpointBody;
import org.wso2.andes.framing.MessageCloseBody;
import org.wso2.andes.framing.MessageConsumeBody;
import org.wso2.andes.framing.MessageEmptyBody;
import org.wso2.andes.framing.MessageGetBody;
import org.wso2.andes.framing.MessageOffsetBody;
import org.wso2.andes.framing.MessageOkBody;
import org.wso2.andes.framing.MessageOpenBody;
import org.wso2.andes.framing.MessageQosBody;
import org.wso2.andes.framing.MessageRecoverBody;
import org.wso2.andes.framing.MessageRejectBody;
import org.wso2.andes.framing.MessageResumeBody;
import org.wso2.andes.framing.MessageTransferBody;
import org.wso2.andes.framing.QueueUnbindBody;
import org.wso2.andes.framing.QueueUnbindOkBody;
import org.wso2.andes.framing.amqp_0_91.DtxStartOkBodyImpl;
import org.wso2.andes.framing.amqp_0_91.MethodDispatcher_0_91;

public class ClientMethodDispatcherImpl_0_91 extends ClientMethodDispatcherImpl implements MethodDispatcher_0_91
{
    /**
     * Class logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AMQProtocolHandler.class);

    public ClientMethodDispatcherImpl_0_91(AMQProtocolSession session)
    {
        super(session);
    }

    public boolean dispatchBasicRecoverSyncOk(BasicRecoverSyncOkBody body, int channelId) throws AMQException
    {
        return false;
    }

    @Override
    public boolean dispatchDtxCommitOk(DtxCommitOkBody body, int channelId) throws AMQException {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Received Dtx.end-Ok message, with status: " + body.getXaResult());
        }
        return true;
    }

    public boolean dispatchBasicRecoverSync(BasicRecoverSyncBody body, int channelId) throws AMQException
    {
        throw new AMQMethodNotImplementedException(body);
    }

    @Override
    public boolean dispatchDtxCommit(DtxCommitBody body, int channelId) throws AMQException {
        throw new AMQMethodNotImplementedException(body);
    }

    public boolean dispatchChannelOk(ChannelOkBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchChannelPing(ChannelPingBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchChannelPong(ChannelPongBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchChannelResume(ChannelResumeBody body, int channelId) throws AMQException
    {
        throw new AMQMethodNotImplementedException(body);
    }

    public boolean dispatchMessageAppend(MessageAppendBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchMessageCancel(MessageCancelBody body, int channelId) throws AMQException
    {
        throw new AMQMethodNotImplementedException(body);
    }

    public boolean dispatchMessageCheckpoint(MessageCheckpointBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchMessageClose(MessageCloseBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchMessageConsume(MessageConsumeBody body, int channelId) throws AMQException
    {
        throw new AMQMethodNotImplementedException(body);
    }

    public boolean dispatchMessageEmpty(MessageEmptyBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchMessageGet(MessageGetBody body, int channelId) throws AMQException
    {
        throw new AMQMethodNotImplementedException(body);
    }

    public boolean dispatchMessageOffset(MessageOffsetBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchMessageOk(MessageOkBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchMessageOpen(MessageOpenBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchMessageQos(MessageQosBody body, int channelId) throws AMQException
    {
        throw new AMQMethodNotImplementedException(body);
    }

    public boolean dispatchMessageRecover(MessageRecoverBody body, int channelId) throws AMQException
    {
        throw new AMQMethodNotImplementedException(body);
    }

    public boolean dispatchMessageReject(MessageRejectBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchMessageResume(MessageResumeBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchMessageTransfer(MessageTransferBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchQueueUnbind(QueueUnbindBody body, int channelId) throws AMQException
    {
        throw new AMQMethodNotImplementedException(body);
    }

    public boolean dispatchBasicRecoverOk(BasicRecoverOkBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchQueueUnbindOk(QueueUnbindOkBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchDtxStartOk(DtxStartOkBody body, int channelId) throws AMQException
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Received dtx.start-ok message, with status: " + ((DtxStartOkBodyImpl )body).getXaResult());
        }
        return true;
    }

    @Override
    public boolean dispatchDtxEndOk(DtxEndOkBody body, int channelId) throws AMQException {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Received dtx.end-ok message, with status: " + body.getXaResult());
        }
        return true;
    }

    @Override
    public boolean dispatchDtxForgetOk(DtxForgetOkBody body, int channelId) throws AMQException {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Received dtx.forget-ok message, with status: " + body.getXaResult());
        }
        return true;
    }

    @Override
    public boolean dispatchDtxPrepareOk(DtxPrepareOkBody body, int channelId) throws AMQException {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Received dtx.prepare-ok message, with status: " + body.getXaResult());
        }
        return true;
    }

    @Override
    public boolean dispatchDtxRollbackOk(DtxRollbackOkBody body, int channelId) throws AMQException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Received dtx.rollback-ok message, with status: " + body.getXaResult());
        }
        return true;
    }

    @Override
    public boolean dispatchDtxSetTimeoutOk(DtxSetTimeoutOkBody body, int channelId) throws AMQException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Received dtx.set-timeout-ok message, with status: " + body.getXaResult());
        }
        return true;
    }

    @Override
    public boolean dispatchDtxEnd(DtxEndBody body, int channelId) throws AMQException {
        throw new AMQMethodNotImplementedException(body);
    }

    @Override
    public boolean dispatchDtxForget(DtxForgetBody body, int channelId) throws AMQException {
        throw new AMQMethodNotImplementedException(body);
    }

    @Override
    public boolean dispatchDtxPrepare(DtxPrepareBody body, int channelId) throws AMQException {
        throw new AMQMethodNotImplementedException(body);
    }

    @Override
    public boolean dispatchDtxRollback(DtxRollbackBody body, int channelId) throws AMQException {
        throw new AMQMethodNotImplementedException(body);
    }

    @Override
    public boolean dispatchDtxSetTimeout(DtxSetTimeoutBody body, int channelId) throws AMQException {
        return false;
    }

}