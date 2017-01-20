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
import org.wso2.andes.framing.DtxSelectBody;
import org.wso2.andes.framing.DtxStartBody;
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
import org.wso2.andes.framing.amqp_0_91.DtxCommitBodyImpl;
import org.wso2.andes.framing.amqp_0_91.DtxEndBodyImpl;
import org.wso2.andes.framing.amqp_0_91.DtxForgetBodyImpl;
import org.wso2.andes.framing.amqp_0_91.DtxPrepareBodyImpl;
import org.wso2.andes.framing.amqp_0_91.DtxRollbackBodyImpl;
import org.wso2.andes.framing.amqp_0_91.DtxStartBodyImpl;
import org.wso2.andes.framing.amqp_0_91.MethodDispatcher_0_91;
import org.wso2.andes.server.state.AMQStateManager;

public class ServerMethodDispatcherImpl_0_91
        extends ServerMethodDispatcherImpl
        implements MethodDispatcher_0_91

{

    private static final BasicRecoverSyncMethodHandler _basicRecoverSyncMethodHandler =
            BasicRecoverSyncMethodHandler.getInstance();
    private static final QueueUnbindHandler _queueUnbindHandler =
            QueueUnbindHandler.getInstance();

    private static final DtxSelectHandler dtxSelectHandler = DtxSelectHandler.getInstance();
    private static final DtxStartHandler dtxStartHandler = DtxStartHandler.getInstance();
    private static final DtxEndHandler dtxEndHandler = DtxEndHandler.getInstance();
    private static final DtxPrepareHandler dtxPrepareHandler = DtxPrepareHandler.getInstance();
    private static final DtxCommitHandler dtxCommitHandler = DtxCommitHandler.getInstance();
    private static final DtxRollbackHandler dtxRollbackHandler = DtxRollbackHandler.getInstance();
    private static final DtxForgetHandler dtxForgetHandler = DtxForgetHandler.getInstance();

    private final AMQStateManager stateManager;

    public ServerMethodDispatcherImpl_0_91(AMQStateManager stateManager)
    {
        super(stateManager);
        this.stateManager = stateManager;
    }

    public boolean dispatchBasicRecoverSync(BasicRecoverSyncBody body, int channelId) throws AMQException
    {
        _basicRecoverSyncMethodHandler.methodReceived(getStateManager(), body, channelId);
        return true;
    }

    @Override
    public boolean dispatchDtxCommit(DtxCommitBody body, int channelId) throws AMQException {
        dtxCommitHandler.methodReceived(stateManager, (DtxCommitBodyImpl) body, channelId);
        return true;
    }

    public boolean dispatchBasicRecoverSyncOk(BasicRecoverSyncOkBody body, int channelId) throws AMQException
    {
        throw new UnexpectedMethodException(body);
    }

    @Override
    public boolean dispatchDtxCommitOk(DtxCommitOkBody body, int channelId) throws AMQException {
        throw new UnexpectedMethodException(body);
    }

    public boolean dispatchDtxSelect(DtxSelectBody body, int channelId) throws AMQException
    {
        dtxSelectHandler.methodReceived(stateManager, body, channelId);
        return true;
    }

    public boolean dispatchDtxStart(DtxStartBody body, int channelId) throws AMQException
    {
        dtxStartHandler.methodReceived(stateManager, (DtxStartBodyImpl) body, channelId);
        return true;
    }

    @Override
    public boolean dispatchDtxEnd(DtxEndBody body, int channelId) throws AMQException {
        dtxEndHandler.methodReceived(stateManager, (DtxEndBodyImpl) body, channelId);
        return true;
    }

    @Override
    public boolean dispatchDtxForget(DtxForgetBody body, int channelId) throws AMQException {
        dtxForgetHandler.methodReceived(stateManager, (DtxForgetBodyImpl) body, channelId);
        return true;
    }

    @Override
    public boolean dispatchDtxPrepare(DtxPrepareBody body, int channelId) throws AMQException {
        dtxPrepareHandler.methodReceived(stateManager, (DtxPrepareBodyImpl) body, channelId);
        return true;
    }

    @Override
    public boolean dispatchDtxRollback(DtxRollbackBody body, int channelId) throws AMQException {
        dtxRollbackHandler.methodReceived(stateManager, (DtxRollbackBodyImpl) body, channelId);
        return true;
    }

    @Override
    public boolean dispatchDtxEndOk(DtxEndOkBody body, int channelId) throws AMQException {
        throw new UnexpectedMethodException(body);
    }

    @Override
    public boolean dispatchDtxForgetOk(DtxForgetOkBody body, int channelId) throws AMQException {
        throw new UnexpectedMethodException(body);
    }

    @Override
    public boolean dispatchDtxPrepareOk(DtxPrepareOkBody body, int channelId) throws AMQException {
        throw new UnexpectedMethodException(body);
    }

    @Override
    public boolean dispatchDtxRollbackOk(DtxRollbackOkBody body, int channelId) throws AMQException {
        throw new UnexpectedMethodException(body);
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
        return false;
    }

    public boolean dispatchMessageAppend(MessageAppendBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchMessageCancel(MessageCancelBody body, int channelId) throws AMQException
    {
        return false;
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
        return false;
    }

    public boolean dispatchMessageEmpty(MessageEmptyBody body, int channelId) throws AMQException
    {
        return false;
    }

    public boolean dispatchMessageGet(MessageGetBody body, int channelId) throws AMQException
    {
        return false;
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
        return false;
    }

    public boolean dispatchMessageRecover(MessageRecoverBody body, int channelId) throws AMQException
    {
        return false;
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

    public boolean dispatchBasicRecoverOk(BasicRecoverOkBody body, int channelId) throws AMQException
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean dispatchQueueUnbindOk(QueueUnbindOkBody body, int channelId) throws AMQException
    {
        throw new UnexpectedMethodException(body);
    }

    public boolean dispatchQueueUnbind(QueueUnbindBody body, int channelId) throws AMQException
    {
        _queueUnbindHandler.methodReceived(getStateManager(),body,channelId);
        return true;
    }
}
