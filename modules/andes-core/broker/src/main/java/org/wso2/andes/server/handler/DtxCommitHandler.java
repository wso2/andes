/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.server.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.AMQChannelException;
import org.wso2.andes.AMQException;
import org.wso2.andes.dtx.XidImpl;
import org.wso2.andes.framing.DtxCommitOkBody;
import org.wso2.andes.framing.amqp_0_91.DtxCommitBodyImpl;
import org.wso2.andes.framing.amqp_0_91.MethodRegistry_0_91;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.disruptor.DisruptorEventCallback;
import org.wso2.andes.kernel.dtx.UnknownDtxBranchException;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.txn.DtxNotSelectedException;
import org.wso2.andes.server.txn.IncorrectDtxStateException;
import org.wso2.andes.server.txn.RollbackOnlyDtxException;
import org.wso2.andes.server.txn.TimeoutDtxException;
import org.wso2.andes.transport.DtxXaStatus;

import javax.transaction.xa.Xid;

public class DtxCommitHandler implements StateAwareMethodListener<DtxCommitBodyImpl> {
    /**
     * Class logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DtxCommitHandler.class);
    private static DtxCommitHandler _instance = new DtxCommitHandler();

    public static DtxCommitHandler getInstance() {
        return _instance;
    }

    private DtxCommitHandler() {
    }

    @Override
    public void methodReceived(final AMQStateManager stateManager, final DtxCommitBodyImpl body, final int channelId)
            throws AMQException {
        Xid xid = new XidImpl(body.getBranchId(), body.getFormat(), body.getGlobalId());
        final AMQProtocolSession session = stateManager.getProtocolSession();

        final AMQChannel channel = session.getChannel(channelId);

        if (channel == null) {
            throw body.getChannelNotFoundException(channelId);
        }

        try {
            channel.commitDtxTransaction(xid, body.getOnePhase(), new DisruptorEventCallback() {

                @Override
                public void execute() {
                    MethodRegistry_0_91 methodRegistry = (MethodRegistry_0_91) session.getMethodRegistry();
                    DtxCommitOkBody dtxCommitOkBody = methodRegistry.createDtxCommitOkBody(
                            DtxXaStatus.XA_OK.getValue());
                    session.writeFrame(dtxCommitOkBody.generateFrame(channelId));
                }

                @Override
                public void onException(Exception exception) {
                    LOGGER.error("Exception occurred when committing ", exception);
                    AMQChannelException channelException = body.getChannelException(AMQConstant.INTERNAL_ERROR,
                                                                                    exception.getMessage());
                    session.writeFrame(channelException.getCloseFrame(channelId));
                    try {
                        session.closeChannel(channelId);
                    } catch (AMQException e) {
                        LOGGER.error("Couldn't close channel (channel id ) " + channelId, e);
                    }

                }
            });

        } catch (DtxNotSelectedException e) {
            throw body.getChannelException(AMQConstant.COMMAND_INVALID, "Dtx not selected ", e);
        } catch (UnknownDtxBranchException e) {
            throw body.getChannelException(AMQConstant.NOT_FOUND, "Commit failed. Unknown XID ", e);
        } catch (IncorrectDtxStateException e) {
            throw body.getChannelException(AMQConstant.COMMAND_INVALID, "Cannot commit, branch in an invalid state", e);
        } catch (AndesException e) {
            throw body.getChannelException(AMQConstant.INTERNAL_ERROR, "Cannot commit, error occurred while committing",
                    e);
        } catch (TimeoutDtxException e) {
            LOGGER.info("Error while preparing dtx: " + e.getMessage());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Error while preparing dtx", e);
            }

            MethodRegistry_0_91 methodRegistry = (MethodRegistry_0_91) session.getMethodRegistry();
            DtxCommitOkBody dtxCommitOkBody = methodRegistry.createDtxCommitOkBody(DtxXaStatus.XA_RBTIMEOUT.getValue());
            session.writeFrame(dtxCommitOkBody.generateFrame(channelId));
        } catch (RollbackOnlyDtxException e) {
            LOGGER.info("Error while preparing dtx: " + e.getMessage());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Error while preparing dtx", e);
            }

            MethodRegistry_0_91 methodRegistry = (MethodRegistry_0_91) session.getMethodRegistry();
            DtxCommitOkBody dtxCommitOkBody = methodRegistry.createDtxCommitOkBody(DtxXaStatus.XA_RBROLLBACK.getValue());
            session.writeFrame(dtxCommitOkBody.generateFrame(channelId));
        }
    }
}
