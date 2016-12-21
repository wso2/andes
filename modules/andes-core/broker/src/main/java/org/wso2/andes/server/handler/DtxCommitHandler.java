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

import org.wso2.andes.AMQException;
import org.wso2.andes.dtx.XidImpl;
import org.wso2.andes.framing.DtxCommitOkBody;
import org.wso2.andes.framing.amqp_0_91.DtxCommitBodyImpl;
import org.wso2.andes.framing.amqp_0_91.MethodRegistry_0_91;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.dtx.UnknownDtxBranchException;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.txn.DtxNotSelectedException;
import org.wso2.andes.server.txn.IncorrectDtxStateException;
import org.wso2.andes.transport.DtxXaStatus;

import javax.transaction.xa.Xid;

public class DtxCommitHandler implements StateAwareMethodListener<DtxCommitBodyImpl> {
    private static DtxCommitHandler _instance = new DtxCommitHandler();

    public static DtxCommitHandler getInstance() {
        return _instance;
    }

    private DtxCommitHandler() {
    }

    @Override
    public void methodReceived(AMQStateManager stateManager, DtxCommitBodyImpl body, final int channelId)
            throws AMQException {
        Xid xid = new XidImpl(body.getBranchId(), body.getFormat(), body.getGlobalId());
        final AMQProtocolSession session = stateManager.getProtocolSession();

        final AMQChannel channel = session.getChannel(channelId);

        if (channel == null) {
            throw body.getChannelNotFoundException(channelId);
        }

        try {
            channel.commitDtxTransaction(xid, new Runnable() {

                @Override
                public void run() {
                    MethodRegistry_0_91 methodRegistry = (MethodRegistry_0_91) session.getMethodRegistry();
                    DtxCommitOkBody dtxCommitOkBody = methodRegistry.createDtxCommitOkBody(DtxXaStatus.XA_OK.getValue());
                    session.writeFrame(dtxCommitOkBody.generateFrame(channelId));
                }
            });


        } catch (DtxNotSelectedException  e) {
            throw new AMQException(AMQConstant.COMMAND_INVALID, "Dtx not selected ", e);
        } catch (UnknownDtxBranchException e) {
            throw new AMQException(AMQConstant.COMMAND_INVALID, "Commit failed. Unknown XID ", e);
        } catch (IncorrectDtxStateException e) {
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Cannot commit, branch in an invalid state", e);
        } catch (AndesException e) {
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Cannot commit, error occurred while committing", e);
        }
    }
}
