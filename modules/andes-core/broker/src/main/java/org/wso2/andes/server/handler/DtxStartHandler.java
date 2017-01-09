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
import org.wso2.andes.framing.DtxStartOkBody;
import org.wso2.andes.framing.amqp_0_91.DtxStartBodyImpl;
import org.wso2.andes.framing.amqp_0_91.MethodRegistry_0_91;
import org.wso2.andes.kernel.dtx.AlreadyKnownDtxException;
import org.wso2.andes.kernel.dtx.JoinAndResumeDtxException;
import org.wso2.andes.kernel.dtx.UnknownDtxBranchException;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.txn.DtxNotSelectedException;
import org.wso2.andes.transport.DtxXaStatus;

import javax.transaction.xa.Xid;

public class DtxStartHandler implements StateAwareMethodListener<DtxStartBodyImpl> {
    private static DtxStartHandler _instance = new DtxStartHandler();

    public static DtxStartHandler getInstance() {
        return _instance;
    }

    private DtxStartHandler() {
    }

    @Override
    public void methodReceived(AMQStateManager stateManager, DtxStartBodyImpl body, int channelId) throws AMQException {

        Xid xid = new XidImpl(body.getBranchId(), body.getFormat(), body.getGlobalId());
        AMQProtocolSession session = stateManager.getProtocolSession();

        AMQChannel channel = session.getChannel(channelId);

        if (channel == null) {
            throw body.getChannelNotFoundException(channelId);
        }

        try {
            channel.startDtxTransaction(xid, body.getJoin(), body.getResume());

            MethodRegistry_0_91 methodRegistry = (MethodRegistry_0_91) session.getMethodRegistry();
            DtxStartOkBody dtxStartOkBody = methodRegistry.createDtxStartOkBody(DtxXaStatus.XA_OK.getValue());
            session.writeFrame(dtxStartOkBody.generateFrame(channelId));
        } catch (DtxNotSelectedException e) {
            throw new AMQException(AMQConstant.COMMAND_INVALID, "Error starting dtx ", e);
        } catch (AlreadyKnownDtxException e) {
            throw new AMQException(AMQConstant.COMMAND_INVALID, "Xid already started an neither join nor "
                    + "resume set" + xid, e);
        } catch (JoinAndResumeDtxException e) {
            throw new AMQException(AMQConstant.COMMAND_INVALID, "Cannot start a branch with both join and resume set "
                    , e);
        } catch (UnknownDtxBranchException e) {
            throw body.getChannelException(AMQConstant.NOT_ALLOWED, "Unknown XID " + xid, e);
        }
    }
}
