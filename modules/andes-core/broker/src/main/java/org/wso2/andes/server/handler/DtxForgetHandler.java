/*
 * Copyright (c) 2017, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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
import org.wso2.andes.framing.DtxForgetOkBody;
import org.wso2.andes.framing.amqp_0_91.DtxForgetBodyImpl;
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

public class DtxForgetHandler implements StateAwareMethodListener<DtxForgetBodyImpl> {
    private static DtxForgetHandler _instance = new DtxForgetHandler();

    public static DtxForgetHandler getInstance() {
        return _instance;
    }

    private DtxForgetHandler() {
    }

    @Override
    public void methodReceived(AMQStateManager stateManager, DtxForgetBodyImpl body, int channelId) throws
            AMQException {
        Xid xid = new XidImpl(body.getBranchId(), body.getFormat(), body.getGlobalId());

        AMQProtocolSession session = stateManager.getProtocolSession();
        AMQChannel channel = session.getChannel(channelId);

        if (channel == null) {
            throw body.getChannelNotFoundException(channelId);
        }

        try {
            channel.forgetDtxTransaction(xid);

            MethodRegistry_0_91 methodRegistry = (MethodRegistry_0_91) session.getMethodRegistry();
            DtxForgetOkBody dtxForgetOkBody = methodRegistry.createDtxForgetOkBody(DtxXaStatus.XA_OK.getValue());
            session.writeFrame(dtxForgetOkBody.generateFrame(channelId));
        } catch (DtxNotSelectedException e) {
            throw body.getChannelException(AMQConstant.NOT_ALLOWED, "Not a distributed transacted session", e);
        } catch (UnknownDtxBranchException e) {
            throw body.getChannelException(AMQConstant.NOT_ALLOWED, "Error forgetting dtx. Unknown branch for the "
                    + "given xid ", e);
        } catch (IncorrectDtxStateException e) {
            throw body.getChannelException(AMQConstant.COMMAND_INVALID, e.getMessage(), e);
        } catch (AndesException e) {
            throw body.getChannelException(AMQConstant.INTERNAL_ERROR, "Error forgetting dtx. Internal Error occurred "
                    , e);
        }
    }
}
