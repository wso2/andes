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
import org.wso2.andes.framing.DtxEndOkBody;
import org.wso2.andes.framing.amqp_0_91.DtxEndBodyImpl;
import org.wso2.andes.framing.amqp_0_91.MethodRegistry_0_91;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.txn.DtxNotSelectedException;
import org.wso2.andes.transport.DtxXaStatus;

import javax.transaction.xa.Xid;

public class DtxEndHandler implements StateAwareMethodListener<DtxEndBodyImpl> {
    private static DtxEndHandler _instance = new DtxEndHandler();

    public static DtxEndHandler getInstance() {
        return _instance;
    }

    private DtxEndHandler() {
    }

    @Override
    public void methodReceived(AMQStateManager stateManager, DtxEndBodyImpl body, int channelId) throws AMQException {
        Xid xid = new XidImpl(body.getBranchId(), body.getFormat(), body.getGlobalId());
        AMQProtocolSession session = stateManager.getProtocolSession();

        AMQChannel channel = session.getChannel(channelId);

        if (channel == null) {
            throw body.getChannelNotFoundException(channelId);
        }

        try {
            channel.endDtxTransaction(xid, body.getFail(), body.getSuspend());

            MethodRegistry_0_91 methodRegistry = (MethodRegistry_0_91) session.getMethodRegistry();
            DtxEndOkBody dtxEndOkBody = methodRegistry.createDtxEndOkBody(DtxXaStatus.XA_OK.getValue());
            session.writeFrame(dtxEndOkBody.generateFrame(channelId));
        } catch (DtxNotSelectedException e) {
            throw new AMQException(AMQConstant.COMMAND_INVALID, "Error ending dtx ", e);
        }
    }
}