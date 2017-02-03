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

import org.apache.commons.lang.SerializationUtils;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.DtxRecoverOkBody;
import org.wso2.andes.framing.amqp_0_91.DtxRecoverBodyImpl;
import org.wso2.andes.framing.amqp_0_91.MethodRegistry_0_91;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.txn.DtxNotSelectedException;

import java.util.ArrayList;
import javax.transaction.xa.Xid;

/**
 * Handler class for dtx.recover
 */
public class DtxRecoverHandler implements StateAwareMethodListener<DtxRecoverBodyImpl> {
    private static DtxRecoverHandler instance = new DtxRecoverHandler();

    public static DtxRecoverHandler getInstance() {
        return instance;
    }

    private DtxRecoverHandler() {
    }

    @Override
    public void methodReceived(AMQStateManager stateManager, DtxRecoverBodyImpl body, int channelId)
            throws AMQException {
        AMQProtocolSession session = stateManager.getProtocolSession();

        AMQChannel channel = session.getChannel(channelId);

        if (channel == null) {
            throw body.getChannelNotFoundException(channelId);
        }

        try {
            ArrayList<Xid> inDoubtXids = channel.recoverDtxTransactions();
            MethodRegistry_0_91 methodRegistry = (MethodRegistry_0_91) session.getMethodRegistry();

            byte[] serializedData = SerializationUtils.serialize(inDoubtXids);
            DtxRecoverOkBody dtxRecoverOkBody = methodRegistry.createDtxRecoverOkBody(serializedData);
            session.writeFrame(dtxRecoverOkBody.generateFrame(channelId));
        } catch (DtxNotSelectedException e) {
            throw body.getChannelException(AMQConstant.NOT_ALLOWED, "Not a distributed transacted session", e);
        }
    }
}
