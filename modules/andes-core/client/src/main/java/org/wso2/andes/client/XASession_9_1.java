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

package org.wso2.andes.client;

import org.wso2.andes.AMQException;
import org.wso2.andes.client.failover.FailoverException;
import org.wso2.andes.framing.BasicQosBody;
import org.wso2.andes.framing.BasicQosOkBody;
import org.wso2.andes.framing.ChannelOpenBody;
import org.wso2.andes.framing.ChannelOpenOkBody;
import org.wso2.andes.framing.DtxSelectBody;
import org.wso2.andes.jms.Session;

import javax.jms.JMSException;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;

class XASession_9_1 extends AMQSession_0_8 implements XASession {

    XASession_9_1(AMQConnection con, int channelId, int defaultPrefetchHigh, int defaultPrefetchLow)
            throws FailoverException, AMQException {
        super(con, channelId, false, Session.AUTO_ACKNOWLEDGE, defaultPrefetchHigh, defaultPrefetchLow);

        con.registerSession(channelId, this);

        try {
            createChannelOverWire(con, channelId, defaultPrefetchHigh);
        } catch (FailoverException | AMQException e) {
            con.deregisterSession(channelId);
            throw e;
        }
        new XAResource_0_9_1(this);

    }

    private void createChannelOverWire(AMQConnection connection, int channelId, int prefetchHigh)
            throws FailoverException, AMQException {
        ChannelOpenBody channelOpenBody = connection.getProtocolHandler().getMethodRegistry()
                                                    .createChannelOpenBody(null);
        connection._protocolHandler.syncWrite(channelOpenBody.generateFrame(channelId), ChannelOpenOkBody.class);

        // todo send low water mark when protocol allows.
        BasicQosBody basicQosBody = connection.getProtocolHandler().getMethodRegistry()
                                              .createBasicQosBody(0, prefetchHigh, false);
        connection._protocolHandler.syncWrite(basicQosBody.generateFrame(channelId), BasicQosOkBody.class);

        DtxSelectBody body = connection.getProtocolHandler().getMethodRegistry().createDtxSelectBody();
        connection._protocolHandler.writeFrame(body.generateFrame(channelId));
    }

    @Override
    public javax.jms.Session getSession() throws JMSException {
        return this;
    }

    @Override
    public XAResource getXAResource() {
        return null;
    }
}
