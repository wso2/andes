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
package org.wso2.andes.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.client.failover.FailoverException;
import org.wso2.andes.framing.ProtocolVersion;
import org.wso2.andes.jms.ChannelLimitReachedException;

import javax.jms.JMSException;
import javax.jms.XASession;

public class AMQConnectionDelegate_9_1 extends AMQConnectionDelegate_8_0
{

    /**
     * Class logger
     */
    private static final Logger _logger = LoggerFactory.getLogger(AMQConnectionDelegate_9_1.class);

    /**
     * Object used to query information related to connection
     */
    private AMQConnection conn;

    public AMQConnectionDelegate_9_1(AMQConnection conn)
    {
        super(conn);
        this.conn = conn;
    }

    @Override
    public ProtocolVersion getProtocolVersion()
    {
        return ProtocolVersion.v0_91;
    }

    /**
     * create an XA Session and start it if required.
     */
    public XASession createXASession(int prefetchHigh, int prefetchLow) throws JMSException
    {
        conn.checkNotClosed();

        if (conn.channelLimitReached()) {
            throw new ChannelLimitReachedException(conn.getMaximumChannelCount());
        }

        int channelId = conn.getNextChannelID();

        if (_logger.isDebugEnabled()) {
            _logger.debug("Write channel open frame for channel id " + channelId);
        }

        XASession_9_1 session;
        try {
            session = new XASession_9_1(conn, channelId, prefetchHigh, prefetchLow);
        } catch (FailoverException | AMQException e) {
            JMSException jmse = new JMSException("Error creating session: " + e);
            jmse.setLinkedException(e);
            jmse.initCause(e);
            throw jmse;
        }

        if (conn._started) {
            try {
                session.start();
            } catch (AMQException e) {
                throw new JMSAMQException("Failed start the XASession", e);
            }
        }

        return session;
    }
}