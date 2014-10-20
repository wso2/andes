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

import java.io.IOException;

import javax.jms.JMSException;
import javax.jms.XASession;

import org.wso2.andes.AMQException;
import org.wso2.andes.client.failover.FailoverException;
import org.wso2.andes.client.failover.FailoverProtectedOperation;
import org.wso2.andes.framing.ProtocolVersion;
import org.wso2.andes.jms.BrokerDetails;
import org.wso2.andes.jms.Session;

public interface AMQConnectionDelegate
{
    ProtocolVersion makeBrokerConnection(BrokerDetails brokerDetail) throws IOException, AMQException;

    Session createSession(final boolean transacted, final int acknowledgeMode,
     final int prefetchHigh, final int prefetchLow) throws JMSException;

    /**
     * Create an XASession with default prefetch values of:
     * High = MaxPrefetch
     * Low  = MaxPrefetch / 2
     * @return XASession
     * @throws JMSException thrown if there is a problem creating the session.
     */
    XASession createXASession() throws JMSException;

    XASession createXASession(int prefetchHigh, int prefetchLow) throws JMSException;

    void failoverPrep();

    void resubscribeSessions() throws JMSException, AMQException, FailoverException;

    void closeConnection(long timeout) throws JMSException, AMQException;

    <T, E extends Exception> T executeRetrySupport(FailoverProtectedOperation<T,E> operation) throws E;

    int getMaxChannelID();

    int getMinChannelID();

    ProtocolVersion getProtocolVersion();

    void verifyClientID() throws JMSException;
}
