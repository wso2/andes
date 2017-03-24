/* Licensed to the Apache Software Foundation (ASF) under one
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
 */
package org.wso2.andes.client;

import org.wso2.andes.AMQException;
import org.wso2.andes.jms.ConnectionURL;

import java.util.ArrayList;
import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicConnection;
import javax.jms.XATopicSession;

/**
 * This class implements the javax.njms.XAConnection interface
 */
public class XAConnectionImpl extends AMQConnection implements XAConnection, XAQueueConnection, XATopicConnection
{
    /**
     * Keep track of active XA sessions
     */
    private final ArrayList<XASession_9_1> xaSessions = new ArrayList<>();

    /**
     * Indicate if the connection.close is called before
     */
    private boolean connectionCloseSignaled = false;

    //-- constructor
    /**
     * Create a XAConnection from a connectionURL
     */
    XAConnectionImpl(ConnectionURL connectionURL, SSLConfiguration sslConfig) throws AMQException {
        super(connectionURL, sslConfig);
    }

    //-- interface XAConnection
    /**
     * Creates an XASession.
     *
     * @return A newly created XASession.
     * @throws JMSException If the XAConnectiono fails to create an XASession due to
     *                      some internal error.
     */
    public synchronized XASession createXASession() throws JMSException {
        checkNotClosed();
        XASession xaSession = _delegate.createXASession();

        // Need to alter the class hierarchy to avoid casting
        if (xaSession instanceof XASession_9_1) {
            xaSessions.add((XASession_9_1) xaSession);
        }

        return xaSession;
    }

    //-- Interface  XAQueueConnection
    /**
     * Creates an XAQueueSession.
     *
     * @return A newly created XASession.
     * @throws JMSException If the XAQueueConnectionImpl fails to create an XASession due to
     *                      some internal error.
     */
    public XAQueueSession createXAQueueSession() throws JMSException {
        return (XAQueueSession) createXASession();
    }

    //-- Interface  XATopicConnection
    /**
     * Creates an XAQueueSession.
     *
     * @return A newly created XASession.
     * @throws JMSException If the XAQueueConnectionImpl fails to create an XASession due to
     *                      some internal error.
     */
    public XATopicSession createXATopicSession() throws JMSException {
        return (XATopicSession) createXASession();
    }

    @Override
    public synchronized void close() throws JMSException {
        if (connectionCloseSignaled) {
            boolean canClosePhysicalConnection = true;

            for (XASession_9_1 xaSession : xaSessions) {
                boolean isTransactionActive = xaSession.indicateConnectionClose();
                if (isTransactionActive) {
                    canClosePhysicalConnection = false;
                }
            }

            if (canClosePhysicalConnection) {
                super.close();
            }

            connectionCloseSignaled = true;
        }
    }

    /**
     * Releases the session from the connection. Normally this should be called after
     * closing the session.
     *
     * @param xaSession XA session to be released
     */
    synchronized void deregisterSession(XASession_9_1 xaSession) {
        xaSessions.remove(xaSession);
    }

    /**
     * Closes the physical connection if there are no active sessions
     *
     * @throws JMSException if there was an error closing the physical connection
     */
    synchronized void internalClose() throws JMSException {
        if (xaSessions.isEmpty()) {
            super.close();
        }
    }
}
