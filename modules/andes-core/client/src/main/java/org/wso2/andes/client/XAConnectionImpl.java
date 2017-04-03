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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.jms.ConnectionURL;

import java.util.ArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.jms.IllegalStateException;
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
     * Class logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(XAConnectionImpl.class);

    /**
     * After this delay(in seconds), the connection will be closed even if a commit or rollback is not received. This
     * is done to avoid stale connections to broker.
     */
    private int connectionCloseTimeout = 60;

    /**
     * Keep track of active XA sessions
     */
    private final ArrayList<XASession_9_1> xaSessions = new ArrayList<>();

    /**
     * Executor service used to schedule connection close task.
     */
    private final ScheduledExecutorService scheduledExecutor;

    /**
     * Indicate if the connection.close is called before
     */
    private boolean connectionCloseSignaled = false;

    /**
     * Hold the future object of the scheduled close task
     */
    private ScheduledFuture<?> connectionCloseFuture;

    //-- constructor
    /**
     * Create a XAConnection from a connectionURL
     */
    XAConnectionImpl(ConnectionURL connectionURL, SSLConfiguration sslConfig,
            ScheduledExecutorService scheduledExecutor) throws AMQException {
        super(connectionURL, sslConfig);
        this.connectionCloseTimeout = Integer.parseInt(System.getProperty("XaConnectionCloseWaitTimeOut", "60"));
        this.scheduledExecutor = scheduledExecutor;
    }

    //-- interface XAConnection
    /**
     * Creates an XASession.
     *
     * @return A newly created XASession.
     * @throws JMSException If the XAConnection fails to create an XASession due to
     *                      some internal error.
     */
    public synchronized XASession createXASession() throws JMSException {
        checkNotClosed();
        if (connectionCloseSignaled) {
            throw new IllegalStateException("Object " + toString() + " has been closed");
        }

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
        if (!connectionCloseSignaled) {
            boolean canClosePhysicalConnection = true;

            for (XASession_9_1 xaSession : xaSessions) {
                boolean isTransactionActive = xaSession.indicateConnectionClose();
                if (isTransactionActive) {
                    canClosePhysicalConnection = false;
                }
            }

            if (canClosePhysicalConnection) {
                super.close();
            } else {
                LOGGER.error("XAConnection.close() was called before committing or rolling back");
                connectionCloseFuture = scheduledExecutor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            LOGGER.error("Closing XAConnection after waiting " + connectionCloseTimeout
                                                 + " seconds for a commit or rollback");
                            closePhysicalConnection();
                        } catch (JMSException e) {
                            LOGGER.error("Error occurred while closing the XAConnection after close timeout");
                        }
                    }
                }, connectionCloseTimeout, TimeUnit.SECONDS);
            }

            connectionCloseSignaled = true;
        }
    }

    /**
     * Close the underline socket connection
     *
     * @throws JMSException if failed to close the socket connection
     */
    private void closePhysicalConnection() throws JMSException {
        super.close();
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
    void internalClose() throws JMSException {
        boolean closeSuccessful = closeIfNoActiveSessions();
        if (closeSuccessful) {
            removeScheduledClose();
        }
    }

    /**
     * Closes the physical connection if there are no active sessions
     *
     * @return true if physical connection is closed, false otherwise
     * @throws JMSException if there was an closing the connection
     */
    private synchronized boolean closeIfNoActiveSessions() throws JMSException {
        if (xaSessions.isEmpty()) {
            closePhysicalConnection();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Remove the scheduled connection close task
     */
    private synchronized void removeScheduledClose() {
        if (connectionCloseFuture != null) {
            connectionCloseFuture.cancel(true);
            connectionCloseFuture = null;
        }
    }
}
