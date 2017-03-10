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

import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.XaJmsSession;
import org.wso2.andes.client.failover.FailoverException;
import org.wso2.andes.framing.BasicQosBody;
import org.wso2.andes.framing.BasicQosOkBody;
import org.wso2.andes.framing.ChannelOpenBody;
import org.wso2.andes.framing.ChannelOpenOkBody;
import org.wso2.andes.framing.DtxCommitBody;
import org.wso2.andes.framing.DtxCommitOkBody;
import org.wso2.andes.framing.DtxEndBody;
import org.wso2.andes.framing.DtxEndOkBody;
import org.wso2.andes.framing.DtxForgetBody;
import org.wso2.andes.framing.DtxForgetOkBody;
import org.wso2.andes.framing.DtxPrepareBody;
import org.wso2.andes.framing.DtxPrepareOkBody;
import org.wso2.andes.framing.DtxRecoverBody;
import org.wso2.andes.framing.DtxRecoverOkBody;
import org.wso2.andes.framing.DtxRollbackBody;
import org.wso2.andes.framing.DtxRollbackOkBody;
import org.wso2.andes.framing.DtxSelectBody;
import org.wso2.andes.framing.DtxSetTimeoutBody;
import org.wso2.andes.framing.DtxSetTimeoutOkBody;
import org.wso2.andes.framing.DtxStartBody;
import org.wso2.andes.framing.DtxStartOkBody;
import org.wso2.andes.framing.amqp_0_91.DtxCommitOkBodyImpl;
import org.wso2.andes.framing.amqp_0_91.DtxEndOkBodyImpl;
import org.wso2.andes.framing.amqp_0_91.DtxForgetOkBodyImpl;
import org.wso2.andes.framing.amqp_0_91.DtxPrepareOkBodyImpl;
import org.wso2.andes.framing.amqp_0_91.DtxRecoverOkBodyImpl;
import org.wso2.andes.framing.amqp_0_91.DtxRollbackOkBodyImpl;
import org.wso2.andes.framing.amqp_0_91.DtxSetTimeoutOkBodyImpl;
import org.wso2.andes.framing.amqp_0_91.DtxStartOkBodyImpl;
import org.wso2.andes.framing.amqp_0_91.MethodRegistry_0_91;
import org.wso2.andes.jms.Session;
import org.wso2.andes.protocol.AMQMethodEvent;
import org.wso2.andes.transport.XaStatus;

import java.util.ArrayList;
import java.util.List;
import javax.jms.JMSException;
import javax.jms.QueueSession;
import javax.jms.TopicSession;
import javax.jms.TransactionInProgressException;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicSession;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class XASession_9_1 extends AMQSession_0_8 implements XASession, XAQueueSession, XATopicSession {

    /**
     * Class logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(XASession_9_1.class);

    /**
     * Method registry used create AMQ Method frames
     */
    private final MethodRegistry_0_91 methodRegistry;

    /**
     * XA Resource object belonging to the current XA session
     */
    private final XAResource_0_9_1 xaResource;

    XASession_9_1(AMQConnection con, int channelId, int defaultPrefetchHigh, int defaultPrefetchLow)
            throws FailoverException, AMQException {
        super(con, channelId, false, Session.AUTO_ACKNOWLEDGE, defaultPrefetchHigh, defaultPrefetchLow);

        methodRegistry = (MethodRegistry_0_91) con.getProtocolHandler().getMethodRegistry();
        con.registerSession(channelId, this);

        try {
            createChannelOverWire(con, channelId, defaultPrefetchHigh);
        } catch (FailoverException | AMQException e) {
            con.deregisterSession(channelId);
            throw e;
        }
        xaResource = new XAResource_0_9_1(this);

    }

    private void createChannelOverWire(AMQConnection connection, int channelId, int prefetchHigh)
            throws FailoverException, AMQException {
        ChannelOpenBody channelOpenBody = methodRegistry.createChannelOpenBody(null);
        connection._protocolHandler.syncWrite(channelOpenBody.generateFrame(channelId), ChannelOpenOkBody.class);

        // todo send low water mark when protocol allows.
        BasicQosBody basicQosBody = methodRegistry.createBasicQosBody(0, prefetchHigh, false);
        connection._protocolHandler.syncWrite(basicQosBody.generateFrame(channelId), BasicQosOkBody.class);

        DtxSelectBody body = methodRegistry.createDtxSelectBody();
        connection._protocolHandler.writeFrame(body.generateFrame(channelId));
    }

    @Override
    public javax.jms.Session getSession() throws JMSException {
        return new XaJmsSession(this);
    }

    @Override
    public XAResource getXAResource() {
        return xaResource;
    }

    /**
     * Send startDtx command to server
     *
     * @param xid  q global transaction identifier to be associated with the resource
     * @param flag one of TMNOFLAGS, TMJOIN, or TMRESUME
     * @return XaStatus returned by server
     * @throws FailoverException when a connection issue is detected
     * @throws AMQException      when an error is detected in AMQ state manager
     */
    XaStatus startDtx(Xid xid, int flag) throws FailoverException, AMQException, XAException {

        throwErrorIfClosed();

        DtxStartBody dtxStartBody = methodRegistry
                .createDtxStartBody(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier(),
                        flag == XAResource.TMJOIN, flag == XAResource.TMRESUME);

        AMQMethodEvent amqMethodEvent = _connection._protocolHandler
                .syncWrite(dtxStartBody.generateFrame(_channelId), DtxStartOkBody.class);

        DtxStartOkBodyImpl response = (DtxStartOkBodyImpl) amqMethodEvent.getMethod();

        return XaStatus.valueOf(response.getXaResult());
    }

    /**
     * Send endDtx command to server
     *
     * @param xid  a global transaction identifier to be associated with the resource
     * @param flag one of TMSUCCESS, TMFAIL, or TMSUSPEND.
     * @return XaStatus returned by server
     * @throws FailoverException when a connection issue is detected
     * @throws AMQException      when an error is detected in AMQ state manager
     */
    public XaStatus endDtx(Xid xid, int flag) throws FailoverException, AMQException, XAException {

        throwErrorIfClosed();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Sending dtx.end for channel " + _channelId + ", xid " + xid);
        }

        DtxEndBody dtxEndBody = methodRegistry
                .createDtxEndBody(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier(),
                        flag == XAResource.TMFAIL, flag == XAResource.TMSUSPEND);

        AMQMethodEvent amqMethodEvent = _connection._protocolHandler
                .syncWrite(dtxEndBody.generateFrame(_channelId), DtxEndOkBody.class);

        DtxEndOkBodyImpl response = (DtxEndOkBodyImpl) amqMethodEvent.getMethod();

        return XaStatus.valueOf(response.getXaResult());
    }

    public XaStatus prepareDtx(Xid xid) throws FailoverException, AMQException, XAException {

        throwErrorIfClosed();

        DtxPrepareBody dtxPrepareBody = methodRegistry
                .createDtxPrepareBody(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());

        AMQMethodEvent amqMethodEvent = _connection._protocolHandler
                .syncWrite(dtxPrepareBody.generateFrame(_channelId), DtxPrepareOkBody.class);

        DtxPrepareOkBodyImpl response = (DtxPrepareOkBodyImpl) amqMethodEvent.getMethod();

        return XaStatus.valueOf(response.getXaResult());
    }

    public XaStatus commitDtx(Xid xid, boolean onePhase) throws FailoverException, AMQException, XAException {

        throwErrorIfClosed();

        DtxCommitBody dtxCommitBody = methodRegistry
                .createDtxCommitBody(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier(), onePhase);

        AMQMethodEvent amqMethodEvent = _connection._protocolHandler
                .syncWrite(dtxCommitBody.generateFrame(_channelId), DtxCommitOkBody.class);

        DtxCommitOkBodyImpl response = (DtxCommitOkBodyImpl) amqMethodEvent.getMethod();

        return XaStatus.valueOf(response.getXaResult());
    }

    public XaStatus rollbackDtx(Xid xid) throws FailoverException, AMQException, XAException {

        throwErrorIfClosed();

        DtxRollbackBody dtxRollbackBody = methodRegistry
                .createDtxRollbackBody(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());

        AMQMethodEvent amqMethodEvent = _connection._protocolHandler
                .syncWrite(dtxRollbackBody.generateFrame(_channelId), DtxRollbackOkBody.class);

        DtxRollbackOkBodyImpl response = (DtxRollbackOkBodyImpl) amqMethodEvent.getMethod();

        return XaStatus.valueOf(response.getXaResult());
    }

    /**
     * Sends a dtx.forget frame to broker node and wait for dtx.forget-ok response
     *
     * @param xid distributed transaction ID
     * @return response status
     * @throws FailoverException if failover process started during communication with server
     * @throws AMQException      if server sends back a error response
     */
    public XaStatus forget(Xid xid) throws FailoverException, AMQException, XAException {

        throwErrorIfClosed();

        DtxForgetBody dtxForgetBody = methodRegistry
                .createDtxForgetBody(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());

        AMQMethodEvent amqMethodEvent = _connection._protocolHandler
                .syncWrite(dtxForgetBody.generateFrame(_channelId), DtxForgetOkBody.class);

        DtxForgetOkBodyImpl response = (DtxForgetOkBodyImpl) amqMethodEvent.getMethod();

        return XaStatus.valueOf(response.getXaResult());
    }

    /**
     * Sends a dtx.set-timeout frame to broker node and wait for dtx.set-timeout-ok response
     *
     * @param xid     distribute transction ID
     * @param timeout transaction timeout value to set
     * @return response status
     * @throws FailoverException if failover process started during communication with server
     * @throws AMQException      if server sends back a error response
     */
    public XaStatus setDtxTimeout(Xid xid, int timeout) throws FailoverException, AMQException, XAException {

        throwErrorIfClosed();

        DtxSetTimeoutBody dtxSetTimeoutBody = methodRegistry
                .createDtxSetTimeoutBody(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier(),
                                         timeout);

        AMQMethodEvent amqMethodEvent = _connection._protocolHandler
                .syncWrite(dtxSetTimeoutBody.generateFrame(_channelId), DtxSetTimeoutOkBody.class);

        DtxSetTimeoutOkBodyImpl response = (DtxSetTimeoutOkBodyImpl) amqMethodEvent.getMethod();

        return XaStatus.valueOf(response.getXaResult());
    }

    /**
     * Sends a dtx.recover frame to broker node and wait for dtx.recover-ok response
     *
     * @return list of XIDs in prepared state
     * @throws FailoverException if failover process started during communication with server
     * @throws AMQException      if server sends back a error response
     */
    public List<Xid> recoverDtxTransactions() throws FailoverException, AMQException, XAException {

        throwErrorIfClosed();

        DtxRecoverBody dtxRecoverBody = methodRegistry.createDtxRecoverBody();

        AMQMethodEvent amqMethodEvent = _connection._protocolHandler
                .syncWrite(dtxRecoverBody.generateFrame(_channelId), DtxRecoverOkBody.class);

        DtxRecoverOkBodyImpl response = (DtxRecoverOkBodyImpl) amqMethodEvent.getMethod();

        byte[] inDoubtRawData = response.getInDoubt();

        // No way to get around this warning. Therefore suppressing
        @SuppressWarnings("unchecked")
        ArrayList<Xid> xidList = (ArrayList<Xid>) SerializationUtils.deserialize(inDoubtRawData);

        return xidList;
    }

    /**
     * Get the queue session associated with this <CODE>XASession</CODE>.
     *
     * @return the queue session object
     * @throws JMSException If an internal error occurs.
     */
    @Override
    public QueueSession getQueueSession() throws JMSException {
        return (QueueSession) getSession();
    }

    /**
     * Gets the topic session associated with this <CODE>XASession</CODE>.
     *
     * @return the topic session object
     * @throws JMSException If an internal error occurs.
     */
    @Override
    public TopicSession getTopicSession() throws JMSException {
        return (TopicSession) getSession();
    }

    /**
     * Throw {@link XAException} if the connection is closed
     *
     * @throws XAException if connection not active
     */
    private void throwErrorIfClosed() throws XAException {
        if (isClosed()) {
            XAException xaException = new XAException("Session is already closed");
            xaException.errorCode = XAException.XA_RBCOMMFAIL;
            throw xaException;
        }
    }

    /**
     * Throws a {@link TransactionInProgressException}, since it should
     * not be called for an XASession object.
     *
     * @throws TransactionInProgressException always.
     */
    public void commit() throws JMSException
    {
        throw new TransactionInProgressException(
                "XASession:  A direct invocation of the commit operation is prohibited!");
    }

    /**
     * Throws a {@link TransactionInProgressException}, since it should
     * not be called for an XASession object.
     *
     * @throws TransactionInProgressException always.
     */
    public void rollback() throws JMSException
    {
        throw new TransactionInProgressException(
                "XASession: A direct invocation of the rollback operation is prohibited!");
    }

    /**
     * Throws a {@link TransactionInProgressException}, since it should
     * not be called for an XASession object.
     *
     * @throws TransactionInProgressException always.
     */
    public void recover() throws JMSException
    {
        throw new TransactionInProgressException(
                "XASession: A direct invocation of the recover operation is prohibited!");
    }

}
