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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.client.failover.FailoverException;
import org.wso2.andes.transport.XaStatus;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import javax.jms.JMSException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

class XAResource_0_9_1 implements XAResource {
    /**
     * Class logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(XAResource_0_9_1.class);

    /**
     * Reference to the connected XA Session
     */
    private final XASession_9_1 session;

    /**
     * Hold similar XAResources belonging to the same resource manager
     */
    private List<XAResource> siblings = new ArrayList<>();

    /**
     * Indicate if the operating on a joined transaction.
     */
    private boolean joined = false;

    /**
     * The time for this resource
     */
    private int timeout;

    /**
     * The XID of this resource
     */
    private Xid xid;

    /**
     * Indicate if underline {@link XASession_9_1} should be closed after commit or rollback
     */
    private boolean pendingSessionClose = false;

    XAResource_0_9_1(XASession_9_1 xaSession_9_1) {
        session = xaSession_9_1;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("start tx branch with xid: {}", xid);
        }

        XaStatus resultStatus;
        try {
            resultStatus = session.commitDtx(xid, onePhase);
        } catch (FailoverException | AMQException e) {
            XAException xaException = new XAException("Error while committing dtx session.");
            xaException.initCause(e);
            throw xaException;
        } finally {
            this.xid = null;
        }

        closeSessionIfClosable();

        checkStatus(resultStatus);
    }

    /**
     * Close the underline {@link XASession_9_1} if the session close was signaled before commit or rollback
     */
    private void closeSessionIfClosable() {
        if (!session.isClosed() && pendingSessionClose) {
            try {
                session.internalClose();
            } catch (JMSException e) {
                LOGGER.error("Error while closing session after commit or rollback", e);
            }
        }
    }

    /**
     * Ends the work performed on behalf of a transaction branch.
     * The resource manager disassociates the XA resource from the transaction branch specified
     * and lets the transaction complete.
     * <ul>
     * <li> If TMSUSPEND is specified in the flags, the transaction branch is temporarily suspended in an incomplete state.
     * The transaction context is in a suspended state and must be resumed via the start method with TMRESUME specified.
     * <li> If TMFAIL is specified, the portion of work has failed. The resource manager may mark the transaction as rollback-only
     * <li> If TMSUCCESS is specified, the portion of work has completed successfully.
     * </ul>
     *
     * @param xid  a global transaction identifier that is the same as the identifier used previously in the start
     *             method
     * @param flag one of TMSUCCESS, TMFAIL, or TMSUSPEND.
     * @throws XAException an error has occurred. An error has occurred. Possible XAException values are XAER_RMERR,
     *                     XAER_RMFAILED, XAER_NOTA, XAER_INVAL, XAER_PROTO, or XA_RB*.
     */
    @Override
    public void end(Xid xid, int flag) throws XAException {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("end tx branch with xid: ", xid);
        }
        switch (flag)
        {
        case(XAResource.TMSUCCESS):
            break;
        case(XAResource.TMFAIL):
            break;
        case(XAResource.TMSUSPEND):
            break;
        default:
            throw new XAException(XAException.XAER_INVAL);
        }

        XaStatus resultStatus;
        try {
            resultStatus = session.endDtx(xid, flag);
        } catch (FailoverException | AMQException e) {
            XAException xaException = new XAException("Error while ending dtx session.");
            xaException.initCause(e);
            throw xaException;
        }

        checkStatus(resultStatus);

        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Calling end for " + siblings.size() + " XAResource siblings");
        }

        if (!joined) {
            for(XAResource sibling: siblings) {
                sibling.end(xid, flag);
            }
        }

        joined = false;
        siblings.clear();
    }

    /**
     * Tells the resource manager to forget about a heuristically completed transaction branch.
     *
     * @param xid transaction identifier
     * @throws XAException An error has occurred. Possible exception values are XAER_RMERR, XAER_RMFAIL,
     *                     XAER_NOTA, XAER_INVAL, or XAER_PROTO.
     */
    @Override
    public void forget(Xid xid) throws XAException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("forget tx branch with xid: ", xid);
        }

        XaStatus resultStatus;
        try {
            resultStatus = session.forget(xid);
        } catch (FailoverException | AMQException e) {
            XAException xaException = new XAException("Error while forgetting dtx session.");
            xaException.initCause(e);
            throw xaException;
        } finally {
            this.xid = null;
        }

        checkStatus(resultStatus);
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        throw new RuntimeException("Feature NotImplemented");
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        if (this == xaResource) {
            return true;
        }

        if (!(xaResource instanceof XAResource_0_9_1)) {
            return false;
        }

        SocketAddress myRemoteAddress = session.getAMQConnection().getProtocolHandler().getRemoteAddress();
        SocketAddress otherRemoteAddress = ((XAResource_0_9_1) xaResource).session.getAMQConnection()
                                                                                  .getProtocolHandler()
                                                                                  .getRemoteAddress();

        boolean isSameRm = (myRemoteAddress != null && otherRemoteAddress != null
                && myRemoteAddress.equals(otherRemoteAddress));

        if(isSameRm)
        {
            if(LOGGER.isDebugEnabled())
            {
                LOGGER.debug("XAResource " + xaResource + " is from the same ResourceManager. Adding XAResource as "
                        + "sibling for AMQP protocol support. ");
            }

            // Adding to both since we don't know which will join which
            siblings.add(xaResource);
            ((XAResource_0_9_1) xaResource).siblings.add(this);
        }

        return isSameRm;
    }

    /**
     * Prepare for a transaction commit of the transaction specified in <code>Xid</code>.
     *
     * @param xid A global transaction identifier.
     * @return A value indicating the resource manager's vote on the outcome of the transaction.
     * The possible values are: XA_RDONLY or XA_OK.
     * @throws XAException An error has occurred. Possible exception values are: XAER_RMERR or XAER_NOTA
     */
    @Override
    public int prepare(Xid xid) throws XAException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("prepare dtx branch with xid: " + xid);
        }

        XaStatus resultStatus;
        try {
            resultStatus = session.prepareDtx(xid);
        } catch (FailoverException | AMQException e) {
            XAException xaException = new XAException("Error while preparing dtx session.");
            xaException.initCause(e);
            throw xaException;
        }

        if (resultStatus == XaStatus.XA_RDONLY) {
            return XA_RDONLY;
        }

        checkStatus(resultStatus);

        return XAResource.XA_OK;
    }

    @Override
    public Xid[] recover(int i) throws XAException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("recover dtx branches with prepared state");
        }

        try {
            List<Xid> xidList = session.recoverDtxTransactions();
            return xidList.toArray(new Xid[xidList.size()]);

        } catch (FailoverException | AMQException e) {
            XAException xaException = new XAException("Error while recovering dtx sessions.");
            xaException.initCause(e);
            throw xaException;
        }
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("start dtx branch with xid: ", xid);
        }

        XaStatus resultStatus;
        try {
            resultStatus = session.rollbackDtx(xid);
        } catch (FailoverException | AMQException e) {
            XAException xaException = new XAException("Error while rolling back dtx session.");
            xaException.initCause(e);
            throw xaException;
        } finally {
            this.xid = null;
        }

        closeSessionIfClosable();

        checkStatus(resultStatus);
    }

    @Override
    public boolean setTransactionTimeout(int timeout) throws XAException {
        this.timeout = timeout;

        if (isTransactionActive()) {
            setDtxTimeoutInServer(timeout);
        }

        return true;
    }

    /**
     * Indicate if transaction is active
     *
     * @return True if transaction is active, false otherwise
     */
    boolean isTransactionActive() {
        return xid != null;
    }

    /**
     * Set the transaction delay for current branch in the server
     *
     * @param timeout transaction timeout value
     * @throws XAException if server responded with an error
     */
    private void setDtxTimeoutInServer(int timeout) throws XAException {
        XaStatus resultStatus;
        try {
            resultStatus = session.setDtxTimeout(xid, timeout);
        } catch (FailoverException | AMQException e) {
            XAException xaException = new XAException("Error while setting transaction timeout back dtx session.");
            xaException.initCause(e);
            throw xaException;
        }

        checkStatus(resultStatus);
    }

    /**
     * Starts work on behalf of a transaction branch specified in xid.
     * <ul>
     * <li> If TMJOIN is specified, an exception is thrown as it is not supported
     * <li> If TMRESUME is specified, the start applies to resuming a suspended transaction specified in the parameter xid.
     * <li> If neither TMJOIN nor TMRESUME is specified and the transaction specified by xid has previously been seen by the
     * resource manager, the resource manager throws the XAException exception with XAER_DUPID error code.
     * </ul>
     *
     * @param xid  a global transaction identifier to be associated with the resource
     * @param flag one of TMNOFLAGS, TMJOIN, or TMRESUME
     * @throws XAException An error has occurred. Possible exceptions
     *                     are XA_RB*, XAER_RMERR, XAER_RMFAIL, XAER_DUPID, XAER_OUTSIDE, XAER_NOTA, XAER_INVAL, or XAER_PROTO.
     */
    @Override
    public void start(Xid xid, int flag) throws XAException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("start tx branch with xid: ", xid);
        }

        switch (flag) {
        case (XAResource.TMNOFLAGS):
        case (XAResource.TMJOIN):
        case (XAResource.TMRESUME):
            break;
        default:
            throw new XAException(XAException.XAER_INVAL);
        }

        XaStatus resultStatus;
        try {
            resultStatus = session.startDtx(xid, flag);
        } catch (FailoverException | AMQException e) {
            XAException xaException = new XAException("Error while starting dtx session.");
            xaException.initCause(e);
            throw xaException;
        }

        checkStatus(resultStatus);

        this.xid = xid;

        if (timeout > 0) {
            setTransactionTimeout(timeout);
        }

        if (flag == XAResource.TMJOIN) {
            joined = true;
        }
    }

    private void checkStatus(XaStatus status) throws XAException {
        switch (status)
        {
        case XA_OK:
            // Do nothing this ok
            break;
        case XA_RBROLLBACK:
            // The tx has been rolled back for an unspecified reason.
            throw new XAException(XAException.XA_RBROLLBACK);
        case XA_RBTIMEOUT:
            // The transaction branch took too long.
            throw new XAException(XAException.XA_RBTIMEOUT);
        case XA_HEURHAZ:
            // The transaction branch may have been heuristically completed.
            throw new XAException(XAException.XA_HEURHAZ);
        case XA_HEURCOM:
            // The transaction branch has been heuristically committed.
            throw new XAException(XAException.XA_HEURCOM);
        case XA_HEURRB:
            // The transaction branch has been heuristically rolled back.
            throw new XAException(XAException.XA_HEURRB);
        case XA_HEURMIX:
            // The transaction branch has been heuristically committed and rolled back.
            throw new XAException(XAException.XA_HEURMIX);
        case XA_RDONLY:
            // The transaction branch was read-only and has been committed.
            throw new XAException(XAException.XA_RDONLY);
        default:
            // this should not happen
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("got unexpected status value: {}", status);
            }
            //A resource manager error has occured in the transaction branch.
            throw new XAException(XAException.XAER_RMERR);
        }
    }

    /**
     * Signal session closure to the XAResource. Session is closed if the XAResource is already done (committed or
     * rolled back). Otherwise the session will be closed after a commit or a rollback is received
     *
     * @throws JMSException if closing session failed
     */
    boolean indicateSessionClosure() throws JMSException {
        pendingSessionClose = true;
        return isTransactionActive();
    }
}
