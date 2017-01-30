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
import org.wso2.andes.AMQConnectionFailureException;
import org.wso2.andes.AMQDisconnectedException;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQProtocolException;
import org.wso2.andes.AMQUnresolvedAddressException;
import org.wso2.andes.client.failover.FailoverException;
import org.wso2.andes.client.failover.FailoverProtectedOperation;
import org.wso2.andes.client.protocol.AMQProtocolHandler;
import org.wso2.andes.configuration.ClientProperties;
import org.wso2.andes.exchange.ExchangeDefaults;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.BasicQosBody;
import org.wso2.andes.framing.BasicQosOkBody;
import org.wso2.andes.framing.ChannelOpenBody;
import org.wso2.andes.framing.ChannelOpenOkBody;
import org.wso2.andes.framing.ProtocolVersion;
import org.wso2.andes.framing.TxSelectBody;
import org.wso2.andes.framing.TxSelectOkBody;
import org.wso2.andes.jms.BrokerDetails;
import org.wso2.andes.jms.Connection;
import org.wso2.andes.jms.ConnectionListener;
import org.wso2.andes.jms.ConnectionURL;
import org.wso2.andes.jms.FailoverPolicy;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.url.URLSyntaxException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.nio.channels.UnresolvedAddressException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;

public class AMQConnection extends Closeable implements Connection, QueueConnection, TopicConnection, Referenceable
{
    private static final Logger _logger = LoggerFactory.getLogger(AMQConnection.class);


    /**
     * This is the "root" mutex that must be held when doing anything that could be impacted by failover. This must be
     * held by any child objects of this connection such as the session, producers and consumers.
     */
    private final Object _failoverMutex = new Object();

    private final Object _sessionCreationLock = new Object();

    /**
     * A channel is roughly analogous to a session. The server can negotiate the maximum number of channels per session
     * and we must prevent the client from opening too many.
     */
    private long _maximumChannelCount;

    /** The maximum size of frame supported by the server */
    private long _maximumFrameSize;

    /**
     * The protocol handler dispatches protocol events for this connection. For example, when the connection is dropped
     * the handler deals with this. It also deals with the initial dispatch of any protocol frames to their appropriate
     * handler.
     */
    protected AMQProtocolHandler _protocolHandler;

    /** Maps from session id (Integer) to AMQSession instance */
    private final ChannelToSessionMap _sessions = new ChannelToSessionMap();

    private final String _clientName;

    /** The user name to use for authentication */
    private String _username;

    /** The password to use for authentication */
    private String _password;

    /** The virtual path to connect to on the AMQ server */
    private String _virtualHost;

    protected ExceptionListener _exceptionListener;

    private ConnectionListener _connectionListener;

    private final ConnectionURL _connectionURL;

    /**
     * Whether this connection is started, i.e. whether messages are flowing to consumers. It has no meaning for message
     * publication.
     */
    protected volatile boolean _started;

    /** Policy dictating how to failover */
    protected FailoverPolicy _failoverPolicy;

    /*
     * _Connected should be refactored with a suitable wait object.
     */
    protected boolean _connected;

    /*
     * The connection meta data
     */
    private QpidConnectionMetaData _connectionMetaData;

    /** Configuration info for SSL */
    private SSLConfiguration _sslConfiguration;

    private AMQShortString _defaultTopicExchangeName = ExchangeDefaults.TOPIC_EXCHANGE_NAME;
    private AMQShortString _defaultQueueExchangeName = ExchangeDefaults.DIRECT_EXCHANGE_NAME;
    private AMQShortString _temporaryTopicExchangeName = ExchangeDefaults.TOPIC_EXCHANGE_NAME;
    private AMQShortString _temporaryQueueExchangeName = ExchangeDefaults.DIRECT_EXCHANGE_NAME;

    /** Thread Pool for executing connection level processes. Such as returning bounced messages. */
    private final ExecutorService _taskPool = Executors.newCachedThreadPool();
    private static final long DEFAULT_TIMEOUT = 1000 * 30;

    protected AMQConnectionDelegate _delegate;

    // this connection maximum number of prefetched messages
    private int _maxPrefetch;

    //Indicates whether persistent messages are synchronized
    private boolean _syncPersistence;

    //Indicates whether we need to sync on every message ack
    private boolean _syncAck;

    //Indicates the sync publish options (persistent|all)
    //By default it's async publish
    private String _syncPublish = "";

    // Indicates whether to use the old map message format or the
    // new amqp-0-10 encoded format.
    private boolean _useLegacyMapMessageFormat;

    /**
     * @param broker      brokerdetails
     * @param username    username
     * @param password    password
     * @param clientName  clientid
     * @param virtualHost virtualhost
     *
     * @throws AMQException
     * @throws URLSyntaxException
     */
    public AMQConnection(String broker, String username, String password, String clientName, String virtualHost)
            throws AMQException, URLSyntaxException
    {
        this(new AMQConnectionURL(
                ConnectionURL.AMQ_PROTOCOL + "://" + username + ":" + password + "@"
                + ((clientName == null) ? "" : clientName) + "/" + virtualHost + "?brokerlist='"
                + AMQBrokerDetails.checkTransport(broker) + "'"), null);
    }

    /**
     * @param broker      brokerdetails
     * @param username    username
     * @param password    password
     * @param clientName  clientid
     * @param virtualHost virtualhost
     *
     * @throws AMQException
     * @throws URLSyntaxException
     */
    public AMQConnection(String broker, String username, String password, String clientName, String virtualHost,
                         SSLConfiguration sslConfig) throws AMQException, URLSyntaxException
    {
        this(new AMQConnectionURL(
                ConnectionURL.AMQ_PROTOCOL + "://" + username + ":" + password + "@"
                + ((clientName == null) ? "" : clientName) + "/" + virtualHost + "?brokerlist='"
                + AMQBrokerDetails.checkTransport(broker) + "'"), sslConfig);
    }

    public AMQConnection(String host, int port, String username, String password, String clientName, String virtualHost)
            throws AMQException, URLSyntaxException
    {
        this(host, port, false, username, password, clientName, virtualHost, null);
    }

    public AMQConnection(String host, int port, String username, String password, String clientName, String virtualHost,
                         SSLConfiguration sslConfig) throws AMQException, URLSyntaxException
    {
        this(host, port, false, username, password, clientName, virtualHost, sslConfig);
    }

    public AMQConnection(String host, int port, boolean useSSL, String username, String password, String clientName,
                         String virtualHost, SSLConfiguration sslConfig) throws AMQException, URLSyntaxException
    {
        this(new AMQConnectionURL(
                useSSL
                ? (ConnectionURL.AMQ_PROTOCOL + "://" + username + ":" + password + "@"
                   + ((clientName == null) ? "" : clientName) + virtualHost + "?brokerlist='tcp://" + host + ":" + port
                   + "'" + "," + BrokerDetails.OPTIONS_SSL + "='true'")
                : (ConnectionURL.AMQ_PROTOCOL + "://" + username + ":" + password + "@"
                   + ((clientName == null) ? "" : clientName) + virtualHost + "?brokerlist='tcp://" + host + ":" + port
                   + "'" + "," + BrokerDetails.OPTIONS_SSL + "='false'")), sslConfig);
    }

    public AMQConnection(String connection) throws AMQException, URLSyntaxException
    {
        this(new AMQConnectionURL(connection), null);
    }

    public AMQConnection(String connection, SSLConfiguration sslConfig) throws AMQException, URLSyntaxException
    {
        this(new AMQConnectionURL(connection), sslConfig);
    }

    /**
     * @todo Some horrible stuff going on here with setting exceptions to be non-null to detect if an exception
     * was thrown during the connection! Intention not clear. Use a flag anyway, not exceptions... Will fix soon.
     */
    public AMQConnection(ConnectionURL connectionURL, SSLConfiguration sslConfig) throws AMQException
    {
        if (connectionURL == null)
        {
            throw new IllegalArgumentException("Connection must be specified");
        }

        // set this connection maxPrefetch
        if (connectionURL.getOption(ConnectionURL.OPTIONS_MAXPREFETCH) != null)
        {
            _maxPrefetch = Integer.parseInt(connectionURL.getOption(ConnectionURL.OPTIONS_MAXPREFETCH));
        }
        else
        {
            // use the default value set for all connections
            _maxPrefetch = Integer.parseInt(System.getProperty(ClientProperties.MAX_PREFETCH_PROP_NAME,
                    ClientProperties.MAX_PREFETCH_DEFAULT));
        }

        if (connectionURL.getOption(ConnectionURL.OPTIONS_SYNC_PERSISTENCE) != null)
        {
            _syncPersistence =
                Boolean.parseBoolean(connectionURL.getOption(ConnectionURL.OPTIONS_SYNC_PERSISTENCE));
            _logger.warn("sync_persistence is a deprecated property, " +
            		"please use sync_publish={persistent|all} instead");
        }
        else
        {
            // use the default value set for all connections
            _syncPersistence = Boolean.getBoolean(ClientProperties.SYNC_PERSISTENT_PROP_NAME);
            if (_syncPersistence)
            {
                _logger.warn("sync_persistence is a deprecated property, " +
                        "please use sync_publish={persistent|all} instead");
            }
        }

        if (connectionURL.getOption(ConnectionURL.OPTIONS_SYNC_ACK) != null)
        {
            _syncAck = Boolean.parseBoolean(connectionURL.getOption(ConnectionURL.OPTIONS_SYNC_ACK));
        }
        else
        {
            // use the default value set for all connections
            _syncAck = Boolean.getBoolean(ClientProperties.SYNC_ACK_PROP_NAME);
        }

        if (connectionURL.getOption(ConnectionURL.OPTIONS_SYNC_PUBLISH) != null)
        {
            _syncPublish = connectionURL.getOption(ConnectionURL.OPTIONS_SYNC_PUBLISH);
        }
        else
        {
            // use the default value set for all connections
            _syncPublish = System.getProperty((ClientProperties.SYNC_PUBLISH_PROP_NAME),_syncPublish);
        }

        if (connectionURL.getOption(ConnectionURL.OPTIONS_USE_LEGACY_MAP_MESSAGE_FORMAT) != null)
        {
            _useLegacyMapMessageFormat =  Boolean.parseBoolean(
                    connectionURL.getOption(ConnectionURL.OPTIONS_USE_LEGACY_MAP_MESSAGE_FORMAT));
        }
        else
        {
            // use the default value set for all connections
            _useLegacyMapMessageFormat = Boolean.getBoolean(ClientProperties.USE_LEGACY_MAP_MESSAGE_FORMAT);
        }

        // Requires permission java.util.PropertyPermission "qpid.amqp.version", "write"
        String amqpVersion = AccessController.doPrivileged(new PrivilegedAction<String>() {
            public String run() {
                return System.getProperty((ClientProperties.AMQP_VERSION), "0-91");
            }
        });

      //  String amqpVersion = System.getProperty((ClientProperties.AMQP_VERSION), "0-10");

        _logger.debug("AMQP version " + amqpVersion);

        _failoverPolicy = new FailoverPolicy(connectionURL, this);
        BrokerDetails brokerDetails = _failoverPolicy.getCurrentBrokerDetails();
        if ("0-8".equals(amqpVersion))
        {
            _delegate = new AMQConnectionDelegate_8_0(this);
        }
        else if ("0-9".equals(amqpVersion))
        {
            _delegate = new AMQConnectionDelegate_0_9(this);
        }
        else if ("0-91".equals(amqpVersion) || "0-9-1".equals(amqpVersion))
        {
            _delegate = new AMQConnectionDelegate_9_1(this);
        }
        else
        {
            _delegate = new AMQConnectionDelegate_0_10(this);
        }

        if (_logger.isDebugEnabled())
        {
            _logger.debug("Connection:" + connectionURL);
        }

        _connectionURL = connectionURL;

        _clientName = connectionURL.getClientName();
        _username = connectionURL.getUsername();
        _password = connectionURL.getPassword();

        setVirtualHost(connectionURL.getVirtualHost());

        if (connectionURL.getDefaultQueueExchangeName() != null)
        {
            _defaultQueueExchangeName = connectionURL.getDefaultQueueExchangeName();
        }

        if (connectionURL.getDefaultTopicExchangeName() != null)
        {
            _defaultTopicExchangeName = connectionURL.getDefaultTopicExchangeName();
        }

        if (connectionURL.getTemporaryQueueExchangeName() != null)
        {
            _temporaryQueueExchangeName = connectionURL.getTemporaryQueueExchangeName();
        }

        if (connectionURL.getTemporaryTopicExchangeName() != null)
        {
            _temporaryTopicExchangeName = connectionURL.getTemporaryTopicExchangeName();
        }

        _protocolHandler = new AMQProtocolHandler(this);

        if (_logger.isDebugEnabled()) {
            _logger.debug("Connecting with ProtocolHandler Version:" + _protocolHandler.getProtocolVersion());
        }

        // We are not currently connected
        _connected = false;

        boolean retryAllowed = true;
        Exception connectionException = null;
        while (!_connected && retryAllowed && brokerDetails != null)
        {
            ProtocolVersion pe = null;
            try
            {
                pe = makeBrokerConnection(brokerDetails);
            }
            catch (Exception e)
            {
                if (_logger.isInfoEnabled())
                {
                    _logger.info("Unable to connect to broker at " +
                                 _failoverPolicy.getCurrentBrokerDetails(),
                                 e);
                }
                connectionException = e;
            }

            if (pe != null)
            {
                // reset the delegate to the version returned by the
                // broker
                initDelegate(pe);
            }
            else if (!_connected)
            {
                retryAllowed = _failoverPolicy.failoverAllowed();
                brokerDetails = _failoverPolicy.getNextBrokerDetails();
            }
        }
        verifyClientID();

        if (_logger.isDebugEnabled())
        {
            _logger.debug("Are we connected:" + _connected);
        }

        if (!_connected)
        {
            if (_logger.isDebugEnabled())
            {
                _logger.debug("Last attempted ProtocolHandler Version:"+_protocolHandler.getProtocolVersion());
            }

            String message = null;

            if (connectionException != null)
            {
                if (connectionException.getCause() != null)
                {
                    message = connectionException.getCause().getMessage();
                }
                else
                {
                    message = connectionException.getMessage();
                }
            }

            if ((message == null) || message.equals(""))
            {
                if (message == null)
                {
                    message = "Unable to Connect";
                }
                else // can only be "" if getMessage() returned it therfore lastException != null
                {
                    message = "Unable to Connect:" + connectionException.getClass();
                }
            }

            for (Throwable th = connectionException; th != null; th = th.getCause())
            {
                if (th instanceof UnresolvedAddressException ||
                    th instanceof UnknownHostException)
                {
                    throw new AMQUnresolvedAddressException
                        (message,
                         _failoverPolicy.getCurrentBrokerDetails().toString(),
                         connectionException);
                }
            }

            throw new AMQConnectionFailureException(message, connectionException);
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug("Connected with ProtocolHandler Version:" + _protocolHandler.getProtocolVersion());
        }

        _sessions.setMaxChannelID(_delegate.getMaxChannelID());
        _sessions.setMinChannelID(_delegate.getMinChannelID());

        _connectionMetaData = new QpidConnectionMetaData(this);
    }

    protected boolean checkException(Throwable thrown)
    {
        Throwable cause = thrown.getCause();

        if (cause == null)
        {
            cause = thrown;
        }

        return ((cause instanceof ConnectException) || (cause instanceof UnresolvedAddressException));
    }

    private void initDelegate(ProtocolVersion pe) throws AMQProtocolException
    {
        try
        {
            String delegateClassName = String.format
                                    ("org.wso2.andes.client.AMQConnectionDelegate_%s_%s",
                                     pe.getMajorVersion(), pe.getMinorVersion());
            if (_logger.isDebugEnabled()) {
                _logger.debug("Looking up delegate '" + delegateClassName + "' Based on PE:" + pe);
            }
            Class c = Class.forName(delegateClassName);
            Class partypes[] = new Class[1];
            partypes[0] = AMQConnection.class;
            _delegate = (AMQConnectionDelegate) c.getConstructor(partypes).newInstance(this);
            //Update our session to use this new protocol version
            _protocolHandler.getProtocolSession().setProtocolVersion(_delegate.getProtocolVersion());

        }
        catch (ClassNotFoundException e)
        {
            throw new AMQProtocolException
                (AMQConstant.UNSUPPORTED_CLIENT_PROTOCOL_ERROR,
                 String.format("Protocol: %s.%s is rquired by the broker but is not " +
                               "currently supported by this client library implementation",
                               pe.getMajorVersion(), pe.getMinorVersion()),
                 e);
        }
        catch (NoSuchMethodException e)
        {
            throw new RuntimeException("unable to locate constructor for delegate", e);
        }
        catch (InstantiationException e)
        {
            throw new RuntimeException("error instantiating delegate", e);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException("error accessing delegate", e);
        }
        catch (InvocationTargetException e)
        {
            throw new RuntimeException("error invoking delegate", e);
        }
    }

    private void setVirtualHost(String virtualHost)
    {
        if (virtualHost != null && virtualHost.startsWith("/"))
        {
            virtualHost = virtualHost.substring(1);
        }

        _virtualHost = virtualHost;
    }

    public boolean attemptReconnection(String host, int port)
    {
        BrokerDetails bd = new AMQBrokerDetails(host, port, _sslConfiguration);

        _failoverPolicy.setBroker(bd);

        try
        {
            makeBrokerConnection(bd);

            return true;
        }
        catch (Exception e)
        {
            if (_logger.isInfoEnabled())
            {
                _logger.info("Unable to connect to broker at " + bd);
            }

            attemptReconnection();
        }

        return false;
    }

    public boolean attemptReconnection()
    {
        BrokerDetails broker = null;
        while (_failoverPolicy.failoverAllowed() && (broker = _failoverPolicy.getNextBrokerDetails()) != null)
        {
            try
            {
                makeBrokerConnection(broker);
                return true;
            }
            catch (Exception e)
            {
                if (!(e instanceof AMQException))
                {
                    if (_logger.isInfoEnabled())
                    {
                        _logger.info("Unable to connect to broker at " + _failoverPolicy.getCurrentBrokerDetails(), e);
                    }
                }
                else
                {
                    if (_logger.isInfoEnabled())
                    {
                        _logger.info(e.getMessage() + ":Unable to connect to broker at "
                                     + _failoverPolicy.getCurrentBrokerDetails());
                    }
                }
            }
        }

        // connection unsuccessful
        return false;
    }

    public ProtocolVersion makeBrokerConnection(final BrokerDetails brokerDetail) throws IOException, AMQException {
        // Requires permission java.lang.RuntimePermission "modifyThreadGroup"
        try {
            ProtocolVersion returnObject = AccessController.doPrivileged(new PrivilegedExceptionAction<ProtocolVersion>() {
                public ProtocolVersion run() throws IOException, AMQException {

                    return _delegate.makeBrokerConnection(brokerDetail);

                }

            });
            return returnObject;
        } catch (PrivilegedActionException e) {
            if (e.getException() instanceof IOException) {
                throw (IOException) e.getException();
            } else if (e.getException() instanceof AMQException) {
                throw (AMQException) e.getException();
            }
        }
        return null;

    }

    public <T, E extends Exception> T executeRetrySupport(final FailoverProtectedOperation<T, E> operation) throws E {
        // Requires permission java.lang.RuntimePermission "modifyThreadGroup"
        T returnObject = null;
        try {
            returnObject = AccessController.doPrivileged(new PrivilegedExceptionAction<T>() {
                public T run() throws E {

                    return _delegate.executeRetrySupport(operation);

                }
            });
        } catch (PrivilegedActionException e) {
            throw (E) e.getException();
        }
        return returnObject;
    }


    /**
     * Get the details of the currently active broker
     *
     * @return null if no broker is active (i.e. no successful connection has been made, or the BrokerDetail instance
     *         otherwise
     */
    public BrokerDetails getActiveBrokerDetails()
    {
        return _failoverPolicy.getCurrentBrokerDetails();
    }

    public boolean failoverAllowed()
    {
        if (!_connected)
        {
            return false;
        }
        else
        {
            return _failoverPolicy.failoverAllowed();
        }
    }

    public org.wso2.andes.jms.Session createSession(final boolean transacted, final int acknowledgeMode) throws JMSException
    {
        return createSession(transacted, acknowledgeMode, _maxPrefetch);
    }

    public org.wso2.andes.jms.Session createSession(final boolean transacted, final int acknowledgeMode, final int prefetch)
            throws JMSException
    {
        return createSession(transacted, acknowledgeMode, prefetch, prefetch);
    }

    public org.wso2.andes.jms.Session createSession(final boolean transacted, final int acknowledgeMode,
                                                     final int prefetchHigh, final int prefetchLow) throws JMSException
    {
        synchronized (_sessionCreationLock)
        {
            checkNotClosed();
            return _delegate.createSession(transacted, acknowledgeMode, prefetchHigh, prefetchLow);
        }
    }

    private void createChannelOverWire(int channelId, int prefetchHigh, int prefetchLow, boolean transacted)
            throws AMQException, FailoverException
    {

        ChannelOpenBody channelOpenBody = getProtocolHandler().getMethodRegistry().createChannelOpenBody(null);

        // TODO: Be aware of possible changes to parameter order as versions change.

        _protocolHandler.syncWrite(channelOpenBody.generateFrame(channelId), ChannelOpenOkBody.class);

        BasicQosBody basicQosBody = getProtocolHandler().getMethodRegistry().createBasicQosBody(0, prefetchHigh, false);

        // todo send low water mark when protocol allows.
        // todo Be aware of possible changes to parameter order as versions change.
        _protocolHandler.syncWrite(basicQosBody.generateFrame(channelId), BasicQosOkBody.class);

        if (transacted)
        {
            if (_logger.isDebugEnabled())
            {
                _logger.debug("Issuing TxSelect for " + channelId);
            }

            TxSelectBody body = getProtocolHandler().getMethodRegistry().createTxSelectBody();

            // TODO: Be aware of possible changes to parameter order as versions change.
            _protocolHandler.syncWrite(body.generateFrame(channelId), TxSelectOkBody.class);
        }
    }

    public void setFailoverPolicy(FailoverPolicy policy)
    {
        _failoverPolicy = policy;
    }

    public FailoverPolicy getFailoverPolicy()
    {
        return _failoverPolicy;
    }

    /**
     * Returns an AMQQueueSessionAdaptor which wraps an AMQSession and throws IllegalStateExceptions where specified in
     * the JMS spec
     *
     * @param transacted
     * @param acknowledgeMode
     *
     * @return QueueSession
     *
     * @throws JMSException
     */
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException
    {
        return new AMQQueueSessionAdaptor(createSession(transacted, acknowledgeMode));
    }

    /**
     * Returns an AMQTopicSessionAdapter which wraps an AMQSession and throws IllegalStateExceptions where specified in
     * the JMS spec
     *
     * @param transacted
     * @param acknowledgeMode
     *
     * @return TopicSession
     *
     * @throws JMSException
     */
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException
    {
        return new AMQTopicSessionAdaptor(createSession(transacted, acknowledgeMode));
    }

    public boolean channelLimitReached()
    {
        return _sessions.size() >= _maximumChannelCount;
    }

    public String getClientID() throws JMSException
    {
        checkNotClosed();

        return _clientName;
    }

    public void setClientID(String clientID) throws JMSException
    {
        checkNotClosed();
        // in AMQP it is not possible to change the client ID. If one is not specified
        // upon connection construction, an id is generated automatically. Therefore
        // we can always throw an exception.
        if (!Boolean.getBoolean(ClientProperties.IGNORE_SET_CLIENTID_PROP_NAME))
        {
            throw new IllegalStateException("Client name cannot be changed after being set");
        }
        else
        {
            _logger.info("Operation setClientID is ignored using ID: " + getClientID());
        }
    }

    public ConnectionMetaData getMetaData() throws JMSException
    {
        checkNotClosed();

        return _connectionMetaData;

    }

    public ExceptionListener getExceptionListener() throws JMSException
    {
        checkNotClosed();

        return _exceptionListener;
    }

    public void setExceptionListener(ExceptionListener listener) throws JMSException
    {
        checkNotClosed();
        _exceptionListener = listener;
    }

    /**
     * Start the connection, i.e. start flowing messages. Note that this method must be called only from a single thread
     * and is not thread safe (which is legal according to the JMS specification).
     *
     * @throws JMSException
     */
    public void start() throws JMSException
    {
        checkNotClosed();
        if (!_started)
        {
            _started = true;
            final Iterator it = _sessions.values().iterator();
            while (it.hasNext())
            {
                final AMQSession s = (AMQSession) (it.next());
                try
                {
                    s.start();
                }
                catch (AMQException e)
                {
                    throw new JMSAMQException(e);
                }
            }

        }
    }

    public void stop() throws JMSException
    {
        checkNotClosed();
        if (_started)
        {
            for (Iterator i = _sessions.values().iterator(); i.hasNext();)
            {
                try
                {
                    ((AMQSession) i.next()).stop();
                }
                catch (AMQException e)
                {
                    throw new JMSAMQException(e);
                }
            }

            _started = false;
        }
    }

    public void     close() throws JMSException
    {
        close(DEFAULT_TIMEOUT);
    }

    public void close(long timeout) throws JMSException
    {
        close(new ArrayList<AMQSession>(_sessions.values()), timeout);
    }

    public void close(List<AMQSession> sessions, long timeout) throws JMSException
    {
        if (!_closed.getAndSet(true))
        {
            _closing.set(true);
            try{
                doClose(sessions, timeout);
            }finally{
                _closing.set(false);
            }
        }
    }

    private void doClose(List<AMQSession> sessions, long timeout) throws JMSException
    {
        synchronized (_sessionCreationLock)
        {
            if (!sessions.isEmpty())
            {
                AMQSession session = sessions.remove(0);

                session.getMessageDeliveryLock().lock();
                try {
                    doClose(sessions, timeout);
                } finally {
                    session.getMessageDeliveryLock().unlock();
                }
            }
            else
            {
                synchronized (getFailoverMutex())
                {
                    try
                    {
                        long startCloseTime = System.currentTimeMillis();

	                    closeAllSessions(null, timeout, startCloseTime);

                        // Requires permission java.lang.RuntimePermission "modifyThread"
                        AccessController.doPrivileged(new PrivilegedAction<Void>() {
                            @Override
                            public Void run() {
                                _taskPool.shutdown();
                                return null;
                            }
                        });

                        if (!_taskPool.isTerminated())
                        {
                            try
                            {
                                // adjust timeout
                                long taskPoolTimeout = adjustTimeout(timeout, startCloseTime);

                                _taskPool.awaitTermination(taskPoolTimeout, TimeUnit.MILLISECONDS);
                            }
                            catch (InterruptedException e)
                            {
                                _logger.info("Interrupted while shutting down connection thread pool.");
                            }
                        }

                        // adjust timeout
                        timeout = adjustTimeout(timeout, startCloseTime);
                        _delegate.closeConnection(timeout);

                        //If the taskpool hasn't shutdown by now then give it shutdownNow.
                        // This will interupt any running tasks.
                        if (!_taskPool.isTerminated())
                        {
                            List<Runnable> tasks = _taskPool.shutdownNow();
                            for (Runnable r : tasks)
                            {
                                _logger.warn("Connection close forced taskpool to prevent execution:" + r);
                            }
                        }
                    }
                    catch (AMQException e)
                    {
                        _logger.error("error:", e);
                        JMSException jmse = new JMSException("Error closing connection: " + e);
                        jmse.setLinkedException(e);
                        jmse.initCause(e);
                        throw jmse;
                    }
                }
            }
        }
    }

    private long adjustTimeout(long timeout, long startTime)
    {
        long now = System.currentTimeMillis();
        timeout -= now - startTime;
        if (timeout < 0)
        {
            timeout = 0;
        }

        return timeout;
    }

    /**
     * Marks all sessions and their children as closed without sending any protocol messages. Useful when you need to
     * mark objects "visible" in userland as closed after failover or other significant event that impacts the
     * connection. <p/> The caller must hold the failover mutex before calling this method.
     */
    private void markAllSessionsClosed()
    {
        final LinkedList sessionCopy = new LinkedList(_sessions.values());
        final Iterator it = sessionCopy.iterator();
        while (it.hasNext())
        {
            final AMQSession session = (AMQSession) it.next();

            session.markClosed();
        }

        _sessions.clear();
    }

    /**
     * Close all the sessions, either due to normal connection closure or due to an error occurring.
     *
     * @param cause if not null, the error that is causing this shutdown <p/> The caller must hold the failover mutex
     *              before calling this method.
     */
    private void closeAllSessions(Throwable cause, long timeout, long starttime) throws JMSException
    {
        final LinkedList sessionCopy = new LinkedList(_sessions.values());
        final Iterator it = sessionCopy.iterator();
        JMSException sessionException = null;
        while (it.hasNext())
        {
            final AMQSession session = (AMQSession) it.next();
            if (cause != null)
            {
                session.closed(cause);
            }
            else
            {
                try
                {
                    if (starttime != -1)
                    {
                        timeout = adjustTimeout(timeout, starttime);
                    }

                    session.close(timeout);
                }
                catch (JMSException e)
                {
                    _logger.error("Error closing session: " + e);
                    sessionException = e;
                }
            }
        }

        _sessions.clear();
        if (sessionException != null)
        {
            throw sessionException;
        }
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector,
                                                       ServerSessionPool sessionPool, int maxMessages) throws JMSException
    {
        checkNotClosed();

        return null;
    }

    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector, ServerSessionPool sessionPool,
                                                       int maxMessages) throws JMSException
    {
        checkNotClosed();

        return null;
    }

    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector, ServerSessionPool sessionPool,
                                                       int maxMessages) throws JMSException
    {
        checkNotClosed();

        return null;
    }

    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector,
                                                              ServerSessionPool sessionPool, int maxMessages) throws JMSException
    {
        // TODO Auto-generated method stub
        checkNotClosed();

        return null;
    }

    public long getMaximumChannelCount() throws JMSException
    {
        checkNotClosed();

        return _maximumChannelCount;
    }

    public void setConnectionListener(ConnectionListener listener)
    {
        _connectionListener = listener;
    }

    public ConnectionListener getConnectionListener()
    {
        return _connectionListener;
    }

    public void setMaximumChannelCount(long maximumChannelCount)
    {
        _maximumChannelCount = maximumChannelCount;
    }

    public void setMaximumFrameSize(long frameMax)
    {
        _maximumFrameSize = frameMax;
    }

    public long getMaximumFrameSize()
    {
        return _maximumFrameSize;
    }

    public ChannelToSessionMap getSessions()
    {
        return _sessions;
    }

    /**
     * Set all sessions in this connection to active state. This will disable any enforced flow control
     */
    public void disableFlowControl() {
        for (AMQSession session : _sessions.values()) {
            session.setFlowControl(true); // Set channel active
        }
    }

    public String getUsername()
    {
        return _username;
    }

    public void setUsername(String id)
    {
        _username = id;
    }

    public String getPassword()
    {
        return _password;
    }

    public String getVirtualHost()
    {
        return _virtualHost;
    }

    public AMQProtocolHandler getProtocolHandler()
    {
        return _protocolHandler;
    }

    public boolean started()
    {
        return _started;
    }

    public void bytesSent(long writtenBytes)
    {
        if (_connectionListener != null)
        {
            _connectionListener.bytesSent(writtenBytes);
        }
    }

    public void bytesReceived(long receivedBytes)
    {
        if (_connectionListener != null)
        {
            _connectionListener.bytesReceived(receivedBytes);
        }
    }

    /**
     * Fire the preFailover event to the registered connection listener (if any)
     *
     * @param redirect true if this is the result of a redirect request rather than a connection error
     *
     * @return true if no listener or listener does not veto change
     */
    public boolean firePreFailover(boolean redirect)
    {
        boolean proceed = true;
        if (_connectionListener != null)
        {
            proceed = _connectionListener.preFailover(redirect);
        }

        return proceed;
    }

    /**
     * Fire the preResubscribe event to the registered connection listener (if any). If the listener vetoes
     * resubscription then all the sessions are closed.
     *
     * @return true if no listener or listener does not veto resubscription.
     *
     * @throws JMSException
     */
    public boolean firePreResubscribe() throws JMSException
    {
        if (_connectionListener != null)
        {
            boolean resubscribe = _connectionListener.preResubscribe();
            if (!resubscribe)
            {
                markAllSessionsClosed();
            }

            return resubscribe;
        }
        else
        {
            return true;
        }
    }

    /** Fires a failover complete event to the registered connection listener (if any). */
    public void fireFailoverComplete()
    {
        if (_connectionListener != null)
        {
            _connectionListener.failoverComplete();
        }
    }

    /**
     * In order to protect the consistency of the connection and its child sessions, consumers and producers, the
     * "failover mutex" must be held when doing any operations that could be corrupted during failover.
     *
     * @return a mutex. Guaranteed never to change for the lifetime of this connection even if failover occurs.
     */
    public final Object getFailoverMutex()
    {
        return _failoverMutex;
    }

    public void failoverPrep()
    {
        _delegate.failoverPrep();
    }

    public void resubscribeSessions() throws JMSException, AMQException, FailoverException
    {
        _delegate.resubscribeSessions();
    }

    /**
     * If failover is taking place this will block until it has completed. If failover is not taking place it will
     * return immediately.
     *
     * @throws InterruptedException
     */
    public void blockUntilNotFailingOver() throws InterruptedException
    {
        _protocolHandler.blockUntilNotFailingOver();
    }

    /**
     * Invoked by the AMQProtocolSession when a protocol session exception has occurred. This method sends the exception
     * to a JMS exception listener, if configured, and propagates the exception to sessions, which in turn will
     * propagate to consumers. This allows synchronous consumers to have exceptions thrown to them.
     *
     * @param cause the exception
     */
    public void exceptionReceived(Throwable cause)
    {

        if (_logger.isDebugEnabled())
        {
            _logger.debug("exceptionReceived done by:" + Thread.currentThread().getName(), cause);
        }

        final JMSException je;
        if (cause instanceof JMSException)
        {
            je = (JMSException) cause;
        }
        else
        {
            AMQConstant code = null;

            if (cause instanceof AMQException)
            {
                code = ((AMQException) cause).getErrorCode();
            }

            if (code != null)
            {
                je = new JMSException(Integer.toString(code.getCode()), "Exception thrown against " + toString() + ": " + cause);
            }
            else
            {
                //Should never get here as all AMQEs are required to have an ErrorCode!
                // Other than AMQDisconnectedEx!

                if (cause instanceof AMQDisconnectedException)
                {
                    Exception last = _protocolHandler.getStateManager().getLastException();
                    if (last != null)
                    {
                        if (_logger.isDebugEnabled()) {
                            _logger.debug("StateManager had an exception for us to use a cause of our Disconnected Exception");
                        }
                        cause = last;
                    }
                }
                je = new JMSException("Exception thrown against " + toString() + ": " + cause);
            }

            if (cause instanceof Exception)
            {
                je.setLinkedException((Exception) cause);
            }

            je.initCause(cause);
        }

        boolean closer = false;

        // in the case of an IOException, MINA has closed the protocol session so we set _closed to true
        // so that any generic client code that tries to close the connection will not mess up this error
        // handling sequence
        if (cause instanceof IOException || cause instanceof AMQDisconnectedException)
        {
            // If we have an IOE/AMQDisconnect there is no connection to close on.
            _closing.set(false);
            closer = !_closed.getAndSet(true);

            _protocolHandler.getProtocolSession().notifyError(je);
        }

        // decide if we are going to close the session
        if (hardError(cause)) {
            closer = (!_closed.getAndSet(true)) || closer;
            {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Closing AMQConnection due to :" + cause);
                }
            }
        } else {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Not a hard-error connection not closing: " + cause);
            }
        }

        // deliver the exception if there is a listener
        if (_exceptionListener != null) {
            _exceptionListener.onException(je);
        } else {
            _logger.error("Throwable Received but no listener set: " + cause);
        }

        // if we are closing the connection, close sessions first
        if (closer) {
            // get the failover mutex before trying to close
            synchronized (getFailoverMutex()) {
                try {
                    closeAllSessions(cause, -1, -1); // FIXME: when doing this end up with RejectedExecutionException from executor.
                } catch (JMSException e) {
                    _logger.error("Error closing all sessions: " + e, e);
                }
            }
        }
    }

    private boolean hardError(Throwable cause)
    {
        if (cause instanceof AMQException)
        {
            return ((AMQException) cause).isHardError();
        }

        return true;
    }

    void registerSession(int channelId, AMQSession session)
    {
        _sessions.put(channelId, session);
    }

    public void deregisterSession(int channelId)
    {
        _sessions.remove(channelId);
    }

    public String toString()
    {
        StringBuffer buf = new StringBuffer("AMQConnection:\n");
        if (_failoverPolicy.getCurrentBrokerDetails() == null)
        {
            buf.append("No active broker connection");
        }
        else
        {
            BrokerDetails bd = _failoverPolicy.getCurrentBrokerDetails();
            buf.append("Host: ").append(String.valueOf(bd.getHost()));
            buf.append("\nPort: ").append(String.valueOf(bd.getPort()));
        }

        buf.append("\nVirtual Host: ").append(String.valueOf(_virtualHost));
        buf.append("\nClient ID: ").append(String.valueOf(_clientName));
        buf.append("\nActive session count: ").append((_sessions == null) ? 0 : _sessions.size());

        return buf.toString();
    }

    /**
     * Returns connection url.
     * @return connection url
     */
    public ConnectionURL getConnectionURL()
    {
        return _connectionURL;
    }

    /**
     * Returns stringified connection url.   This url is suitable only for display
     * as {@link AMQConnectionURL#toString()} converts any password to asterisks.
     * @return connection url
     */
    public String toURL()
    {
        return _connectionURL.toString();
    }

    public Reference getReference() throws NamingException
    {
        return new Reference(AMQConnection.class.getName(), new StringRefAddr(AMQConnection.class.getName(), toURL()),
                             AMQConnectionFactory.class.getName(), null); // factory location
    }

    public SSLConfiguration getSSLConfiguration()
    {
        return _sslConfiguration;
    }

    public AMQShortString getDefaultTopicExchangeName()
    {
        return _defaultTopicExchangeName;
    }

    public void setDefaultTopicExchangeName(AMQShortString defaultTopicExchangeName)
    {
        _defaultTopicExchangeName = defaultTopicExchangeName;
    }

    public AMQShortString getDefaultQueueExchangeName()
    {
        return _defaultQueueExchangeName;
    }

    public void setDefaultQueueExchangeName(AMQShortString defaultQueueExchangeName)
    {
        _defaultQueueExchangeName = defaultQueueExchangeName;
    }

    public AMQShortString getTemporaryTopicExchangeName()
    {
        return _temporaryTopicExchangeName;
    }

    public AMQShortString getTemporaryQueueExchangeName()
    {
        return _temporaryQueueExchangeName; // To change body of created methods use File | Settings | File Templates.
    }

    public void setTemporaryTopicExchangeName(AMQShortString temporaryTopicExchangeName)
    {
        _temporaryTopicExchangeName = temporaryTopicExchangeName;
    }

    public void setTemporaryQueueExchangeName(AMQShortString temporaryQueueExchangeName)
    {
        _temporaryQueueExchangeName = temporaryQueueExchangeName;
    }

    public void performConnectionTask(Runnable task)
    {
        _taskPool.execute(task);
    }

    public AMQSession getSession(int channelId)
    {
        return _sessions.get(channelId);
    }

    public ProtocolVersion getProtocolVersion()
    {
        return _delegate.getProtocolVersion();
    }

    public boolean isFailingOver()
    {
        return (_protocolHandler.getFailoverLatch() != null);
    }

    /**
     * Get the maximum number of messages that this connection can pre-fetch.
     *
     * @return The maximum number of messages that this connection can pre-fetch.
     */
    public long getMaxPrefetch()
    {
        return _maxPrefetch;
    }

    /**
     * Indicates whether persistent messages are synchronized
     *
     * @return true if persistent messages are synchronized false otherwise
     */
    public boolean getSyncPersistence()
    {
        return _syncPersistence;
    }

    /**
     * Indicates whether we need to sync on every message ack
     */
    public boolean getSyncAck()
    {
        return _syncAck;
    }

    public String getSyncPublish()
    {
        return _syncPublish;
    }

    public int getNextChannelID()
    {
        return _sessions.getNextChannelId();
    }

    public boolean isUseLegacyMapMessageFormat()
    {
        return _useLegacyMapMessageFormat;
    }

    private void verifyClientID() throws AMQException
    {
        if (Boolean.getBoolean(ClientProperties.QPID_VERIFY_CLIENT_ID))
        {
            try
            {
                _delegate.verifyClientID();
            }
            catch(JMSException e)
            {
                throw new AMQException(AMQConstant.ALREADY_EXISTS,"ClientID must be unique",e);
            }
        }
    }
}
