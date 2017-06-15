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
import org.wso2.andes.configuration.ClientProperties;
import org.wso2.andes.jms.Connection;
import org.wso2.andes.jms.ConnectionListener;
import org.wso2.andes.jms.ConnectionURL;
import org.wso2.andes.url.AMQBindingURL;
import org.wso2.andes.url.URLSyntaxException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Hashtable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.JMSRuntimeException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAJMSContext;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnection;
import javax.jms.XATopicConnectionFactory;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;


public class AMQConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory,
                                             ObjectFactory, Referenceable, XATopicConnectionFactory,
                                             XAQueueConnectionFactory, XAConnectionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(AMQConnectionFactory.class);

    private String host;
    private int port;
    private String defaultUsername;
    private String defaultPassword;
    private String virtualPath;

    private ConnectionURL connectionDetails;
    private SSLConfiguration sslConfig;

    private ConnectionListener connectionListener = null;
    private ThreadLocal<Boolean> removeBURL = new ThreadLocal<Boolean>();

    /**
     * Scheduled executor share by the XAConnectionImpl objects
     */
    private ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    private static final Logger log = LoggerFactory.getLogger(AMQConnectionFactory.class);

    public AMQConnectionFactory()
    {

    }

    /**
     * This is the Only constructor used!
     * It is used form the context and from the JNDI objects.
     */
    public AMQConnectionFactory(String url) throws URLSyntaxException
    {
        connectionDetails = new AMQConnectionURL(url);
    }

    /**
     * This constructor is never used!
     */
    public AMQConnectionFactory(ConnectionURL url)
    {
        connectionDetails = url;
    }

    /**
     * This constructor is never used!
     */
    public AMQConnectionFactory(String broker, String username, String password, String clientName, String virtualHost)
            throws URLSyntaxException
    {
        this(new AMQConnectionURL(
                ConnectionURL.AMQ_PROTOCOL + "://" + username + ":" + password + "@" + clientName + "/" + virtualHost + "?brokerlist='" + broker + "'"));
    }

    /**
     * This constructor is never used!
     */
    public AMQConnectionFactory(String host, int port, String virtualPath)
    {
        this(host, port, "guest", "guest", virtualPath);
    }

    /**
     * This constructor is never used!
     */
    public AMQConnectionFactory(String host, int port, String defaultUsername,
                                String defaultPassword, String virtualPath)
    {
        this.host = host;
        this.port = port;
        this.defaultUsername = defaultUsername;
        this.defaultPassword = defaultPassword;
        this.virtualPath = virtualPath;

//todo when setting Host/Port has been resolved then we can use this otherwise those methods won't work with the following line.
//        _connectionDetails = new AMQConnectionURL(
//                ConnectionURL.AMQ_PROTOCOL + "://" +
//                        _defaultUsername + ":" + _defaultPassword + "@" +
//                        virtualPath + "?brokerlist='tcp://" + host + ":" + port + "'");
    }

    /**
     * @return The defaultPassword.
     */
    public final String getDefaultPassword(String password)
    {
        if (connectionDetails != null)
        {
            return connectionDetails.getPassword();
        }
        else
        {
            return defaultPassword;
        }
    }

    /**
     * @param password The defaultPassword to set.
     */
    public final void setDefaultPassword(String password)
    {
        if (connectionDetails != null)
        {
            connectionDetails.setPassword(password);
        }
        defaultPassword = password;
    }

    /**
     * Getter for SSLConfiguration
     *
     * @return SSLConfiguration if set, otherwise null
     */
    public final SSLConfiguration getSSLConfiguration()
    {
        return sslConfig;
    }

    /**
     * Setter for SSLConfiguration
     *
     * @param sslConfig config to store
     */
    public final void setSSLConfiguration(SSLConfiguration sslConfig)
    {
        this.sslConfig = sslConfig;
    }

    /**
     * @return The defaultUsername.
     */
    public final String getDefaultUsername(String password)
    {
        if (connectionDetails != null)
        {
            return connectionDetails.getUsername();
        }
        else
        {
            return defaultUsername;
        }
    }

    /**
     * @param username The defaultUsername to set.
     */
    public final void setDefaultUsername(String username)
    {
        if (connectionDetails != null)
        {
            connectionDetails.setUsername(username);
        }
        defaultUsername = username;
    }

    /**
     * @return The host.
     */
    public final String getHost()
    {
        //todo this doesn't make sense in a multi broker URL as we have no current as that is done by AMQConnection
        return host;
    }

    /**
     * @param host The host to set.
     */
    public final void setHost(String host)
    {
        //todo if _connectionDetails is set then run _connectionDetails.addBrokerDetails()
        // Should perhaps have this method changed to setBroker(host,port)
        this.host = host;
    }

    /**
     * @return The port.
     */
    public final int getPort()
    {
        //todo see getHost
        return port;
    }

    /**
     * @param port The port to set.
     */
    public final void setPort(int port)
    {
        //todo see setHost
        this.port = port;
    }

    /**
     * @return the virtualPath.
     */
    public final String getVirtualPath()
    {
        if (connectionDetails != null)
        {
            return connectionDetails.getVirtualHost();
        }
        else
        {
            return virtualPath;
        }
    }

    /**
     * @param path The virtualPath to set.
     */
    public final void setVirtualPath(String path)
    {
        if (connectionDetails != null)
        {
            connectionDetails.setVirtualHost(path);
        }

        virtualPath = path;
    }

    public Connection createConnection() throws JMSException
    {

        if(removeBURL == null) {
            removeBURL = new ThreadLocal<Boolean>();
            removeBURL.set(new Boolean(false));
        } else {

            if (removeBURL.get() == null) {
                removeBURL.set(new Boolean(false));
            }

        }

        // Requires permission java.util.PropertyPermission "qpid.dest_syntax", "write"
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                if (!removeBURL.get()) {
                    System.setProperty("qpid.dest_syntax", "BURL");
                } else {
                    System.clearProperty("qpid.dest_syntax");
                }
                return null; // nothing to return
            }
        });

        /**
         * In AMQP it is not possible to change the client ID. If one is not specified upon connection
         * construction, an id is generated automatically. Therefore we can always throw an exception.
         *
         * We can bypass this illegal state exception by setting the following system property. So we
         * are setting it up here to fix the issue https://wso2.org/jira/browse/MB-162.
         * */

        // Requires permission java.util.PropertyPermission "ignore_setclientID", "write";
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                System.setProperty(ClientProperties.IGNORE_SET_CLIENTID_PROP_NAME, "true");
                return null; // nothing to return
            }
        });

        try
        {
            if (connectionDetails != null)
            {

                // if connection needs to be encrypted using SSL, ConnectionURL must have ssl=true enabled. We first check that  option
                // here and if 'ssl=true' SSLConfiguration is generated.
                /*if(_connectionDetails.getSslEnabled() != null && Boolean.parseBoolean(_connectionDetails.getSslEnabled())){

                    _sslConfig = new SSLConfiguration();
                    _sslConfig.setKeystorePath(_connectionDetails.getKeyStore());
                    _sslConfig.setKeystorePassword(_connectionDetails.getKeyStorePassword());
                    _sslConfig.setTrustStorePath(_connectionDetails.getTrustStore());
                    _sslConfig.setTrustStorePassword(_connectionDetails.getTrustStorePassword());

                }*/
                AMQConnection amqConnection = new AMQConnection(connectionDetails, sslConfig);
                if (logger.isDebugEnabled()) {
                    Throwable t = new Throwable();
                    logger.debug("Setting connection listener to newly created connection from stack : " + displayStack(t).toString());
                }
                amqConnection.setConnectionListener(connectionListener);
                return amqConnection;
            }
            else
            {
                AMQConnection amqConnection = new AMQConnection(host, port, defaultUsername, defaultPassword, null,
                                                                virtualPath);
                amqConnection.setConnectionListener(connectionListener);
                return amqConnection;
            }
        }
        catch (Exception e)
        {
            JMSException jmse = new JMSException("Error creating connection: " + e.getMessage());
            jmse.setLinkedException(e);
            jmse.initCause(e);
            log.error("Error while creating connection", e);
            throw jmse;
        }


    }

    public Connection createConnection(String userName, String password) throws JMSException
    {
        return createConnection(userName, password, null);
    }

    /**
     * Create a JMSContext with default identity and Auto Acknowledge acknowledgement mode.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSContext createContext() {
        return createContext(JMSContext.AUTO_ACKNOWLEDGE);
    }

    /**
     * Create a JMSContext with default identity and specified acknowledgement mode.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSContext createContext(int sessionMode) {
        return createContext(defaultUsername, defaultPassword);
    }

    /**
     * Create a JMSContext with specified identity and Auto Acknowledge acknowledgement mode.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSContext createContext(String username, String password) {
        return createContext(username, password, JMSContext.AUTO_ACKNOWLEDGE);
    }

    /**
     * Create a JMSContext with specified identity and acknowledgement mode.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSContext createContext(String username, String password, int sessionMode) {
        if (sessionMode == JMSContext.SESSION_TRANSACTED ||
                sessionMode == JMSContext.AUTO_ACKNOWLEDGE ||
                sessionMode == JMSContext.CLIENT_ACKNOWLEDGE ||
                sessionMode == JMSContext.DUPS_OK_ACKNOWLEDGE) {
            try {
                defaultUsername = username;
                defaultPassword = password;
                AMQConnection connection = (AMQConnection) createConnection();
                return new AMQJMSContext(connection, sessionMode);
            } catch (JMSException e) {
                throw new JMSRuntimeException(e.getMessage(), null, e);
            }
        } else {
            throw new JMSRuntimeException("Invalid Session Mode: " + sessionMode);
        }
    }

    /**
     * Create an XAJMSContext with default identity.
     *
     * {@inheritDoc}
     */
    @Override
    public XAJMSContext createXAContext() {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Create an XAJMSContext with specified identity.
     *
     * {@inheritDoc}
     */
    @Override
    public XAJMSContext createXAContext(String userName, String password) {
        throw new JmsNotImplementedRuntimeException();
    }

    public Connection createConnection(String userName, String password, String id) throws JMSException
    {

        if (removeBURL == null) {
            removeBURL = new ThreadLocal<Boolean>();
            removeBURL.set(new Boolean(false));
        } else {
            try {
                if(null== removeBURL.get()){
                    removeBURL.set(new Boolean(false));
                }
            } catch (NullPointerException e) {
                removeBURL.set(new Boolean(false));
            }
        }
        if (!removeBURL.get()) {
            System.setProperty("qpid.dest_syntax", "BURL");
        } else {
            System.getProperties().remove("qpid.dest_syntax");
        }
        try
        {
            if (connectionDetails != null)
            {
                connectionDetails.setUsername(userName);
                connectionDetails.setPassword(password);
                connectionDetails.setClientName(id);

                AMQConnection amqConnection = new AMQConnection(connectionDetails, sslConfig);
                if (logger.isDebugEnabled()) {
                    Throwable t = new Throwable();
                    logger.debug("Setting connection listener while creating connection from stack : " + displayStack(t).toString());
                }
                amqConnection.setConnectionListener(connectionListener);
                return amqConnection;
            }
            else
            {
                AMQConnection amqConnection = new AMQConnection(host, port, userName, password, id, virtualPath);
                amqConnection.setConnectionListener(connectionListener);
                return amqConnection;
            }
        }
        catch (Exception e)
        {
            JMSException jmse = new JMSException("Error creating connection: " + e.getMessage());
            jmse.setLinkedException(e);
            jmse.initCause(e);
            throw jmse;
        }
    }

    public QueueConnection createQueueConnection() throws JMSException {
        if (removeBURL == null) {
            removeBURL = new ThreadLocal<Boolean>();
            removeBURL.set(new Boolean(true));
        } else {
            removeBURL.set(new Boolean(true));
        }
        return (QueueConnection) createConnection();
    }

    public QueueConnection createQueueConnection(String username, String password) throws JMSException {
        if (removeBURL == null) {
            removeBURL = new ThreadLocal<Boolean>();
            removeBURL.set(new Boolean(true));
        } else {
            removeBURL.set(new Boolean(true));
        }
        return (QueueConnection) createConnection(username, password);
    }

    public TopicConnection createTopicConnection() throws JMSException
    {
        return (TopicConnection) createConnection();
    }

    public TopicConnection createTopicConnection(String username, String password) throws JMSException
    {
        return (TopicConnection) createConnection(username, password);
    }


    public ConnectionURL getConnectionURL()
    {
        return connectionDetails;
    }

    public String getConnectionURLString()
    {
        return connectionDetails.toString();
    }


    public final void setConnectionURLString(String url) throws URLSyntaxException
    {
        connectionDetails = new AMQConnectionURL(url);
    }

    /**
     * JNDI interface to create objects from References.
     *
     * @param obj  The Reference from JNDI
     * @param name
     * @param ctx
     * @param env
     *
     * @return AMQConnection,AMQTopic,AMQQueue, or AMQConnectionFactory.
     *
     * @throws Exception
     */
    public Object getObjectInstance(Object obj, Name name, Context ctx, Hashtable env) throws Exception
    {
        if (obj instanceof Reference)
        {
            Reference ref = (Reference) obj;

            if (ref.getClassName().equals(AMQConnection.class.getName()))
            {
                RefAddr addr = ref.get(AMQConnection.class.getName());

                if (addr != null)
                {
                    AMQConnection amqConnection = new AMQConnection((String) addr.getContent());
                    amqConnection.setConnectionListener(connectionListener);
                    return amqConnection;
                }
            }

            if (ref.getClassName().equals(AMQQueue.class.getName()))
            {
                RefAddr addr = ref.get(AMQQueue.class.getName());

                if (addr != null)
                {
                    return new AMQQueue(new AMQBindingURL((String) addr.getContent()));
                }
            }

            if (ref.getClassName().equals(AMQTopic.class.getName()))
            {
                RefAddr addr = ref.get(AMQTopic.class.getName());

                if (addr != null)
                {
                    return new AMQTopic(new AMQBindingURL((String) addr.getContent()));
                }
            }

            if (ref.getClassName().equals(AMQConnectionFactory.class.getName()))
            {
                RefAddr addr = ref.get(AMQConnectionFactory.class.getName());

                if (addr != null)
                {
                    return new AMQConnectionFactory((String) addr.getContent());
                }
            }

        }
        return null;
    }


    public Reference getReference() throws NamingException
    {
        return new Reference(
                AMQConnectionFactory.class.getName(),
                new StringRefAddr(AMQConnectionFactory.class.getName(), connectionDetails.getURL()),
                             AMQConnectionFactory.class.getName(), null);          // factory location
    }

    // ---------------------------------------------------------------------------------------------------
    // the following methods are provided for XA compatibility
    // Those methods are only supported by 0_10 and above
    // ---------------------------------------------------------------------------------------------------

    /**
     * Creates a XAConnection with the default user identity.
     * <p> The XAConnection is created in stopped mode. No messages
     * will be delivered until the <code>Connection.start</code> method
     * is explicitly called.
     *
     * @return A newly created XAConnection
     * @throws JMSException         If creating the XAConnection fails due to some internal error.
     * @throws JMSSecurityException If client authentication fails due to an invalid user name or password.
     */
    public XAConnection createXAConnection() throws JMSException
    {
        try
        {
            return new XAConnectionImpl(connectionDetails, sslConfig, scheduledExecutor);
        }
        catch (Exception e)
        {
            JMSException jmse = new JMSException("Error creating connection: " + e.getMessage());
            jmse.setLinkedException(e);
            jmse.initCause(e);
            throw jmse;
        }
    }

    /**
     * Creates a XAConnection with the specified user identity.
     * <p> The XAConnection is created in stopped mode. No messages
     * will be delivered until the <code>Connection.start</code> method
     * is explicitly called.
     *
     * @param username the caller's user name
     * @param password the caller's password
     * @return A newly created XAConnection.
     * @throws JMSException         If creating the XAConnection fails due to some internal error.
     * @throws JMSSecurityException If client authentication fails due to an invalid user name or password.
     */
    public XAConnection createXAConnection(String username, String password) throws JMSException
    {
        if (connectionDetails != null)
        {
            connectionDetails.setUsername(username);
            connectionDetails.setPassword(password);
        }
        else
        {
            throw new JMSException("A URL must be specified to access XA connections");
        }
        return createXAConnection();
    }


    /**
     * Creates a XATopicConnection with the default user identity.
     * <p> The XATopicConnection is created in stopped mode. No messages
     * will be delivered until the <code>Connection.start</code> method
     * is explicitly called.
     *
     * @return A newly created XATopicConnection
     * @throws JMSException         If creating the XATopicConnection fails due to some internal error.
     * @throws JMSSecurityException If client authentication fails due to an invalid user name or password.
     */
    public XATopicConnection createXATopicConnection() throws JMSException
    {
        return (XATopicConnection) createXAConnection();
    }

    /**
     * Creates a XATopicConnection with the specified user identity.
     * <p> The XATopicConnection is created in stopped mode. No messages
     * will be delivered until the <code>Connection.start</code> method
     * is explicitly called.
     *
     * @param username the caller's user name
     * @param password the caller's password
     * @return A newly created XATopicConnection.
     * @throws JMSException         If creating the XATopicConnection fails due to some internal error.
     * @throws JMSSecurityException If client authentication fails due to an invalid user name or password.
     */
    public XATopicConnection createXATopicConnection(String username, String password) throws JMSException
    {
         return (XATopicConnection) createXAConnection(username, password);
    }

    /**
     * Creates a XAQueueConnection with the default user identity.
     * <p> The XAQueueConnection is created in stopped mode. No messages
     * will be delivered until the <code>Connection.start</code> method
     * is explicitly called.
     *
     * @return A newly created XAQueueConnection
     * @throws JMSException         If creating the XAQueueConnection fails due to some internal error.
     * @throws JMSSecurityException If client authentication fails due to an invalid user name or password.
     */
    public XAQueueConnection createXAQueueConnection() throws JMSException
    {
       return (XAQueueConnection) createXAConnection();
    }

    /**
     * Creates a XAQueueConnection with the specified user identity.
     * <p> The XAQueueConnection is created in stopped mode. No messages
     * will be delivered until the <code>Connection.start</code> method
     * is explicitly called.
     *
     * @param username the caller's user name
     * @param password the caller's password
     * @return A newly created XAQueueConnection.
     * @throws JMSException         If creating the XAQueueConnection fails due to some internal error.
     * @throws JMSSecurityException If client authentication fails due to an invalid user name or password.
     */
    public XAQueueConnection createXAQueueConnection(String username, String password) throws JMSException
    {
        return (XAQueueConnection) createXAConnection(username, password);
    }

    public ConnectionListener getConnectionListener() {
        return connectionListener;
    }

    public void setConnectionListener(ConnectionListener connectionListener) {
        this.connectionListener = connectionListener;
    }

    private StringWriter displayStack(Throwable t) {
         StringWriter errors = new StringWriter();
         t.printStackTrace(new PrintWriter(errors));

        return errors;
    }
}
