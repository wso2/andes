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

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Hashtable;


public class AMQConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory,
                                             ObjectFactory, Referenceable
{
    private static final Logger logger = LoggerFactory.getLogger(AMQConnectionFactory.class);

    private String _host;
    private int _port;
    private String _defaultUsername;
    private String _defaultPassword;
    private String _virtualPath;

    protected ConnectionURL _connectionDetails;
    protected SSLConfiguration _sslConfig;

    private ConnectionListener connectionListener = null;
    private ThreadLocal<Boolean> removeBURL = new ThreadLocal<Boolean>();

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
        _connectionDetails = new AMQConnectionURL(url);
    }

    /**
     * This constructor is never used!
     */
    public AMQConnectionFactory(ConnectionURL url)
    {
        _connectionDetails = url;
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
    public AMQConnectionFactory(String host, int port, String defaultUsername, String defaultPassword,
                                String virtualPath)
    {
        _host = host;
        _port = port;
        _defaultUsername = defaultUsername;
        _defaultPassword = defaultPassword;
        _virtualPath = virtualPath;

//todo when setting Host/Port has been resolved then we can use this otherwise those methods won't work with the following line.
//        _connectionDetails = new AMQConnectionURL(
//                ConnectionURL.AMQ_PROTOCOL + "://" +
//                        _defaultUsername + ":" + _defaultPassword + "@" +
//                        virtualPath + "?brokerlist='tcp://" + host + ":" + port + "'");
    }

    /**
     * @return The _defaultPassword.
     */
    public final String getDefaultPassword(String password)
    {
        if (_connectionDetails != null)
        {
            return _connectionDetails.getPassword();
        }
        else
        {
            return _defaultPassword;
        }
    }

    /**
     * @param password The _defaultPassword to set.
     */
    public final void setDefaultPassword(String password)
    {
        if (_connectionDetails != null)
        {
            _connectionDetails.setPassword(password);
        }
        _defaultPassword = password;
    }

    /**
     * Getter for SSLConfiguration
     *
     * @return SSLConfiguration if set, otherwise null
     */
    public final SSLConfiguration getSSLConfiguration()
    {
        return _sslConfig;
    }

    /**
     * Setter for SSLConfiguration
     *
     * @param sslConfig config to store
     */
    public final void setSSLConfiguration(SSLConfiguration sslConfig)
    {
        _sslConfig = sslConfig;
    }

    /**
     * @return The _defaultPassword.
     */
    public final String getDefaultUsername(String password)
    {
        if (_connectionDetails != null)
        {
            return _connectionDetails.getUsername();
        }
        else
        {
            return _defaultUsername;
        }
    }

    /**
     * @param username The _defaultUsername to set.
     */
    public final void setDefaultUsername(String username)
    {
        if (_connectionDetails != null)
        {
            _connectionDetails.setUsername(username);
        }
        _defaultUsername = username;
    }

    /**
     * @return The _host .
     */
    public final String getHost()
    {
        //todo this doesn't make sense in a multi broker URL as we have no current as that is done by AMQConnection
        return _host;
    }

    /**
     * @param host The _host to set.
     */
    public final void setHost(String host)
    {
        //todo if _connectionDetails is set then run _connectionDetails.addBrokerDetails()
        // Should perhaps have this method changed to setBroker(host,port)
        _host = host;
    }

    /**
     * @return _port The _port to set.
     */
    public final int getPort()
    {
        //todo see getHost
        return _port;
    }

    /**
     * @param port The port to set.
     */
    public final void setPort(int port)
    {
        //todo see setHost
        _port = port;
    }

    /**
     * @return he _virtualPath.
     */
    public final String getVirtualPath()
    {
        if (_connectionDetails != null)
        {
            return _connectionDetails.getVirtualHost();
        }
        else
        {
            return _virtualPath;
        }
    }

    /**
     * @param path The _virtualPath to set.
     */
    public final void setVirtualPath(String path)
    {
        if (_connectionDetails != null)
        {
            _connectionDetails.setVirtualHost(path);
        }

        _virtualPath = path;
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
            if (_connectionDetails != null)
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
                AMQConnection amqConnection = new AMQConnection(_connectionDetails, _sslConfig);
                if (logger.isDebugEnabled()) {
                    Throwable t = new Throwable();
                    logger.debug("Setting connection listener to newly created connection from stack : " + displayStack(t).toString());
                }
                amqConnection.setConnectionListener(connectionListener);
                return amqConnection;
            }
            else
            {
                AMQConnection amqConnection = new AMQConnection(_host, _port, _defaultUsername, _defaultPassword, null,
                                                                _virtualPath);
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
            if (_connectionDetails != null)
            {
                _connectionDetails.setUsername(userName);
                _connectionDetails.setPassword(password);
                _connectionDetails.setClientName(id);

                AMQConnection amqConnection = new AMQConnection(_connectionDetails, _sslConfig);
                if (logger.isDebugEnabled()) {
                    Throwable t = new Throwable();
                    logger.debug("Setting connection listener while creating connection from stack : " + displayStack(t).toString());
                }
                amqConnection.setConnectionListener(connectionListener);
                return amqConnection;
            }
            else
            {
                AMQConnection amqConnection = new AMQConnection(_host, _port, userName, password, id, _virtualPath);
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
        return _connectionDetails;
    }

    public String getConnectionURLString()
    {
        return _connectionDetails.toString();
    }


    public final void setConnectionURLString(String url) throws URLSyntaxException
    {
        _connectionDetails = new AMQConnectionURL(url);
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
                new StringRefAddr(AMQConnectionFactory.class.getName(), _connectionDetails.getURL()),
                             AMQConnectionFactory.class.getName(), null);          // factory location
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
