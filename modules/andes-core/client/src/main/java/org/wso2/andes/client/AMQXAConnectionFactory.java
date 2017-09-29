/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.andes.client;

import org.wso2.andes.url.AMQBindingURL;
import org.wso2.andes.url.URLSyntaxException;

import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnection;
import javax.jms.XATopicConnectionFactory;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import java.util.Hashtable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This class contains XA Connection Factory
 * To initialize a XA Connection Factory,
 * In the jndi file, we should define connection Factory with xaconnectionfactory prefix
 * Example : xaconnectionfactory.QueueConnectionFactory = amqp://admin:admin@clientID/carbon?brokerlist='tcp://localhost:5675'
 */
public class AMQXAConnectionFactory extends AMQConnectionFactory implements XATopicConnectionFactory,
        XAQueueConnectionFactory, XAConnectionFactory {

    /**
     * Scheduled executor share by the XAConnectionImpl objects
     */
    private ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();


    public AMQXAConnectionFactory(String url) throws URLSyntaxException {
        super(url);
    }

    public AMQXAConnectionFactory() {

    }

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
            return new XAConnectionImpl(_connectionDetails, _sslConfig, scheduledExecutor);
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
        if (_connectionDetails != null)
        {
            _connectionDetails.setUsername(username);
            _connectionDetails.setPassword(password);
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

    /**
     * Retrieves the Reference of this object (AMQXAConnectionFactory).
     *
     * @return The non-null Reference of this object.
     * @exception NamingException If a naming exception was encountered
     *         while retrieving the reference.
     */
    public Reference getReference() throws NamingException
    {
        return new Reference(
                AMQXAConnectionFactory.class.getName(),
                new StringRefAddr(AMQXAConnectionFactory.class.getName(), _connectionDetails.getURL()),
                AMQXAConnectionFactory.class.getName(), null);          // factory location
    }


    /**
     * JNDI interface to create objects from References.
     *
     * @param obj  The Reference from JNDI
     * @param name JNDI name
     * @param ctx  Context
     * @param env  Environment
     *
     * @return XAConnectionImpl,AMQTopic,AMQQueue, or AMQXAConnectionFactory.
     *
     * @throws Exception
     */
    public Object getObjectInstance(Object obj, Name name, Context ctx, Hashtable env) throws Exception
    {
        if (obj instanceof Reference)
        {
            Reference ref = (Reference) obj;

            if (ref.getClassName().equals(XAConnectionImpl.class.getName()))
            {
                RefAddr addr = ref.get(XAConnectionImpl.class.getName());

                if (addr != null)
                {
                    XAConnectionImpl xaConnection =
                            new XAConnectionImpl(new AMQConnectionURL((String) addr.getContent()), null,
                                    Executors.newSingleThreadScheduledExecutor());
                    xaConnection.setConnectionListener(getConnectionListener());
                    return xaConnection;
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

            if (ref.getClassName().equals(AMQXAConnectionFactory.class.getName()))
            {
                RefAddr addr = ref.get(AMQXAConnectionFactory.class.getName());

                if (addr != null)
                {
                    return new AMQXAConnectionFactory((String) addr.getContent());
                }
            }

        }
        return null;
    }
}
