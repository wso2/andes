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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.jms.MessageProducer;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

/**
 * Implementation of JMSContext for JMS 2.0 Support.
 * One Context will have only one connection, but can have multiple sessions.
 */
public class AMQJMSContext extends Closeable implements AutoCloseable, JMSContext {

    private static final Logger logger = LoggerFactory.getLogger(AMQJMSContext.class);

    /**
     * The connection for the particular context.
     */
    private final AMQConnection connection;

    /**
     * The acknowledgement mode for the particular context.
     */
    private final int sessionMode;

    /**
     * Multiple Sessions(JMSContexts) are possible with a single connection.
     * The session variable refers to the session of this particular JMSContext.
     */
    private AMQSession session;
    /**
     * The total number of Sessions/JMSContext sharing the single connection created when creating
     * the initial JMSContext.
     */
    private AtomicInteger sessionCount;
    /**
     * Number of sessions when creating the initial JMSContext set to 1.
     */
    private static final AtomicInteger initialSessionCount = new AtomicInteger(1);

    /**
     * To specify whether the underlying connection used by this JMSContext
     * will be started automatically when a consumer is created.
     */
    private boolean autoStart = true;

    /**
     * Underlying MessageProducer instance shared by all JMSProducers of this JMSContext.
     */
    private MessageProducer sharedMessageProducer;

    /**
     * This is the constructor called when creating the initial JMSContext with a
     * particular connection.
     * This is called only from the ConnectionFactory.
     *
     * @param connection  underlying connection for this JMSContext
     * @param sessionMode session Mode for this JMSContext
     */
    public AMQJMSContext(AMQConnection connection, int sessionMode) {
        this(connection, sessionMode, initialSessionCount);
        if (logger.isDebugEnabled()) {
            logger.debug("Created JMSContext with Session Mode: {}", sessionMode);
        }
    }

    /**
     * The constructor called with subsequent createContext() calls on the initial JMSContext.
     *
     * @param connection   underlying connection used to create initial JMSContext
     * @param sessionMode  session mode for the new context to be created
     * @param sessionCount number of sessions/contexts relevant to this connection
     */
    private AMQJMSContext(AMQConnection connection, int sessionMode, AtomicInteger sessionCount) {
        this.connection = connection;
        this.sessionMode = sessionMode;
        this.sessionCount = sessionCount;
    }

    /**
     * Method to create a new JMSContext with the same connection, but different session.
     *
     * {@inheritDoc}
     */
    @Override
    public synchronized JMSContext createContext(int sessionMode) {
        if (sessionCount.get() != 0) {
            sessionCount.incrementAndGet();
            if (logger.isDebugEnabled()) {
                logger.debug("Created new Context with session mode: {}", sessionMode);
            }
            return new AMQJMSContext(connection, sessionMode, sessionCount);
        } else {
            throw new JMSRuntimeException("The Connection is Closed.");
        }
    }

    /**
     * Method to create a JMSProducer for this JMSContext.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer createProducer() {
        return new BasicJMSProducer((AMQSession_0_8) getSession(),
                (BasicMessageProducer_0_8) getSharedMessageProducer());
    }

    /**
     * Method to retrieve the client ID of the underlying connection.
     *
     * {@inheritDoc}
     */
    @Override
    public String getClientID() {
        try {
            return connection.getClientID();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to set the client ID of the underlying connection.
     *
     * {@inheritDoc}
     */
    @Override
    public void setClientID(String clientID) {
        try {
            connection.setClientID(clientID);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to retrieve the connection metadata of the underlying connection.
     *
     * {@inheritDoc}
     */
    @Override
    public ConnectionMetaData getMetaData() {
        try {
            return connection.getMetaData();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to retrieve the exception listener of the underlying connection.
     *
     * {@inheritDoc}
     */
    @Override
    public ExceptionListener getExceptionListener() {
        try {
            return connection.getExceptionListener();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to set the exception listener of the underlying connection.
     *
     * {@inheritDoc}
     */
    @Override
    public void setExceptionListener(ExceptionListener listener) {
        try {
            connection.setExceptionListener(listener);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to start/restart delivery of incoming messages by the underlying connection.
     *
     * {@inheritDoc}
     */
    @Override
    public void start() {
        try {
            connection.start();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to temporarily stop delivery of incoming messages by the underlying connection.
     *
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        try {
            connection.stop();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to specify whether the underlying connection is started automatically when a consumer
     * is created.
     *
     * {@inheritDoc}
     */
    @Override
    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    /**
     * Method to retrieve whether the underlying connection would be started automatically when a
     * consumer is created.
     *
     * {@inheritDoc}
     */
    @Override
    public boolean getAutoStart() {
        return autoStart;
    }

    /**
     * Method to close the JMSContext.
     *
     * {@inheritDoc}
     */
    @Override
    public synchronized void close() {
        boolean isExceptionThrown = false;
        try {
            if (session != null) {
                session.close();
            }
        } catch (JMSException e) {
            isExceptionThrown = true;
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        } finally {
            try {
                if (sessionCount.decrementAndGet() == 0) {
                    connection.close();
                }
            } catch (JMSException e) {
                if (!isExceptionThrown) {
                    throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
                }
            }
        }
    }

    /**
     * Method to create a BytesMessage.
     *
     * {@inheritDoc}
     */
    @Override
    public BytesMessage createBytesMessage() {
        try {
            return getSession().createBytesMessage();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a MapMessage.
     *
     * {@inheritDoc}
     */
    @Override
    public MapMessage createMapMessage() {
        try {
            return getSession().createMapMessage();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a Message.
     *
     * {@inheritDoc}
     */
    @Override
    public Message createMessage() {
        try {
            //TODO: Should create a Message object:currently creates a BytesMessage object
            return getSession().createMessage();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create an ObjectMessage.
     *
     * {@inheritDoc}
     */
    @Override
    public ObjectMessage createObjectMessage() {
        try {
            return getSession().createObjectMessage();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create an initialized ObjectMessage.
     *
     * {@inheritDoc}
     */
    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        try {
            return getSession().createObjectMessage(object);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a StreamMessage.
     *
     * {@inheritDoc}
     */
    @Override
    public StreamMessage createStreamMessage() {
        try {
            return getSession().createStreamMessage();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a TextMessage.
     *
     * {@inheritDoc}
     */
    @Override
    public TextMessage createTextMessage() {
        try {
            return getSession().createTextMessage();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create an initialized TextMessage.
     *
     * {@inheritDoc}
     */
    @Override
    public TextMessage createTextMessage(String text) {
        try {
            return getSession().createTextMessage(text);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to retrieve whether the underlying session is in transacted mode.
     *
     * {@inheritDoc}
     */
    @Override
    public boolean getTransacted() {
        return sessionMode == Session.SESSION_TRANSACTED;
    }

    /**
     * Method to retrieve the session mode of the underlying session.
     *
     * {@inheritDoc}
     */
    @Override
    public int getSessionMode() {
        return sessionMode;
    }

    /**
     * Method to commit all messages done in this transaction and release any locks held.
     *
     * {@inheritDoc}
     */
    @Override
    public void commit() {
        try {
            getSession().commit();
        } catch (IllegalStateException e) {
            throw new IllegalStateRuntimeException(e.getMessage(), e.getErrorCode(), e);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to rollback any messages done in this transaction and release any locks held.
     *
     * {@inheritDoc}
     */
    @Override
    public void rollback() {
        try {
            getSession().rollback();
        } catch (IllegalStateException e) {
            throw new IllegalStateRuntimeException(e.getMessage(), e.getErrorCode(), e);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to stop message delivery in the underlying session and restart delivery with the
     * oldest unacknowledged message.
     *
     * {@inheritDoc}
     */
    @Override
    public void recover() {
        try {
            getSession().recover();
        } catch (IllegalStateException e) {
            throw new IllegalStateRuntimeException(e.getMessage(), e.getErrorCode(), e);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a consumer for the specified destination.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSConsumer createConsumer(Destination destination) {
        AMQSession_0_8 currentSession = (AMQSession_0_8) getSession();
        try {
            startConnectionIfRequired();
            BasicJMSConsumer jmsConsumer =
                    new BasicJMSConsumer(currentSession.createConsumer(destination));
            if (logger.isDebugEnabled()) {
                logger.debug("Created Consumer for {}", destination);
            }
            return jmsConsumer;
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a consumer for the specified destination with the specified selector.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector) {
        AMQSession_0_8 currentSession = (AMQSession_0_8) getSession();
        try {
            startConnectionIfRequired();
            BasicJMSConsumer jmsConsumer = new BasicJMSConsumer(
                    currentSession.createConsumer(destination, messageSelector));
            if (logger.isDebugEnabled()) {
                logger.debug("Created Consumer for: {} with Selector: {}",
                        new Object[]{destination, messageSelector});
            }
            return jmsConsumer;
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a consumer for the specified destination with the specified selector and
     * noLocal parameter.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector,
                                      boolean noLocal) {
        AMQSession_0_8 currentSession = (AMQSession_0_8) getSession();
        try {
            startConnectionIfRequired();
            BasicJMSConsumer jmsConsumer = new BasicJMSConsumer(
                    currentSession.createConsumer(destination, messageSelector, noLocal));
            if (logger.isDebugEnabled()) {
                logger.debug("Created Consumer for: {} with Selector: {} and noLocal: {}",
                        new Object[]{destination, messageSelector, noLocal});
            }
            return jmsConsumer;
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a queue which could be referred to with the specified name.
     *
     * {@inheritDoc}
     */
    @Override
    public Queue createQueue(String queueName) {
        try {
            Queue queue = getSession().createQueue(queueName);
            if (logger.isDebugEnabled()) {
                logger.debug("Created Queue: {}", queueName);
            }
            return queue;
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a topic which could be referred to with the specified name.
     *
     * {@inheritDoc}
     */
    @Override
    public Topic createTopic(String topicName) {
        try {
            Topic topic = getSession().createTopic(topicName);
            if (logger.isDebugEnabled()) {
                logger.debug("Created Topic: {}", topicName);
            }
            return topic;
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create an unshared durable subscription of the given name on the specified topic
     * and create a consumer on that subscription.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        AMQSession_0_8 currentSession = (AMQSession_0_8) getSession();
        try {
            startConnectionIfRequired();
            BasicJMSConsumer jmsConsumer =
                    new BasicJMSConsumer(currentSession.createDurableConsumer(topic, name));
            if (logger.isDebugEnabled()) {
                logger.debug("Created Durable Consumer for: {}", topic);
            }
            return jmsConsumer;
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create an unshared durable subscription of the given name on the specified topic
     * with the specified message selector and noLocal parameter and create a consumer on that
     * subscription.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector,
                                             boolean noLocal) {
        AMQSession_0_8 currentSession = (AMQSession_0_8) getSession();
        try {
            startConnectionIfRequired();
            BasicJMSConsumer jmsConsumer = new BasicJMSConsumer(
                    currentSession.createDurableConsumer(topic, name, messageSelector, noLocal));
            if (logger.isDebugEnabled()) {
                logger.debug("Created Durable Consumer for: {} with Selector: {} ",
                        new Object[]{topic, messageSelector});
            }
            return jmsConsumer;
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a shared durable subscription of the given name on the specified topic
     * and create a consumer on that durable subscription.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Method to create a shared durable subscription of the given name on the specified topic
     * with the specified message selector and create a consumer on that durable subscription.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name,
                                                   String messageSelector) {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Method to create a shared non-durable subscription of the given name on the specified topic
     * and create a consumer on that subscription.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Method to create a shared non-durable subscription with the given name on the specified topic
     * with the specified message selector and create a consumer on that subscription.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName,
                                            String messageSelector) {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Method to create a QueueBrowser to peek at the messages on the specified queue.
     *
     * {@inheritDoc}
     */
    @Override
    public QueueBrowser createBrowser(Queue queue) {
        try {
            QueueBrowser queueBrowser = getSession().createBrowser(queue);
            if (logger.isDebugEnabled()) {
                logger.debug("Created Browser for: {}", queue);
            }
            return queueBrowser;
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a QueueBrowser to peek at the messages with the specified message selector
     * on the specified queue.
     *
     * {@inheritDoc}
     */
    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        try {
            QueueBrowser queueBrowser = getSession().createBrowser(queue, messageSelector);
            if (logger.isDebugEnabled()) {
                logger.debug("Created Browser for: {} with Selector: {}",
                        new Object[]{queue, messageSelector});
            }
            return queueBrowser;
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a temporary queue.
     *
     * {@inheritDoc}
     */
    @Override
    public TemporaryQueue createTemporaryQueue() {
        try {
            TemporaryQueue temporaryQueue = getSession().createTemporaryQueue();
            logger.debug("Created Temporary Queue");
            return temporaryQueue;
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to create a temporary topic.
     *
     * {@inheritDoc}
     */
    @Override
    public TemporaryTopic createTemporaryTopic() {
        try {
            TemporaryTopic temporaryTopic = getSession().createTemporaryTopic();
            logger.debug("Created Temporary Topic");
            return temporaryTopic;
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to unsubscribe a durable subscription.
     *
     * {@inheritDoc}
     * TODO: Should throw InvalidDestinationRuntimeException if invalid name.
     */
    @Override
    public void unsubscribe(String name) {
        try {
            getSession().unsubscribe(name);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to acknowledge all messages consumed by the underlying session.
     *
     * {@inheritDoc}
     */
    @Override
    public void acknowledge() {
        try {
            getSession().acknowledge();
        } catch (IllegalStateException e) {
            throw new IllegalStateRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to retrieve the session.
     * Creates the session if not already created.
     *
     * @return session currently applicable session
     */
    private AMQSession getSession() {
        if (session == null) {
            try {
                session = (AMQSession) connection.createSession(getSessionMode());
            } catch (JMSException e) {
                throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
            }
        }
        return session;
    }

    /**
     * Method to retrieve the single message producer of the JMSContext.
     * Creates message producer if not already created.
     *
     * @return MessageProducer the shared MessageProducer
     */
    private MessageProducer getSharedMessageProducer() {
        if (sharedMessageProducer == null) {
            try {
                sharedMessageProducer = session.createProducer(null);
            } catch (java.lang.IllegalStateException e) {
                throw new IllegalStateRuntimeException(e.getMessage(), null, e);
            } catch (JMSException e) {
                throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
            }
        }
        return sharedMessageProducer;
    }

    /**
     * According to the spec: connection needs to be started automatically when a consumer is
     * created if autoStart is set to true.
     */
    private void startConnectionIfRequired() {
        if (getAutoStart()) {
            try {
                if (!connection._started) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Connection Started for JMSContext");
                    }
                    connection.start();
                }
            } catch (JMSException e) {
                throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
            }
        }
    }
}
