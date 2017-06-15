/*
 * Copyright (c) 2017, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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

package org.wso2.andes;

import org.wso2.andes.client.JmsNotImplementedRuntimeException;
import org.wso2.andes.client.XASession_9_1;

import java.io.Serializable;
import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

/**
 * Forward all methods to the XA session object except the close method. When the close is called the underline
 * XA session is not closed. But the method invocations will throw an IllegalStateException after session close.
 */
public class XaJmsSession implements Session, QueueSession, TopicSession {

    /**
     * Wrapped session object
     */
    private final XASession_9_1 session;

    /**
     * We can use a non atomic variable since the JMS sessions are not thread safe
     */
    private boolean closed = false;

    public XaJmsSession(XASession_9_1 session) {
        this.session = session;
    }

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        checkNotClosed();
        return session.createBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        checkNotClosed();
        return session.createMapMessage();
    }

    @Override
    public Message createMessage() throws JMSException {
        checkNotClosed();
        return session.createMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        checkNotClosed();
        return session.createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable serializable) throws JMSException {
        checkNotClosed();
        return session.createObjectMessage(serializable);
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        checkNotClosed();
        return session.createStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        checkNotClosed();
        return session.createTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String s) throws JMSException {
        checkNotClosed();
        return session.createTextMessage(s);
    }

    @Override
    public boolean getTransacted() throws JMSException {
        checkNotClosed();
        return session.getTransacted();
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        checkNotClosed();
        return session.getAcknowledgeMode();
    }

    @Override
    public void commit() throws JMSException {
        checkNotClosed();
        session.commit();
    }

    @Override
    public void rollback() throws JMSException {
        checkNotClosed();
        session.rollback();
    }

    @Override
    public void close() throws JMSException {
        closed = true;
    }

    @Override
    public void recover() throws JMSException {
        checkNotClosed();
        session.recover();
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkNotClosed();
        return session.getMessageListener();
    }

    @Override
    public void setMessageListener(MessageListener messageListener) throws JMSException {
        checkNotClosed();
        session.setMessageListener(messageListener);
    }

    @Override
    public void run() {
        session.run();
    }

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        checkNotClosed();
        return session.createProducer(destination);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        checkNotClosed();
        return session.createConsumer(destination);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String s) throws JMSException {
        checkNotClosed();
        return session.createConsumer(destination, s);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String s, boolean b) throws JMSException {
        checkNotClosed();
        return session.createConsumer(destination,s,b);
    }

    @Override
    public Queue createQueue(String s) throws JMSException {
        checkNotClosed();
        return session.createQueue(s);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        checkNotClosed();
        return session.createReceiver(queue);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String s) throws JMSException {
        checkNotClosed();
        return session.createReceiver(queue,s);
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        checkNotClosed();
        return session.createSender(queue);
    }

    @Override
    public Topic createTopic(String s) throws JMSException {
        checkNotClosed();
        return session.createTopic(s);
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        checkNotClosed();
        return session.createSubscriber(topic);
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic, String s, boolean b) throws JMSException {
        checkNotClosed();
        return session.createSubscriber(topic, s, b);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String s) throws JMSException {
        checkNotClosed();
        return session.createDurableSubscriber(topic, s);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String s, String s1, boolean b) throws JMSException {
        checkNotClosed();
        return session.createDurableSubscriber(topic, s, s1, b);
    }

    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        checkNotClosed();
        return session.createPublisher(topic);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        checkNotClosed();
        return session.createBrowser(queue);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String s) throws JMSException {
        checkNotClosed();
        return session.createBrowser(queue, s);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        checkNotClosed();
        return session.createTemporaryQueue();
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        checkNotClosed();
        return session.createTemporaryTopic();
    }

    @Override
    public void unsubscribe(String s) throws JMSException {
        checkNotClosed();
        session.unsubscribe(s);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName)
            throws JMSException {
        throw new JmsNotImplementedRuntimeException();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName,
                                                String messageSelector) throws JMSException {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
        checkNotClosed();
        return session.createDurableConsumer(topic, name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector,
                                                 boolean noLocal) throws JMSException {
        checkNotClosed();
        return session.createDurableConsumer(topic, name, messageSelector, noLocal);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name)
            throws JMSException {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name,
                                                       String messageSelector) throws JMSException {
        throw new JmsNotImplementedRuntimeException();
    }

    private void checkNotClosed() throws JMSException {
        if (closed) {
            throw new IllegalStateException("Session has been closed");
        }
    }
}
