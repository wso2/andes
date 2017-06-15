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

import javax.jms.JMSConsumer;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

/**
 * andes Implementation of JMSConsumer.
 */
public class BasicJMSConsumer extends Closeable implements JMSConsumer {

    /**
     * To refer to the underlying MessageConsumer of this JMSConsumer.
     */
    private final MessageConsumer messageConsumer;

    /**
     * Create a new BasicJMSConsumer instance.
     *
     * @param messageConsumer The underlying MessageConsumer for this JMSConsumer
     */
    public BasicJMSConsumer(MessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    /**
     * Method to retrieve the JMSConsumer's message selector expression.
     *
     * {@inheritDoc}
     */
    @Override
    public String getMessageSelector() throws JMSRuntimeException {
        try {
            return messageConsumer.getMessageSelector();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to retrieve the JMSConsumer's MessageListener.
     *
     * {@inheritDoc}
     */
    @Override
    public MessageListener getMessageListener() throws JMSRuntimeException {
        try {
            return messageConsumer.getMessageListener();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to set the JMSConsumer's MessageListener.
     *
     * {@inheritDoc}
     */
    @Override
    public void setMessageListener(MessageListener messageListener) throws JMSRuntimeException {
        try {
            messageConsumer.setMessageListener(messageListener);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to receive the next message produced for this JMSConsumer.
     *
     * {@inheritDoc}
     */
    @Override
    public Message receive() throws JMSRuntimeException {
        try {
            return messageConsumer.receive();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to receive the next message that arrives within the specified timeout.
     *
     * {@inheritDoc}
     */
    @Override
    public Message receive(long timeout) throws JMSRuntimeException {
        try {
            return messageConsumer.receive(timeout);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to receive the next message if one is immediately available.
     *
     * {@inheritDoc}
     */
    @Override
    public Message receiveNoWait() {
        try {
            return messageConsumer.receiveNoWait();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to close the JMSConsumer.
     *
     * {@inheritDoc}
     */
    @Override
    public void close() throws JMSRuntimeException {
        _closed.set(true);
        try {
            messageConsumer.close();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to receive the next message produced for this JMSConsumer and return the body
     * as an object of the specified type.
     *
     * {@inheritDoc}
     */
    @Override
    public <T> T receiveBody(Class<T> c) throws JMSRuntimeException {
        try {
            Message message = messageConsumer.receive();
            if (message == null) {
                return null;
            } else {
                return message.getBody(c);
            }
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to receive the next message that arrives within the specified timeout and return
     * the body as an object of the specified type.
     *
     * {@inheritDoc}
     */
    @Override
    public <T> T receiveBody(Class<T> c, long timeout) throws JMSRuntimeException {
        try {
            Message message = messageConsumer.receive(timeout);
            if (message == null) {
                return null;
            } else {
                return message.getBody(c);
            }

        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Method to receive the next message if one is immediately available and return the body as
     * an object of the specified type.
     *
     * {@inheritDoc}
     */
    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
        try {
            Message message = messageConsumer.receiveNoWait();
            if (message == null) {
                return null;
            } else {
                return message.getBody(c);
            }

        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }
}
