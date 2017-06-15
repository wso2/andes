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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

/**
 * andes Implementation of JMSProducer.
 * TODO: use Delivery Delay, DisableMessageID, etc.
 */
public class BasicJMSProducer extends Closeable implements JMSProducer {

    /**
     * To refer to the underlying Session of the JMSContext that creates this JMSProducer.
     */
    private final AMQSession_0_8 session;

    /**
     * To refer to the underlying MessageProducer common to the JMSContext creating this producer.
     */
    private final BasicMessageProducer_0_8 messageProducer;

    /**
     * To indicate whether MessageIDs are disabled.
     */
    private boolean disableMessageID = false;
    /**
     * To indicate whether MessageTimestamps are disabled.
     */
    private boolean disableMessageTimestamp = false;
    /**
     * To refer to the CompletionListener of this JMSProducer used to send messages asynchronously.
     */
    private CompletionListener completionListener = null;
    /**
     * To maintain the delivery mode of the messages sent using this JMSProducer.
     */
    private int deliveryMode = DeliveryMode.PERSISTENT;
    /**
     * To maintain the priority of the messages sent using this JMSProducer.
     */
    private int priority = Message.DEFAULT_PRIORITY;
    /**
     * To maintain the delivery delay of the messages sent using this JMSProducer.
     */
    private long deliveryDelay = Message.DEFAULT_DELIVERY_DELAY;
    /**
     * To maintain the time to live of the messages sent using this JMSProducer.
     */
    private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    /**
     * To maintain the JMSCorrelationID header value of the messages sent using this JMSProducer.
     */
    private String jmsCorrelationID;
    /**
     * To maintain the JMSType header value of the messages sent using this JMSProducer.
     */
    private String jmsType;
    /**
     * To maintain the JMSReplyTo header value of the messages sent using this JMSProducer.
     */
    private Destination jmsReplyTo;
    /**
     * To maintain the JMSCorrelationID header value as an array of bytes.
     */
    private byte[] jmsCorrelationIDAsBytes;
    /**
     * To maintain message properties.
     */
    private Map<String, Object> messageProperties = new HashMap<String, Object>();

    /**
     * Create a new BasicJMSProducer instance.
     *
     * @param session           the underlying session of the JMSContext creating the JMSProducer
     * @param messageProducer   the underlying MessageProducer common to the JMSContext
     */
    public BasicJMSProducer(AMQSession_0_8 session, BasicMessageProducer_0_8 messageProducer) {
        this.session = session;
        this.messageProducer = messageProducer;
    }

    /**
     * Method to send a Message.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer send(Destination destination, Message message) {
        try {
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        return this;
    }

    /**
     * Method to send a TextMessage with specified string content.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer send(Destination destination, String body) {
        try {
            TextMessage message = session.createTextMessage(body);
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        return this;
    }

    /**
     * Method to send a MapMessage with specified map content.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer send(Destination destination, Map<String, Object> body) {
        try {
            MapMessage message = session.createMapMessage();
            for (Map.Entry<String, Object> mapEntry : body.entrySet()) {
                message.setObject(mapEntry.getKey(), mapEntry.getValue());
            }
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        return this;
    }

    /**
     * Method to send a BytesMessage with specified bytes content.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer send(Destination destination, byte[] body) {
        try {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(body);
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        return this;
    }

    /**
     * Method to send an ObjectMessage with specified content.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer send(Destination destination, Serializable body) {
        try {
            ObjectMessage message = session.createObjectMessage();
            message.setObject(body);
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        return this;
    }

    /**
     * Method to specify whether message IDs may be disabled for messages sent using
     * this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setDisableMessageID(boolean value) {
        disableMessageID = value;
        return this;
    }

    /**
     * Method to identify whether message IDs have been disabled.
     *
     * {@inheritDoc}
     */
    @Override
    public boolean getDisableMessageID() {
        return disableMessageID;
    }

    /**
     * Method to specify whether message timestamps may be disabled for messages sent using
     * this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setDisableMessageTimestamp(boolean value) {
        disableMessageTimestamp = value;
        return this;
    }

    /**
     * Method to identify whether message timestamps have been disabled.
     *
     * {@inheritDoc}
     */
    @Override
    public boolean getDisableMessageTimestamp() {
        return disableMessageTimestamp;
    }

    /**
     * Method to set the delivery mode of messages sent using this JMSProducer to the
     * specified mode.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setDeliveryMode(int deliveryMode) {
        if (deliveryMode == DeliveryMode.NON_PERSISTENT) {
            this.deliveryMode = deliveryMode;
        } else if (deliveryMode != DeliveryMode.PERSISTENT) {
            throw new JMSRuntimeException("Invalid Delivery Mode " + deliveryMode);
        }
        return this;
    }

    /**
     * Method to retrieve the delivery mode of messages sent using this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public int getDeliveryMode() {
        return deliveryMode;
    }

    /**
     * Method to set priority level on messages sent by this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setPriority(int priority) {
        if (priority >= 0 && priority <= 9) {
            this.priority = priority;
        } else {
            throw new JMSRuntimeException("Invalid Priority Level: " + priority
                    + ", Valid Range: 0-9");
        }
        return this;
    }

    /**
     * Method to retrieve the priority of messages that are sent using this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public int getPriority() {
        return priority;
    }

    /**
     * Method to set time to live of messages sent using this JMSProducer to the specified value.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    /**
     * Method to retrieve the time to live of messages sent using this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public long getTimeToLive() {
        return timeToLive;
    }

    /**
     * Method to set the delivery delay on this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setDeliveryDelay(long deliveryDelay) {
        this.deliveryDelay = deliveryDelay;
        return this;
    }

    /**
     * Method to retrieve the delivery delay set on this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public long getDeliveryDelay() {
        return deliveryDelay;
    }

    /**
     * Method to specify whether subsequent sends using this producer should be asynchronous,
     * setting the specified completion listener.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setAsync(CompletionListener completionListener) {
        this.completionListener = completionListener;
        return this;
    }

    /**
     * Method to retrieve the completion listener set if subsequent calls on the JMSProducer are
     * configured to be asynchronous.
     *
     * {@inheritDoc}
     */
    @Override
    public CompletionListener getAsync() {
        return completionListener;
    }

    /**
     * Method to specify that the messages sent using this JMSProducer will have the specified
     * property set to the specified boolean value.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setProperty(String name, boolean value) {
        return setMessageProperty(name, value);
    }

    /**
     * Method to specify that the messages sent using this JMSProducer will have the specified
     * property set to the specified byte value.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setProperty(String name, byte value) {
        return setMessageProperty(name, value);
    }

    /**
     * Method to specify that the messages sent using this JMSProducer will have the specified
     * property set to the specified short value.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setProperty(String name, short value) {
        return setMessageProperty(name, value);
    }

    /**
     * Method to specify that the messages sent using this JMSProducer will have the specified
     * property set to the specified int value.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setProperty(String name, int value) {
        return setMessageProperty(name, value);
    }

    /**
     * Method to specify that the messages sent using this JMSProducer will have the specified
     * property set to the specified long value.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setProperty(String name, long value) {
        return setMessageProperty(name, value);
    }

    /**
     * Method to specify that the messages sent using this JMSProducer will have the specified
     * property set to the specified float value.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setProperty(String name, float value) {
        return setMessageProperty(name, value);
    }

    /**
     * Method to specify that the messages sent using this JMSProducer will have the specified
     * property set to the specified double value.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setProperty(String name, double value) {
        return setMessageProperty(name, value);
    }

    /**
     * Method to specify that the messages sent using this JMSProducer will have the specified
     * property set to the specified String value.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setProperty(String name, String value) {
        return setMessageProperty(name, value);
    }

    /**
     * Method to specify that the messages sent using this JMSProducer will have the specified
     * property set to the specified Java Object value.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setProperty(String name, Object value) {
        return setMessageProperty(name, value);
    }

    /**
     * Method to clear any message properties set on this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer clearProperties() {
        messageProperties.clear();
        return this;
    }

    /**
     * Method to retrieve if a property with the specified name has been set on this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public boolean propertyExists(String name) {
        return messageProperties.containsKey(name);
    }

    /**
     * Method to retrieve the message property with the specified name, set on this JMSProducer,
     * converted to a boolean.
     *
     * {@inheritDoc}
     */
    @Override
    public boolean getBooleanProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Method to retrieve the message property with the specified name, set on this JMSProducer,
     * converted to a byte.
     *
     * {@inheritDoc}
     */
    @Override
    public byte getByteProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Method to retrieve the message property with the specified name, set on this JMSProducer,
     * converted to a short.
     *
     * {@inheritDoc}
     */
    @Override
    public short getShortProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Method to retrieve the message property with the specified name, set on this JMSProducer,
     * converted to an int.
     *
     * {@inheritDoc}
     */
    @Override
    public int getIntProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Method to retrieve the message property with the specified name, set on this JMSProducer,
     * converted to a long.
     *
     * {@inheritDoc}
     */
    @Override
    public long getLongProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Method to retrieve the message property with the specified name, set on this JMSProducer,
     * converted to a float.
     *
     * {@inheritDoc}
     */
    @Override
    public float getFloatProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Method to retrieve the message property with the specified name, set on this JMSProducer,
     * converted to a double.
     *
     * {@inheritDoc}
     */
    @Override
    public double getDoubleProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Method to retrieve the message property with the specified name, set on this JMSProducer,
     * converted to a String.
     *
     * {@inheritDoc}
     */
    @Override
    public String getStringProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    /**
     * Method to retrieve the message property with the specified name, set on this JMSProducer,
     * converted to objectified format.
     *
     * {@inheritDoc}
     */
    @Override
    public Object getObjectProperty(String name) {
        return messageProperties.get(name);
    }

    /**
     * Method to retrieve a set of the names of the message properties that have been set on
     * this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public Set<String> getPropertyNames() {
        return messageProperties.keySet();
    }

    /**
     * Method to set the JMSCorrelationID header value of the messages sent using this JMSProducer
     * to the specified correlationID given as an array of bytes.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID) {
        jmsCorrelationIDAsBytes = correlationID;
        return this;
    }

    /**
     * Method to retrieve the JMSCorrelationID header value that has been set on this JMSProducer
     * as an array of bytes.
     *
     * {@inheritDoc}
     */
    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        return jmsCorrelationIDAsBytes;
    }

    /**
     * Method to set the JMSCorrelationID header value of the messages sent using this JMSProducer
     * to the specified correlationID given as a string.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setJMSCorrelationID(String correlationID) {
        jmsCorrelationID = correlationID;
        return this;
    }

    /**
     * Method to retrieve the JMSCorrelationID header value that has been set on this JMSProducer
     * as a string.
     *
     * {@inheritDoc}
     */
    @Override
    public String getJMSCorrelationID() {
        return jmsCorrelationID;
    }

    /**
     * Method to set the JMSType header value of the messages sent using this JMSProducer
     * to the specified type.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setJMSType(String type) {
        jmsType = type;
        return this;
    }

    /**
     * Method to retrieve the JMSType header value that has been set on this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public String getJMSType() {
        return jmsType;
    }

    /**
     * Method to set the JMSReplyTo header value of the messages sent using this JMSProducer
     * to the specified destination.
     *
     * {@inheritDoc}
     */
    @Override
    public JMSProducer setJMSReplyTo(Destination replyTo) {
        jmsReplyTo = replyTo;
        return this;
    }

    /**
     * Method to retrieve the JMSReplyTo header value that has been set on this JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public Destination getJMSReplyTo() {
        return jmsReplyTo;
    }

    /**
     * Method to close this JMSProducer. Not available with javax.jms.JMSProducer.
     *
     * {@inheritDoc}
     */
    @Override
    public void close() {
        _closed.set(true);
        messageProducer.close();
    }

    /**
     * The underlying send method used for all send calls on the JMSProducer.
     *
     * @param destination the destination to which messages need to be sent
     * @param message     the message to be sent
     * @throws JMSException if sending fails due to an internal error
     */
    private void sendMessage(Destination destination, Message message) throws JMSException {
        if (message == null) {
            throw new MessageFormatRuntimeException("Message Cannot Be Null");
        }
        if (destination == null) {
            throw new InvalidDestinationRuntimeException("Destination Cannot Be Null");
        }

        for (Map.Entry<String, Object> entry : messageProperties.entrySet()) {
            message.setObjectProperty(entry.getKey(), entry.getValue());
        }

        //TODO: if message properties read-only --> MessageNotWriteableRuntimeException
        if (jmsCorrelationID != null) {
            message.setJMSCorrelationID(jmsCorrelationID);
        }
        if (jmsCorrelationIDAsBytes != null) {
            message.setJMSCorrelationIDAsBytes(jmsCorrelationIDAsBytes);
        }
        if (jmsReplyTo != null) {
            message.setJMSReplyTo(jmsReplyTo);
        }
        if (jmsType != null) {
            message.setJMSType(jmsType);
        }

        try {
            messageProducer.send(destination, message, deliveryMode, priority, timeToLive);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * The underlying method called by all methods setting message properties.
     *
     * @param name  name of the property
     * @param value the value to set
     * @return JMSProducer  this JMSProducer
     */
    private JMSProducer setMessageProperty(String name, Object value) {
        try {
            if (name != null && name.length() > 0) {
                checkValueValidity(value);
                messageProperties.put(name, value);
                return this;
            } else {
                throw new IllegalArgumentException("Name Cannot be Null or Empty.");
            }
        } catch (MessageFormatException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    /**
     * Helper method to check if value is valid.
     *
     * @param value value to be checked
     * @throws MessageFormatRuntimeException if the value is invalid
     */
    private void checkValueValidity(Object value) throws MessageFormatException {
        boolean isValid = false;
        if (value instanceof Boolean || value instanceof Byte || value instanceof Character ||
                value instanceof Double || value instanceof Float || value instanceof Integer ||
                value instanceof Long || value instanceof Short || value instanceof String ||
                value == null) {
            isValid = true;
        }

        if (!isValid) {
            throw new MessageFormatException("Values of Type " + value.getClass()
                    + " are Not Allowed");
        }
    }
}
