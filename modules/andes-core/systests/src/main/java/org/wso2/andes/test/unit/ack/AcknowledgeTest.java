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

package org.wso2.andes.test.unit.ack;

import org.wso2.andes.client.AMQDestination;
import org.wso2.andes.client.AMQSession;
import org.wso2.andes.test.utils.FailoverBaseCase;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.MessageProducer;

/**
 * Test the various JMS Acknowledge Modes the single testAcking method does all
 * the work of receiving and validation of acking.
 *
 * The ack mode is provided from the various test methods. 
 */
public class AcknowledgeTest extends FailoverBaseCase
{
    protected int NUM_MESSAGES;
    protected Connection _connection;
    protected Queue _queue;
    protected Session _consumerSession;
    protected MessageConsumer _consumer;
    protected MessageProducer _producer;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        NUM_MESSAGES = 5;

        _queue = getTestQueue();

        _logger.info("AT: setup");
        //Create Producer put some messages on the queue
        _connection = getConnection();
    }

    protected void init(boolean transacted, int mode) throws Exception
    {
        _consumerSession = _connection.createSession(transacted, mode);
        _consumer = _consumerSession.createConsumer(_queue);
        _producer = _consumerSession.createProducer(_queue);

        // These should all end up being prefetched by session
        sendMessage(_consumerSession, _queue, 1);

        assertEquals("Wrong number of messages on queue", 1,
                     ((AMQSession) _consumerSession).getQueueDepth((AMQDestination) _queue));
    }

    /**
     * The main test method.
     *
     * Receive the initial message and then proceed to send and ack messages
     * until we have processed NUM_MESSAGES worth of messages.
     *
     * Each message is tagged with an INDEX value and these are used to check
     * that the messages are received in the correct order.
     *
     * The test concludes by validating that the queue depth is 0 as expected.
     *
     * @param transacted
     * @param mode
     *
     * @throws Exception
     */
    protected void testAcking(boolean transacted, int mode) throws Exception
    {
        init(transacted, mode);

        _connection.start();

        Message msg = _consumer.receive(1500);

        int count = 0;
        while (count < NUM_MESSAGES)
        {
            assertNotNull("Message " + count + " not correctly received.", msg);
            assertEquals("Incorrect message received", count, msg.getIntProperty(INDEX));
            count++;

            if (count < NUM_MESSAGES)
            {
                //Send the next message
                _producer.send(createNextMessage(_consumerSession, count));
            }

            doAcknowlegement(msg);

            msg = _consumer.receive(1500);
        }

        if (_consumerSession.getTransacted())
        {
            //Acknowledge the last msg if we are testing transacted otherwise queueDepth will be 1
            doAcknowlegement(msg);
        }

        assertEquals("Wrong number of messages on queue", 0,
                     ((AMQSession) _consumerSession).getQueueDepth((AMQDestination) _queue));
    }

    /**
     * Perform the acknowledgement of messages if additionally required.
     *
     * @param msg
     *
     * @throws JMSException
     */
    protected void doAcknowlegement(Message msg) throws JMSException
    {
        if (_consumerSession.getTransacted())
        {
            _consumerSession.commit();
        }

        if (_consumerSession.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE)
        {
            msg.acknowledge();
        }
    }

    public void testClientAck() throws Exception
    {
        testAcking(false, Session.CLIENT_ACKNOWLEDGE);
    }

    public void testAutoAck() throws Exception
    {
        testAcking(false, Session.AUTO_ACKNOWLEDGE);
    }

    public void testTransacted() throws Exception
    {
        testAcking(true, Session.SESSION_TRANSACTED);
    }

    public void testDupsOk() throws Exception
    {
        testAcking(false, Session.DUPS_OK_ACKNOWLEDGE);
    }

    public void testNoAck() throws Exception
    {
        testAcking(false, AMQSession.NO_ACKNOWLEDGE);
    }

    public void testPreAck() throws Exception
    {
        testAcking(false, AMQSession.PRE_ACKNOWLEDGE);
    }

}
