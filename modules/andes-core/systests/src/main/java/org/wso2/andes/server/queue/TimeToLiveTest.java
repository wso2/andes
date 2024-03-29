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

package org.wso2.andes.server.queue;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TopicSubscriber;

import junit.framework.Assert;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.wso2.andes.client.AMQConnection;
import org.wso2.andes.client.AMQDestination;
import org.wso2.andes.client.AMQQueue;
import org.wso2.andes.client.AMQSession;
import org.wso2.andes.client.AMQTopic;
import org.wso2.andes.test.utils.QpidBrokerTestCase;

public class TimeToLiveTest extends QpidBrokerTestCase
{
    private static final Logger _logger = LogManager.getLogger(TimeToLiveTest.class);

    protected final String QUEUE = "TimeToLiveQueue";

    private final long TIME_TO_LIVE = 100L;

    private static final int MSG_COUNT = 50;
    private static final long SERVER_TTL_TIMEOUT = 60000L;

    public void testPassiveTTL() throws Exception
    {
        //Create Client 1
        Connection clientConnection = getConnection();
        
        Session clientSession = clientConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = clientSession.createQueue(QUEUE); 
        
        // Create then close the consumer so the queue is actually created
        // Closing it then reopening it ensures that the consumer shouldn't get messages
        // which should have expired and allows a shorter sleep period. See QPID-1418
        
        MessageConsumer consumer = clientSession.createConsumer(queue);
        consumer.close();

        //Create Producer
        Connection producerConnection = getConnection();

        producerConnection.start();

        // Move to a Transacted session to ensure that all messages have been delivered to broker before
        // we start waiting for TTL
        Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);

        MessageProducer producer = producerSession.createProducer(queue);

        //Set TTL
        int msg = 0;
        producer.send(nextMessage(String.valueOf(msg), true, producerSession, producer));

        producer.setTimeToLive(TIME_TO_LIVE);

        for (; msg < MSG_COUNT - 2; msg++)
        {
            producer.send(nextMessage(String.valueOf(msg), false, producerSession, producer));
        }

        //Reset TTL
        producer.setTimeToLive(0L);
        producer.send(nextMessage(String.valueOf(msg), false, producerSession, producer));

        producerSession.commit();

        consumer = clientSession.createConsumer(queue);

        // Ensure we sleep the required amount of time.
        ReentrantLock waitLock = new ReentrantLock();
        Condition wait = waitLock.newCondition();
        final long MILLIS = 1000000L;

        long waitTime = TIME_TO_LIVE * MILLIS;
        while (waitTime > 0)
        {
            try
            {
                waitLock.lock();

                waitTime = wait.awaitNanos(waitTime);
            }
            catch (InterruptedException e)
            {
                //Stop if we are interrupted
                fail(e.getMessage());
            }
            finally
            {
                waitLock.unlock();
            }

        }

        clientConnection.start();

        //Receive Message 0
        // Set 5s receive time for messages we expect to receive.
        Message receivedFirst = consumer.receive(5000);
        Message receivedSecond = consumer.receive(5000);
        Message receivedThird = consumer.receive(1000);
        
        // Log the messages to help diagnosis incase of failure
        _logger.info("First:"+receivedFirst);
        _logger.info("Second:"+receivedSecond);
        _logger.info("Third:"+receivedThird);

        // Only first and last messages sent should survive expiry
        Assert.assertNull("More messages received", receivedThird); 

        Assert.assertNotNull("First message not received", receivedFirst);
        Assert.assertTrue("First message doesn't have first set.", receivedFirst.getBooleanProperty("first"));
        Assert.assertEquals("First message has incorrect TTL.", 0L, receivedFirst.getLongProperty("TTL"));

        Assert.assertNotNull("Final message not received", receivedSecond);
        Assert.assertFalse("Final message has first set.", receivedSecond.getBooleanProperty("first"));
        Assert.assertEquals("Final message has incorrect TTL.", 0L, receivedSecond.getLongProperty("TTL"));

        clientConnection.close();

        producerConnection.close();
    }

    private Message nextMessage(String msg, boolean first, Session producerSession, MessageProducer producer) throws JMSException
    {
        Message send = producerSession.createTextMessage("Message " + msg);
        send.setBooleanProperty("first", first);
        send.setStringProperty("testprop", "TimeToLiveTest");
        send.setLongProperty("TTL", producer.getTimeToLive());
        return send;
    }


    /**
     * Tests the expired messages get actively deleted even on queues which have no consumers
     * @throws Exception 
     */
    public void testActiveTTL() throws Exception
    {
        Connection producerConnection = getConnection();
        AMQSession producerSession = (AMQSession) producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = producerSession.createTemporaryQueue();
        producerSession.declareAndBind((AMQDestination) queue);
        MessageProducer producer = producerSession.createProducer(queue);
        producer.setTimeToLive(1000L);

        // send Messages
        for(int i = 0; i < MSG_COUNT; i++)
        {
            producer.send(producerSession.createTextMessage("Message: "+i));
        }
        long failureTime = System.currentTimeMillis() + 2 * SERVER_TTL_TIMEOUT;

        // check Queue depth for up to TIMEOUT seconds after the Queue Depth hasn't changed for 100ms.
        long messageCount = MSG_COUNT;
        long lastPass;

        do
        {
            lastPass = messageCount;
            Thread.sleep(100);
            messageCount = producerSession.getQueueDepth((AMQDestination) queue);

            // If we have received messages in the last loop then extend the timeout time.
            // if we get messages stuck that are not expiring then the failureTime will occur
            // failing the test. This will help with the scenario when the broker does not
            // have enough CPU cycles to process the TTLs.
            if (lastPass != messageCount)
            {
                failureTime = System.currentTimeMillis() + 2 * SERVER_TTL_TIMEOUT;
            }
        }
        while(messageCount > 0L && System.currentTimeMillis() < failureTime);

        assertEquals("Messages not automatically expired: ", 0L, messageCount);

        producer.close();
        producerSession.close();
        producerConnection.close();
    }
    
    public void testPassiveTTLwithDurableSubscription() throws Exception
    {
        //Create Client 1
        Connection clientConnection = getConnection();
        
        Session clientSession = clientConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        // Create and close the durable subscriber
        AMQTopic topic = new AMQTopic((AMQConnection) clientConnection, getTestQueueName());
        TopicSubscriber durableSubscriber = clientSession.createDurableSubscriber(topic, getTestQueueName(),"testprop='TimeToLiveTest'", false);
        durableSubscriber.close();
        
        //Create Producer
        Connection producerConnection = getConnection();

        producerConnection.start();

        // Move to a Transacted session to ensure that all messages have been delivered to broker before
        // we start waiting for TTL
        Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);

        MessageProducer producer = producerSession.createProducer(topic);

        //Set TTL
        int msg = 0;
        producer.send(nextMessage(String.valueOf(msg), true, producerSession, producer));

        producer.setTimeToLive(TIME_TO_LIVE);

        for (; msg < MSG_COUNT - 2; msg++)
        {
            producer.send(nextMessage(String.valueOf(msg), false, producerSession, producer));
        }

        //Reset TTL
        producer.setTimeToLive(0L);
        producer.send(nextMessage(String.valueOf(msg), false, producerSession, producer));

        producerSession.commit();
        
        //resubscribe
        durableSubscriber = clientSession.createDurableSubscriber(topic, getTestQueueName(),"testprop='TimeToLiveTest'", false);

        // Ensure we sleep the required amount of time.
        ReentrantLock waitLock = new ReentrantLock();
        Condition wait = waitLock.newCondition();
        final long MILLIS = 1000000L;

        long waitTime = TIME_TO_LIVE * MILLIS;
        while (waitTime > 0)
        {
            try
            {
                waitLock.lock();

                waitTime = wait.awaitNanos(waitTime);
            }
            catch (InterruptedException e)
            {
                //Stop if we are interrupted
                fail(e.getMessage());
            }
            finally
            {
                waitLock.unlock();
            }

        }

        clientConnection.start();

        //Receive Message 0
        // Set 5s receive time for messages we expect to receive.
        Message receivedFirst = durableSubscriber.receive(5000);
        Message receivedSecond = durableSubscriber.receive(5000);
        Message receivedThird = durableSubscriber.receive(1000);
        
        // Log the messages to help diagnosis incase of failure
        _logger.info("First:"+receivedFirst);
        _logger.info("Second:"+receivedSecond);
        _logger.info("Third:"+receivedThird);

        // Only first and last messages sent should survive expiry
        Assert.assertNull("More messages received", receivedThird); 

        Assert.assertNotNull("First message not received", receivedFirst);
        Assert.assertTrue("First message doesn't have first set.", receivedFirst.getBooleanProperty("first"));
        Assert.assertEquals("First message has incorrect TTL.", 0L, receivedFirst.getLongProperty("TTL"));

        Assert.assertNotNull("Final message not received", receivedSecond);
        Assert.assertFalse("Final message has first set.", receivedSecond.getBooleanProperty("first"));
        Assert.assertEquals("Final message has incorrect TTL.", 0L, receivedSecond.getLongProperty("TTL"));

        clientSession.unsubscribe(getTestQueueName());
        clientConnection.close();

        producerConnection.close();
    }

    public void testActiveTTLwithDurableSubscription() throws Exception
    {
        //Create Client 1
        Connection clientConnection = getConnection();
        Session clientSession = clientConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        // Create and close the durable subscriber
        AMQTopic topic = new AMQTopic((AMQConnection) clientConnection, getTestQueueName());
        TopicSubscriber durableSubscriber = clientSession.createDurableSubscriber(topic, "MyDurableTTLSubscription","testprop='TimeToLiveTest'", false);
        durableSubscriber.close();
        
        //Create Producer
        Connection producerConnection = getConnection();
        AMQSession producerSession = (AMQSession) producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(topic);
        producer.setTimeToLive(1000L);

        // send Messages
        for(int i = 0; i < MSG_COUNT; i++)
        {
            producer.send(producerSession.createTextMessage("Message: "+i));
        }
        long failureTime = System.currentTimeMillis() + 2 * SERVER_TTL_TIMEOUT;

        // check Queue depth for up to TIMEOUT seconds after the Queue Depth hasn't changed for 100ms.
        long messageCount = MSG_COUNT;
        long lastPass;
        AMQQueue subcriptionQueue = new AMQQueue("amq.topic","clientid" + ":" + "MyDurableTTLSubscription");
        do
        {
            lastPass = messageCount;
            Thread.sleep(100);
            messageCount = producerSession.getQueueDepth((AMQDestination) subcriptionQueue);

            // If we have received messages in the last loop then extend the timeout time.
            // if we get messages stuck that are not expiring then the failureTime will occur
            // failing the test. This will help with the scenario when the broker does not
            // have enough CPU cycles to process the TTLs.
            if (lastPass != messageCount)
            {
                failureTime = System.currentTimeMillis() + 2 * SERVER_TTL_TIMEOUT;
            }
        }
        while(messageCount > 0L && System.currentTimeMillis() < failureTime);

        assertEquals("Messages not automatically expired: ", 0L, messageCount);

        producer.close();
        producerSession.close();
        producerConnection.close();
        
        clientSession.unsubscribe("MyDurableTTLSubscription");
        clientSession.close();
        clientConnection.close();
    }

}
