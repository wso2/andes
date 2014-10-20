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

import org.apache.log4j.Logger;
import org.wso2.andes.client.AMQSession;
import org.wso2.andes.client.AMQDestination;
import org.wso2.andes.AMQException;
import org.wso2.andes.management.common.mbeans.ManagedQueue;
import org.wso2.andes.server.logging.AbstractTestLogging;
import org.wso2.andes.test.utils.JMXTestUtils;
import org.wso2.andes.framing.AMQShortString;

import javax.jms.*;
import javax.naming.NamingException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;

public class ProducerFlowControlTest extends AbstractTestLogging
{
    private static final int TIMEOUT = 10000;

    private static final Logger _logger = Logger.getLogger(ProducerFlowControlTest.class);

    private Connection producerConnection;
    private MessageProducer producer;
    private Session producerSession;
    private Queue queue;
    private Connection consumerConnection;
    private Session consumerSession;

    private MessageConsumer consumer;
    private final AtomicInteger _sentMessages = new AtomicInteger();

    private JMXTestUtils _jmxUtils;
    private boolean _jmxUtilConnected;
    private static final String USER = "admin";

    public void setUp() throws Exception
    {
        _jmxUtils = new JMXTestUtils(this, USER , USER);
        _jmxUtils.setUp();
        _jmxUtilConnected=false;
        super.setUp();

        _monitor.reset();

        producerConnection = getConnection();
        producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        producerConnection.start();

        consumerConnection = getConnection();
        consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    }

    public void tearDown() throws Exception
    {
        if(_jmxUtilConnected)
        {
            try
            {
                _jmxUtils.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        producerConnection.close();
        consumerConnection.close();
        super.tearDown();
    }

    public void testCapacityExceededCausesBlock()
            throws JMSException, NamingException, AMQException, InterruptedException
    {
        String queueName = getTestQueueName();
        
        final Map<String,Object> arguments = new HashMap<String, Object>();
        arguments.put("x-qpid-capacity",1000);
        arguments.put("x-qpid-flow-resume-capacity",800);
        ((AMQSession) producerSession).createQueue(new AMQShortString(queueName), true, false, false, arguments);
        queue = producerSession.createQueue("direct://amq.direct/"+queueName+"/"+queueName+"?durable='false'&autodelete='true'");
        ((AMQSession) producerSession).declareAndBind((AMQDestination)queue);
        producer = producerSession.createProducer(queue);

        _sentMessages.set(0);


        // try to send 5 messages (should block after 4)
        sendMessagesAsync(producer, producerSession, 5, 50L);

        Thread.sleep(5000);

        assertEquals("Incorrect number of message sent before blocking", 4, _sentMessages.get());

        consumer = consumerSession.createConsumer(queue);
        consumerConnection.start();


        consumer.receive();

        Thread.sleep(1000);

        assertEquals("Message incorrectly sent after one message received", 4, _sentMessages.get());


        consumer.receive();

        Thread.sleep(1000);

        assertEquals("Message not sent after two messages received", 5, _sentMessages.get());

    }

    public void testBrokerLogMessages()
            throws JMSException, NamingException, AMQException, InterruptedException, IOException
    {
        String queueName = getTestQueueName();
        
        final Map<String,Object> arguments = new HashMap<String, Object>();
        arguments.put("x-qpid-capacity",1000);
        arguments.put("x-qpid-flow-resume-capacity",800);
        ((AMQSession) producerSession).createQueue(new AMQShortString(queueName), true, false, false, arguments);
        queue = producerSession.createQueue("direct://amq.direct/"+queueName+"/"+queueName+"?durable='false'&autodelete='true'");
        ((AMQSession) producerSession).declareAndBind((AMQDestination)queue);
        producer = producerSession.createProducer(queue);

        _sentMessages.set(0);


        // try to send 5 messages (should block after 4)
        sendMessagesAsync(producer, producerSession, 5, 50L);

        Thread.sleep(5000);
        List<String> results = waitAndFindMatches("QUE-1003");

        assertEquals("Did not find correct number of QUE-1003 queue overfull messages", 1, results.size());

        consumer = consumerSession.createConsumer(queue);
        consumerConnection.start();


        while(consumer.receive(1000) != null);

        results = waitAndFindMatches("QUE-1004");

        assertEquals("Did not find correct number of UNDERFULL queue underfull messages", 1, results.size());


        
    }


    public void testClientLogMessages()
            throws JMSException, NamingException, AMQException, InterruptedException, IOException
    {
        String queueName = getTestQueueName();
        
        setTestClientSystemProperty("qpid.flow_control_wait_failure","3000");
        setTestClientSystemProperty("qpid.flow_control_wait_notify_period","1000");

        Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);


        final Map<String,Object> arguments = new HashMap<String, Object>();
        arguments.put("x-qpid-capacity",1000);
        arguments.put("x-qpid-flow-resume-capacity",800);
        ((AMQSession) session).createQueue(new AMQShortString(queueName), true, false, false, arguments);
        queue = producerSession.createQueue("direct://amq.direct/"+queueName+"/"+queueName+"?durable='false'&autodelete='true'");
        ((AMQSession) session).declareAndBind((AMQDestination)queue);
        producer = session.createProducer(queue);

        _sentMessages.set(0);


        // try to send 5 messages (should block after 4)
        MessageSender sender = sendMessagesAsync(producer, producerSession, 5, 50L);

        Thread.sleep(TIMEOUT);
        List<String> results = waitAndFindMatches("Message send delayed by", TIMEOUT);
        assertTrue("No delay messages logged by client",results.size()!=0);
        results = findMatches("Message send failed due to timeout waiting on broker enforced flow control");
        assertEquals("Incorrect number of send failure messages logged by client",1,results.size());



    }


    public void testFlowControlOnCapacityResumeEqual()
            throws JMSException, NamingException, AMQException, InterruptedException
    {
        String queueName = getTestQueueName();
        
        final Map<String,Object> arguments = new HashMap<String, Object>();
        arguments.put("x-qpid-capacity",1000);
        arguments.put("x-qpid-flow-resume-capacity",1000);
        ((AMQSession) producerSession).createQueue(new AMQShortString(queueName), true, false, false, arguments);
        queue = producerSession.createQueue("direct://amq.direct/"+queueName+"/"+queueName+"?durable='false'&autodelete='true'");
        ((AMQSession) producerSession).declareAndBind((AMQDestination)queue);
        producer = producerSession.createProducer(queue);

        _sentMessages.set(0);

        // try to send 5 messages (should block after 4)
        sendMessagesAsync(producer, producerSession, 5, 50L);

        Thread.sleep(5000);

        assertEquals("Incorrect number of message sent before blocking", 4, _sentMessages.get());

        consumer = consumerSession.createConsumer(queue);
        consumerConnection.start();


        consumer.receive();

        Thread.sleep(1000);

        assertEquals("Message incorrectly sent after one message received", 5, _sentMessages.get());
        

    }


    public void testFlowControlSoak()
            throws Exception, NamingException, AMQException, InterruptedException
    {
        String queueName = getTestQueueName();
        
        _sentMessages.set(0);
        final int numProducers = 10;
        final int numMessages = 100;

        final Map<String,Object> arguments = new HashMap<String, Object>();
        arguments.put("x-qpid-capacity",6000);
        arguments.put("x-qpid-flow-resume-capacity",3000);

        ((AMQSession) consumerSession).createQueue(new AMQShortString(queueName), false, false, false, arguments);

        queue = producerSession.createQueue("direct://amq.direct/"+queueName+"/"+queueName+"?durable='false'&autodelete='false'");
        ((AMQSession) consumerSession).declareAndBind((AMQDestination)queue);
        consumerConnection.start();

        Connection[] producers = new Connection[numProducers];
        for(int i = 0 ; i < numProducers; i ++)
        {

            producers[i] = getConnection();
            producers[i].start();
            Session session = producers[i].createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer myproducer = session.createProducer(queue);
            MessageSender sender = sendMessagesAsync(myproducer, session, numMessages, 50L);
        }

        consumer = consumerSession.createConsumer(queue);
        consumerConnection.start();

        for(int j = 0; j < numProducers * numMessages; j++)
        {
        
            Message msg = consumer.receive(5000);
            Thread.sleep(50L);
            assertNotNull("Message not received("+j+"), sent: "+_sentMessages.get(), msg);

        }



        Message msg = consumer.receive(500);
        assertNull("extra message received", msg);


        for(int i = 0; i < numProducers; i++)
        {
            producers[i].close();
        }

    }



    public void testSendTimeout()
            throws JMSException, NamingException, AMQException, InterruptedException
    {
        String queueName = getTestQueueName();
        
        setTestClientSystemProperty("qpid.flow_control_wait_failure","3000");
        Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);


        final Map<String,Object> arguments = new HashMap<String, Object>();
        arguments.put("x-qpid-capacity",1000);
        arguments.put("x-qpid-flow-resume-capacity",800);
        ((AMQSession) session).createQueue(new AMQShortString(queueName), true, false, false, arguments);
        queue = producerSession.createQueue("direct://amq.direct/"+queueName+"/"+queueName+"?durable='false'&autodelete='true'");
        ((AMQSession) session).declareAndBind((AMQDestination)queue);
        producer = session.createProducer(queue);

        _sentMessages.set(0);


        // try to send 5 messages (should block after 4)
        MessageSender sender = sendMessagesAsync(producer, producerSession, 5, 50L);

        Thread.sleep(10000);

        Exception e = sender.getException();

        assertNotNull("No timeout exception on sending", e);

    }
    
    
    public void testFlowControlAttributeModificationViaJMX()
    throws JMSException, NamingException, AMQException, InterruptedException, Exception
    {
        _jmxUtils.open();
        _jmxUtilConnected = true;
        
        String queueName = getTestQueueName();
        
        //create queue
        final Map<String,Object> arguments = new HashMap<String, Object>();
        arguments.put("x-qpid-capacity",0);
        arguments.put("x-qpid-flow-resume-capacity",0);
        ((AMQSession) producerSession).createQueue(new AMQShortString(queueName), true, false, false, arguments);

        queue = producerSession.createQueue("direct://amq.direct/"+queueName+"/"+queueName+"?durable='false'&autodelete='true'");

        ((AMQSession) producerSession).declareAndBind((AMQDestination)queue);
        producer = producerSession.createProducer(queue);
        
        Thread.sleep(1000);
        
        //Create a JMX MBean proxy for the queue
        ManagedQueue queueMBean = _jmxUtils.getManagedObject(ManagedQueue.class, _jmxUtils.getQueueObjectName("test", queueName));
        assertNotNull(queueMBean);
        
        //check current attribute values are 0 as expected
        assertTrue("Capacity was not the expected value", queueMBean.getCapacity() == 0L);
        assertTrue("FlowResumeCapacity was not the expected value", queueMBean.getFlowResumeCapacity() == 0L);
        
        //set new values that will cause flow control to be active, and the queue to become overfull after 1 message is sent
        queueMBean.setCapacity(250L);
        queueMBean.setFlowResumeCapacity(250L);
        assertTrue("Capacity was not the expected value", queueMBean.getCapacity() == 250L);
        assertTrue("FlowResumeCapacity was not the expected value", queueMBean.getFlowResumeCapacity() == 250L);
        assertFalse("Queue should not be overfull", queueMBean.isFlowOverfull());
        
        // try to send 2 messages (should block after 1)
        _sentMessages.set(0);
        sendMessagesAsync(producer, producerSession, 2, 50L);

        Thread.sleep(2000);

        //check only 1 message was sent, and queue is overfull
        assertEquals("Incorrect number of message sent before blocking", 1, _sentMessages.get());
        assertTrue("Queue should be overfull", queueMBean.isFlowOverfull());
        
        //raise the attribute values, causing the queue to become underfull and allow the second message to be sent.
        queueMBean.setCapacity(300L);
        queueMBean.setFlowResumeCapacity(300L);
        
        Thread.sleep(2000);

        //check second message was sent, and caused the queue to become overfull again
        assertEquals("Second message was not sent after lifting FlowResumeCapacity", 2, _sentMessages.get());
        assertTrue("Queue should be overfull", queueMBean.isFlowOverfull());
        
        //raise capacity above queue depth, check queue remains overfull as FlowResumeCapacity still exceeded
        queueMBean.setCapacity(700L);
        assertTrue("Queue should be overfull", queueMBean.isFlowOverfull());
        
        //receive a message, check queue becomes underfull
        
        consumer = consumerSession.createConsumer(queue);
        consumerConnection.start();
        
        consumer.receive();
        
        //perform a synchronous op on the connection
        ((AMQSession) consumerSession).sync();
        
        assertFalse("Queue should not be overfull", queueMBean.isFlowOverfull());
        
        consumer.receive();
    }

    private MessageSender sendMessagesAsync(final MessageProducer producer,
                                            final Session producerSession,
                                            final int numMessages,
                                            long sleepPeriod)
    {
        MessageSender sender = new MessageSender(producer, producerSession, numMessages,sleepPeriod);
        new Thread(sender).start();
        return sender;
    }

    private void sendMessages(MessageProducer producer, Session producerSession, int numMessages, long sleepPeriod)
            throws JMSException
    {

        for (int msg = 0; msg < numMessages; msg++)
        {
            producer.send(nextMessage(msg, producerSession));
            _sentMessages.incrementAndGet();


            try
            {
                ((AMQSession)producerSession).sync();
            }
            catch (AMQException e)
            {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    private static final byte[] BYTE_300 = new byte[300];


    private Message nextMessage(int msg, Session producerSession) throws JMSException
    {
        BytesMessage send = producerSession.createBytesMessage();
        send.writeBytes(BYTE_300);
        send.setIntProperty("msg", msg);

        return send;
    }


    private class MessageSender implements Runnable
    {
        private final MessageProducer _producer;
        private final Session _producerSession;
        private final int _numMessages;



        private JMSException _exception;
        private long _sleepPeriod;

        public MessageSender(MessageProducer producer, Session producerSession, int numMessages, long sleepPeriod)
        {
            _producer = producer;
            _producerSession = producerSession;
            _numMessages = numMessages;
            _sleepPeriod = sleepPeriod;
        }

        public void run()
        {
            try
            {
                sendMessages(_producer, _producerSession, _numMessages, _sleepPeriod);
            }
            catch (JMSException e)
            {
                _exception = e;
            }
        }

        public JMSException getException()
        {
            return _exception;
        }
    }
}
