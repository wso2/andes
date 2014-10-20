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
package org.wso2.andes.testkit;


import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Random;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.wso2.andes.client.AMQAnyDestination;
import org.wso2.andes.client.AMQConnection;
import org.wso2.andes.tools.MessageFactory;

/**
 * A generic sender which sends a stream of messages
 * to a given address in a broker (host/port) 
 * until told to stop by killing it.
 * 
 * It has a feedback loop to ensure it doesn't fill
 * up queues due to a slow consumer.
 * 
 * It doesn't check for correctness or measure anything
 * leaving those concerns to another entity. 
 * However it prints a timestamp every x secs(-Dreport_frequency)
 * as checkpoint to figure out how far the test has progressed if
 * a failure occurred.
 * 
 * It also takes in an optional Error handler to
 * pass out any error in addition to writing them to std err. 
 * 
 * This is intended more as building block to create
 * more complex test cases. However there is a main method
 * provided to use this standalone.
 *
 * The following options are available and configurable 
 * via jvm args.
 * 
 * msg_size (256) 
 * msg_count (10) - # messages before waiting for feedback
 * sleep_time (1000 ms) - sleep time btw each iteration
 * report_frequency - how often a timestamp is printed
 * durable
 * transacted
 * tx_size - size of transaction batch in # msgs. 
 */
public class Sender extends Client
{
    protected int msg_size = 256;
    protected int msg_count = 10;
    protected int iterations = -1;
    protected long sleep_time = 1000;

    protected Destination dest = null;
    protected Destination replyTo =  null;
    protected DateFormat df = new SimpleDateFormat("yyyy.MM.dd 'at' HH:mm:ss");
    protected NumberFormat nf = new DecimalFormat("##.00");
    
    protected MessageProducer producer;
    Random gen = new Random(19770905);
    
    public Sender(Connection con,String addr) throws Exception
    {
       super(con);
       this.msg_size = Integer.getInteger("msg_size", 100);
       this.msg_count = Integer.getInteger("msg_count", 10);
       this.iterations = Integer.getInteger("iterations", -1);
       this.sleep_time = Long.getLong("sleep_time", 1000);
       this.setSsn(con.createSession(isTransacted(),Session.AUTO_ACKNOWLEDGE));
       this.dest = new AMQAnyDestination(addr);
       this.producer = getSsn().createProducer(dest);
       this.replyTo = getSsn().createTemporaryQueue();
       
       System.out.println("Sending messages to : " + addr);
    }

    /*
     * If msg_size not specified it generates a message
     * between 500-1500 bytes.
     */
    protected Message getNextMessage() throws Exception
    {
        int s =  msg_size == -1 ? 500 + gen.nextInt(1000) : msg_size;
        Message msg = (getContentType().equals("text/plain")) ?
                MessageFactory.createTextMessage(getSsn(), s):
                MessageFactory.createBytesMessage(getSsn(), s);
                
       msg.setJMSDeliveryMode((isDurable()) ? DeliveryMode.PERSISTENT
				: DeliveryMode.NON_PERSISTENT);
       return msg;
    }
         
    public void run()
    {
    	try 
    	{
    		boolean infinite = (iterations == -1);
			for (int x=0; infinite || x < iterations; x++)
			{
				long now = System.currentTimeMillis();
			    if (now - getStartTime() >= getReportFrequency())
			    {
			    	System.out.println(df.format(now) + " - iterations : " + x);
			    	setStartTime(now);
			    }
			    
			    for (int i = 0; i < msg_count; i++)
			    {
			        Message msg = getNextMessage();
			        msg.setIntProperty("sequence",i);
			        producer.send(msg);
			        if (isTransacted() && msg_count % getTxSize() == 0)
			        {
			        	getSsn().commit();
			        }
			    }
			    TextMessage m = getSsn().createTextMessage("End");
			    m.setJMSReplyTo(replyTo);
			    producer.send(m);

			    if (isTransacted())
			    {
			        getSsn().commit();
			    }

			    MessageConsumer feedbackConsumer = getSsn().createConsumer(replyTo);
			    feedbackConsumer.receive();
			    feedbackConsumer.close();
			    if (isTransacted())
			    {
			        getSsn().commit();
			    }
			    Thread.sleep(sleep_time);
			}
		}
    	catch (Exception e)
        {
            handleError("Exception sending messages",e);
        }   	
    }
    
    // Receiver host port address
    public static void main(String[] args) throws Exception
    {
    	String host = "127.0.0.1";
    	int port = 5672;
    	String addr = "message_queue";
    	
    	if (args.length > 0)
    	{
    		host = args[0];
    	}
    	if (args.length > 1)
    	{
    		port = Integer.parseInt(args[1]);
    	}
    	if (args.length > 2)
    	{
    	    addr = args[2];    
    	}
    	
        AMQConnection con = new AMQConnection(
				"amqp://username:password@topicClientid/test?brokerlist='tcp://"
						+ host + ":" + port + "'");
        
        Sender sender = new Sender(con,addr);
        sender.run();
    }
}
