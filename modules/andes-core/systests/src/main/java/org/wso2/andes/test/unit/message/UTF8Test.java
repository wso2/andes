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
package org.wso2.andes.test.unit.message;

import org.wso2.andes.test.utils.QpidBrokerTestCase;
import org.wso2.andes.transport.Connection;
import org.wso2.andes.transport.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InitialContext;
import javax.jms.*;
import java.util.Properties;
import java.io.*;


/**
 * This test makes sure that utf8 characters can be used for
 * specifying exchange, queue name and routing key.
 *
 * those tests are related to qpid-1384
 */
public class UTF8Test extends QpidBrokerTestCase
{
    private static final Logger _logger = LoggerFactory.getLogger(UTF8Test.class);

    public void testPlainEn() throws Exception
    {
         invoke("UTF8En");
    }


    public void testUTF8Jp() throws Exception
    {
        invoke("UTF8Jp");
    }

    private void invoke(String name) throws Exception
    {
        String path = System.getProperties().getProperty("QPID_HOME");
        path = path + "/../systests/src/main/java/org/apache/qpid/test/unit/message/" + name;
         BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF8"));
        runTest(in.readLine(), in.readLine(), in.readLine(), in.readLine());
        in.close();
    }

    private void runTest(String exchangeName, String queueName, String routingKey, String data) throws Exception
    {
        _logger.info("Running test for exchange: " + exchangeName
                + " queue Name: " + queueName
                + " routing key: " + routingKey);       
        declareQueue(exchangeName, routingKey, queueName);

        javax.jms.Connection con =  getConnection();
        javax.jms.Session sess = con.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        Destination dest = getDestination(exchangeName, routingKey, queueName);
        // Send data
        MessageProducer msgProd = sess.createProducer(dest);
        TextMessage message = sess.createTextMessage(data);
        msgProd.send(message);
        // consume data
        MessageConsumer msgCons = sess.createConsumer(dest);
        con.start();
        TextMessage m = (TextMessage) msgCons.receive(RECEIVE_TIMEOUT);
        assertNotNull(m);
        assertEquals(m.getText(), data);
    }

    private void declareQueue(String exch, String routkey, String qname) throws Exception
    {
        Connection conn = new Connection();
        conn.connect("localhost", QpidBrokerTestCase.DEFAULT_PORT, "test", "guest", "guest",false);
        Session sess = conn.createSession(0);
        sess.exchangeDeclare(exch, "direct", null, null);
        sess.queueDeclare(qname, null, null);
        sess.exchangeBind(qname, exch, routkey, null);
        sess.sync();
        conn.close();        
    }

    private Destination getDestination(String exch, String routkey, String qname) throws Exception
    {
        Properties props = new Properties();
        props.setProperty("destination.directUTF8Queue",
                "direct://" + exch + "//" + qname + "?autodelete='false'&durable='false'"
                        + "&routingkey='" + routkey + "'");

        // Get our connection context
        InitialContext ctx = new InitialContext(props);
        return (Destination) ctx.lookup("directUTF8Queue");
    }
}
