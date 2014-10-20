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
package org.wso2.andes.server.store;

import org.wso2.andes.server.util.InternalBrokerBaseCase;
import org.wso2.andes.server.protocol.InternalTestProtocolSession;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQShortString;

import java.util.List;

public class MessageStoreShutdownTest extends InternalBrokerBaseCase
{

    public void test()
    {
        subscribe(getSession(), getChannel(), getQueue());

        try
        {
            publishMessages(getSession(), getChannel(), 1);
        }
        catch (AMQException e)
        {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            fail(e.getMessage());
        }

        try
        {
            getRegistry().close();
        }
        catch (Exception e)
        {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            fail(e.getMessage());
        }

        assertTrue("Session should now be closed", getSession().isClosed());


        //Test attempting to modify the broker state after session has been closed.

        //The Message should have been removed from the unacked list.

        //Ack Messages
        List<InternalTestProtocolSession.DeliveryPair> list = getSession().getDelivers(getChannel().getChannelId(), new AMQShortString("sgen_1"), 1);

        InternalTestProtocolSession.DeliveryPair pair = list.get(0);

        try
        {
            // The message should now be requeued and so unable to ack it.
            getChannel().acknowledgeMessage(pair.getDeliveryTag(), false);
        }
        catch (AMQException e)
        {
            assertEquals("Incorrect exception thrown", "Single ack on delivery tag 1 not known for channel:1", e.getMessage());
        }

    }

}
