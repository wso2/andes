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
package org.wso2.andes.extras.exchanges.diagnostic;

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.exchange.AbstractExchange;
import org.wso2.andes.server.exchange.ExchangeType;
import org.wso2.andes.server.message.InboundMessage;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.virtualhost.VirtualHost;

import java.util.ArrayList;
import java.util.Map;

/**
 * This is a special diagnostic exchange type which doesn't actually do anything
 * with messages. When it receives a message, it writes information about the
 * current memory usage to the "memory" property of the message and places it on the
 * diagnosticqueue for retrieval
 */
public class DiagnosticExchange extends AbstractExchange
{
    private static final Logger _logger = Logger.getLogger(DiagnosticExchange.class);

    public static final AMQShortString DIAGNOSTIC_EXCHANGE_CLASS = new AMQShortString("x-diagnostic");
    public static final AMQShortString DIAGNOSTIC_EXCHANGE_NAME = new AMQShortString("diagnostic");

    /** The logger */
    //private static final Logger _logger = Logger.getLogger(DiagnosticExchange.class);

    public static final ExchangeType<DiagnosticExchange> TYPE = new ExchangeType<DiagnosticExchange>()
    {

        public AMQShortString getName()
        {
            return DIAGNOSTIC_EXCHANGE_CLASS;
        }

        public Class<DiagnosticExchange> getExchangeClass()
        {
            return DiagnosticExchange.class;
        }

        public DiagnosticExchange newInstance(VirtualHost host,
                                            AMQShortString name,
                                            boolean durable,
                                            int ticket,
                                            boolean autoDelete) throws AMQException
        {
            DiagnosticExchange exch = new DiagnosticExchange();
            exch.initialise(host,name,durable,ticket,autoDelete);
            return exch;
        }

        public AMQShortString getDefaultExchangeName()
        {
            return DIAGNOSTIC_EXCHANGE_NAME ;
        }
    };

    public DiagnosticExchange()
    {
        super(TYPE);
    }

    public Logger getLogger()
    {
        return _logger;
    }

    public void registerQueue(String routingKey, AMQQueue queue, Map<String, Object> args) throws AMQException
    {
        // No op
    }


    public boolean isBound(AMQShortString routingKey, AMQQueue queue)
    {
        return false;
    }

    public boolean isBound(AMQShortString routingKey)
    {
        return false;
    }

    public boolean isBound(AMQQueue queue)
    {
        return false;
    }

    public boolean hasBindings()
    {
        return false;
    }

    public ArrayList<AMQQueue> doRoute(InboundMessage payload)
    {
        //TODO shouldn't modify messages... perhaps put a new message on the queue?
        /*
        Long value = new Long(SizeOf.getUsedMemory());
        AMQShortString key = new AMQShortString("memory");
        FieldTable headers = ((BasicContentHeaderProperties)payload.getMessageHeader().properties).getHeaders();
        headers.put(key, value);
        ((BasicContentHeaderProperties)payload.getMessageHeader().properties).setHeaders(headers);
        */
        AMQQueue q = getQueueRegistry().getQueue(new AMQShortString("diagnosticqueue"));
        ArrayList<AMQQueue> queues =  new ArrayList<AMQQueue>();
        queues.add(q);
        return queues;
    }


	public boolean isBound(AMQShortString routingKey, FieldTable arguments,
			AMQQueue queue) {
		// TODO Auto-generated method stub
		return false;
	}

    protected void onBind(final Binding binding)
    {
        // No op
    }

    protected void onUnbind(final Binding binding)
    {
        // No op
    }
}
