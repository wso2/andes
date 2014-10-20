/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.qmf;

import org.wso2.andes.transport.codec.BBDecoder;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.message.ServerMessage;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.queue.BaseQueue;
import org.wso2.andes.server.registry.IApplicationRegistry;
import org.wso2.andes.AMQException;

import java.util.ArrayList;
import java.util.Collection;

public class QMFPackageQueryCommand extends QMFCommand
{
    public QMFPackageQueryCommand(QMFCommandHeader header, BBDecoder decoder)
    {
        super(header);
    }

    public void process(VirtualHost virtualHost, ServerMessage message)
    {
        String exchangeName = message.getMessageHeader().getReplyToExchange();
        String routingKey = message.getMessageHeader().getReplyToRoutingKey();


        IApplicationRegistry appRegistry = virtualHost.getApplicationRegistry();
        QMFService service = appRegistry.getQMFService();

        Collection<QMFPackage> supportedSchemas = service.getSupportedSchemas();
        
        QMFCommand[] commands = new QMFCommand[ supportedSchemas.size() + 1 ];

        int i = 0;
        for(QMFPackage p : supportedSchemas)
        {
            commands[i++] = new QMFPackageIndicationCommand(this, p.getName());
        }
        commands[ commands.length - 1 ] = new QMFCommandCompletionCommand(this);


        for(QMFCommand cmd : commands)
        {

            QMFMessage responseMessage = new QMFMessage(routingKey, cmd);

            Exchange exchange = virtualHost.getExchangeRegistry().getExchange(exchangeName);

            ArrayList<? extends BaseQueue> queues = exchange.route(responseMessage);


            for(BaseQueue q : queues)
            {
                try
                {
                    q.enqueue(responseMessage);
                }
                catch (AMQException e)
                {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }
}
