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
package org.wso2.andes.server.logging.subjects;

import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.queue.AMQQueue;
import static org.wso2.andes.server.logging.subjects.LogSubjectFormat.BINDING_FORMAT;

public class BindingLogSubject extends AbstractLogSubject
{

    /**
     * Create a BindingLogSubject that Logs in the following format.
     *
     * [ vh(/)/ex(amq.direct)/qu(testQueue)/bd(testQueue) ]
     *
     * @param routingKey
     * @param exchange
     * @param queue
     */
    public BindingLogSubject(String routingKey, Exchange exchange,
                             AMQQueue queue)
    {
        //"vh(/{0})/ex({1}/{2})/qu({3})/rk({4})"
//        _logString = new StringBuffer("[")
//            .append("vh(").append(queue.getVirtualHost().getName()).append(")/")
//            .append("ex(").append(exchange.getTypeShortString()).append(")/").append(exchange.getNameShortString()).append(")")
//            .append("qu(").append(queue.getNameShortString()).append(")/")
//            .append("rk(").append(routingKey).append("")
//            .append("]").toString();
//        System.out.println(_logString);
        //Above was very expensive, so reverting logs to minimal
      _logString = new StringBuffer("[")
      .append(routingKey).append(" ").append(queue.getNameShortString()).append("]").toString();
    }

}
