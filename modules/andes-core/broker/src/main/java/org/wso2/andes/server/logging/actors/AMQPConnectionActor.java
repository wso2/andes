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
package org.wso2.andes.server.logging.actors;

import org.wso2.andes.server.logging.RootMessageLogger;
import org.wso2.andes.server.logging.subjects.ConnectionLogSubject;
import org.wso2.andes.server.protocol.AMQProtocolSession;



/**
 * An AMQPConnectionActor represtents a connectionthrough the AMQP port.
 * <p/>
 * This is responsible for correctly formatting the LogActor String in the log
 * <p/>
 * [ con:1(user@127.0.0.1/) ]
 * <p/>
 * To do this it requires access to the IO Layers.
 */
public class AMQPConnectionActor extends AbstractActor
{
    private ConnectionLogSubject _logSubject;

    public AMQPConnectionActor(AMQProtocolSession session, RootMessageLogger rootLogger)
    {
        super(rootLogger);

        _logSubject = new ConnectionLogSubject(session);
    }

    public String getLogMessage()
    {
        return _logSubject.toLogString();
    }
}

