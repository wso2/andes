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
package org.wso2.andes.server.exchange;

import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.server.configuration.VirtualHostConfiguration;

import java.util.Collection;


public interface ExchangeFactory
{
    Exchange createExchange(AMQShortString exchange, AMQShortString type, boolean durable, boolean autoDelete,
                            int ticket)
            throws AMQException;

    void initialise(VirtualHostConfiguration hostConfig);

    Collection<ExchangeType<? extends Exchange>> getRegisteredTypes();
    
    Collection<ExchangeType<? extends Exchange>> getPublicCreatableTypes();

    Exchange createExchange(String exchange, String type, boolean durable, boolean autoDelete) throws AMQException;
}
