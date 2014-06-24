package org.wso2.andes.server.protocol;
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


import org.wso2.andes.protocol.ProtocolEngine;
import org.wso2.andes.protocol.ProtocolEngineFactory;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.virtualhost.VirtualHostRegistry;
import org.wso2.andes.transport.network.NetworkConnection;

public class AMQProtocolEngineFactory implements ProtocolEngineFactory
{
    private VirtualHostRegistry _vhosts;

    public AMQProtocolEngineFactory()
    {
        this(1);
    }
    
    public AMQProtocolEngineFactory(Integer port)
    {
        _vhosts = ApplicationRegistry.getInstance().getVirtualHostRegistry();
    }
   
    
    public ProtocolEngine newProtocolEngine(NetworkConnection network)
    {
        return new AMQProtocolEngine(_vhosts, network);
    }
}
