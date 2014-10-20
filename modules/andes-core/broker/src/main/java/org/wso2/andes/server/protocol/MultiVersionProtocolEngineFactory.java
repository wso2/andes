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
package org.wso2.andes.server.protocol;

import java.util.EnumSet;
import java.util.Set;

import org.wso2.andes.protocol.ProtocolEngine;
import org.wso2.andes.protocol.ProtocolEngineFactory;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.registry.IApplicationRegistry;
import org.wso2.andes.transport.network.NetworkConnection;

public class MultiVersionProtocolEngineFactory implements ProtocolEngineFactory
{
    private static final Set<AmqpProtocolVersion> ALL_VERSIONS = EnumSet.allOf(AmqpProtocolVersion.class);

    private final IApplicationRegistry _appRegistry;
    private final String _fqdn;
    private final Set<AmqpProtocolVersion> _supported;


    public MultiVersionProtocolEngineFactory()
    {
        this(1, "localhost", ALL_VERSIONS);
    }

    public MultiVersionProtocolEngineFactory(String fqdn, Set<AmqpProtocolVersion> versions)
    {
        this(1, fqdn, versions);
    }


    public MultiVersionProtocolEngineFactory(String fqdn)
    {
        this(1, fqdn, ALL_VERSIONS);
    }

    public MultiVersionProtocolEngineFactory(int instance, String fqdn, Set<AmqpProtocolVersion> supportedVersions)
    {
        _appRegistry = ApplicationRegistry.getInstance();
        _fqdn = fqdn;
        _supported = supportedVersions;
    }


    public ProtocolEngine newProtocolEngine(NetworkConnection network)
    {
        return new MultiVersionProtocolEngine(_appRegistry, _fqdn, _supported, network);
    }
}
