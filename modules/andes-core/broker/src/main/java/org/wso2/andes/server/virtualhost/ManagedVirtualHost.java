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
package org.wso2.andes.server.virtualhost;

import java.io.IOException;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;

/**
 * The management interface exposed to allow management of a virtualHost
 */
public interface ManagedVirtualHost
{
    static final String TYPE = "VirtualHost";
    static final int VERSION = 1;

    /**
     * Returns the name of the managed virtualHost.
     * @return the name of the exchange.
     * @throws java.io.IOException
     */
    @MBeanAttribute(name="Name", description= TYPE + " Name")
    String getName() throws IOException;


}
