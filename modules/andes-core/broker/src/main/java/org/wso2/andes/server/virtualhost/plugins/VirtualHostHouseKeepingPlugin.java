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
package org.wso2.andes.server.virtualhost.plugins;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.virtualhost.HouseKeepingTask;
import org.wso2.andes.server.virtualhost.VirtualHost;

import java.util.concurrent.TimeUnit;

public abstract class VirtualHostHouseKeepingPlugin extends HouseKeepingTask implements VirtualHostPlugin
{
    protected final Log _logger = LogFactory.getLog(getClass());

    public VirtualHostHouseKeepingPlugin(VirtualHost vhost)
    {
        super(vhost);
    }


    /**
     * Long value representing the delay between repeats
     *
     * @return
     */
    public abstract long getDelay();

    /**
     * Option to specify what the delay value represents
     *
     * @return
     *
     * @see java.util.concurrent.TimeUnit for valid value.
     */
    public abstract TimeUnit getTimeUnit();
}
