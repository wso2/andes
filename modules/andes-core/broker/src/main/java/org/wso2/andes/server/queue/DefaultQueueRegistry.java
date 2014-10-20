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
package org.wso2.andes.server.queue;

import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.server.virtualhost.VirtualHost;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultQueueRegistry implements QueueRegistry
{
    private ConcurrentMap<AMQShortString, AMQQueue> _queueMap = new ConcurrentHashMap<AMQShortString, AMQQueue>();

    private final VirtualHost _virtualHost;

    public DefaultQueueRegistry(VirtualHost virtualHost)
    {
        _virtualHost = virtualHost;
    }

    public VirtualHost getVirtualHost()
    {
        return _virtualHost;
    }

    public void registerQueue(AMQQueue queue)
    {
        _queueMap.put(queue.getNameShortString(), queue);
    }

    public void unregisterQueue(AMQShortString name)
    {
        _queueMap.remove(name);
    }

    public AMQQueue getQueue(AMQShortString name)
    {
        return _queueMap.get(name);
    }

    public Collection<AMQShortString> getQueueNames()
    {
        return _queueMap.keySet();
    }

    public Collection<AMQQueue> getQueues()
    {
        return _queueMap.values();
    }

    public AMQQueue getQueue(String queue)
    {
        return getQueue(new AMQShortString(queue));
    }
}
