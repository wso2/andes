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

import org.wso2.andes.server.virtualhost.VirtualHost;

import java.util.Map;

public class ConflationQueue extends SimpleAMQQueue
{
    protected ConflationQueue(String name,
                              boolean durable,
                              String owner,
                              boolean autoDelete,
                              boolean exclusive,
                              VirtualHost virtualHost,
                              Map<String, Object> args,
                              String conflationKey)
    {
        super(name, durable, owner, autoDelete, exclusive, virtualHost, new ConflationQueueList.Factory(conflationKey), args);
    }

    public String getConflationKey()
    {
        return ((ConflationQueueList) _entries).getConflationKey();
    }

}
