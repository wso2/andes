/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

import org.wso2.andes.pool.ReadWriteRunnable;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.AMQException;
import org.apache.log4j.Logger;

public class SubFlushRunner implements ReadWriteRunnable {
    private static final Logger _logger = Logger.getLogger(SubFlushRunner.class);


    private final Subscription _sub;
    private final String _name;
    private static final long ITERATIONS = SimpleAMQQueue.MAX_ASYNC_DELIVERIES;

    public SubFlushRunner(Subscription sub)
    {
        _sub = sub;
        _name = "SubFlushRunner-"+_sub;
    }

    public void run()
    {

        String originalName = Thread.currentThread().getName();
        try
        {
            Thread.currentThread().setName(_name);

            boolean complete = false;
            try
            {
                CurrentActor.set(_sub.getLogActor());
                complete = getQueue().flushSubscription(_sub, ITERATIONS);

            }
            catch (AMQException e)
            {
                _logger.error(e);
            }
            finally
            {
                CurrentActor.remove();
            }
            if (!complete && !_sub.isSuspended())
            {
                getQueue().execute(this);
            }

        }
        finally
        {
            Thread.currentThread().setName(originalName);
        }

    }

    private SimpleAMQQueue getQueue()
    {
        return (SimpleAMQQueue) _sub.getQueue();
    }

    public boolean isRead()
    {
        return false;
    }

    public boolean isWrite()
    {
        return true;
    }
}
