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

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.pool.ReadWriteRunnable;
import org.wso2.andes.server.logging.actors.CurrentActor;

/**
 * QueueRunners are Runnables used to process a queue when requiring
 * asynchronous message delivery to subscriptions, which is necessary
 * when straight-through delivery of a message to a subscription isn't
 * possible during the enqueue operation.
 */
public class QueueRunner implements ReadWriteRunnable
{
    private static final Logger _logger = Logger.getLogger(QueueRunner.class);

    private final String _name;
    private final SimpleAMQQueue _queue;

    public QueueRunner(SimpleAMQQueue queue, long count)
    {
        _queue = queue;
        _name = "QueueRunner-" + count + "-" + queue.getLogActor();
    }

    public void run()
    {
        String originalName = Thread.currentThread().getName();
        try
        {
            Thread.currentThread().setName(_name);
            CurrentActor.set(_queue.getLogActor());

            _queue.processQueue(this);
        }
        catch (AMQException e)
        {
            _logger.error("Exception during asynchronous delivery by " + _name, e);
        }
        finally
        {
            CurrentActor.remove();
            Thread.currentThread().setName(originalName);
        }
    }

    public boolean isRead()
    {
        return false;
    }

    public boolean isWrite()
    {
        return true;
    }

    public String toString()
    {
        return _name;
    }
}
