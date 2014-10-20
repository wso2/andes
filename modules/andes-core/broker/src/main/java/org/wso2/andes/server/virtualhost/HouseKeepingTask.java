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

import org.apache.log4j.Logger;
import org.wso2.andes.server.logging.RootMessageLogger;
import org.wso2.andes.server.logging.actors.AbstractActor;
import org.wso2.andes.server.logging.actors.CurrentActor;

public abstract class HouseKeepingTask implements Runnable
{
    Logger _logger = Logger.getLogger(this.getClass());

    private VirtualHost _virtualHost;

    private String _name;

    private RootMessageLogger _rootLogger;
    public HouseKeepingTask(VirtualHost vhost)
    {
        _virtualHost = vhost;
        _name = _virtualHost.getName() + ":" + this.getClass().getSimpleName();
        _rootLogger = CurrentActor.get().getRootMessageLogger();
    }

    final public void run()
    {
        // Don't need to undo this as this is a thread pool thread so will
        // always go through here before we do any real work.
        CurrentActor.set(new AbstractActor(_rootLogger)
        {
            @Override
            public String getLogMessage()
            {
                return _name;
            }
        });

        try
        {
            execute();
        }
        catch (Throwable e)
        {
            _logger.warn(this.getClass().getSimpleName() + " throw exception: " + e, e);
        }
        finally
        {
            CurrentActor.remove();
        }
    }

    public VirtualHost getVirtualHost()
    {
        return _virtualHost;
    }

    /** Execute the plugin. */
    public abstract void execute();

}
