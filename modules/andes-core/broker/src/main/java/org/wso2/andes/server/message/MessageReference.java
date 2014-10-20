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
package org.wso2.andes.server.message;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class MessageReference<M extends ServerMessage>
{

    private final AtomicBoolean _released = new AtomicBoolean(false);

    private volatile M _message;

    public MessageReference(M message)
    {
        _message = message;
        onReference(message);
    }

    abstract protected void onReference(M message);

    abstract protected void onRelease(M message);

    public M getMessage()
    {
        return _message;
    }

    public void release()
    {
        if(!_released.getAndSet(true))
        {
            if(_message != null)
            {
                onRelease(_message);
            }
        }
    }

}
