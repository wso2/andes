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

package org.wso2.andes.server.subscription;

import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.transport.ServerSession;
import org.wso2.andes.transport.Method;

public class MessageAcceptCompletionListener implements Method.CompletionListener
{
    private final Subscription_0_10 _sub;
    private final QueueEntry _entry;
    private final ServerSession _session;
    private boolean _restoreCredit;

    public MessageAcceptCompletionListener(Subscription_0_10 sub, ServerSession session, QueueEntry entry, boolean restoreCredit)
    {
        super();
        _sub = sub;
        _entry = entry;
        _session = session;
        _restoreCredit = restoreCredit;
    }

    public void onComplete(Method method)
    {
        if(_restoreCredit)
        {
            _sub.restoreCredit(_entry);
        }
        if(_entry.isAcquiredBy(_sub))
        {
            _session.acknowledge(_sub, _entry);
        }

        _session.removeDispositionListener(method);
    }
}
