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

package org.wso2.andes.server.flow;

import org.wso2.andes.server.message.ServerMessage;

public class MessageAndBytesCreditManager extends AbstractFlowCreditManager implements FlowCreditManager
{
    private long _messageCredit;
    private long _bytesCredit;

    public MessageAndBytesCreditManager(final long messageCredit, final long bytesCredit)
    {
        _messageCredit = messageCredit;
        _bytesCredit = bytesCredit;
    }

    public synchronized long getMessageCredit()
    {
        return _messageCredit;
    }

    public synchronized long getBytesCredit()
    {
        return _bytesCredit;
    }

    public synchronized void restoreCredit(long messageCredit, long bytesCredit)
    {        
        _messageCredit += messageCredit;
        _bytesCredit += bytesCredit;
        setSuspended(hasCredit());
    }

    public synchronized void removeAllCredit()
    {
        _messageCredit = 0L;
        _bytesCredit = 0L;
        setSuspended(true);
    }

    public synchronized boolean hasCredit()
    {
        return (_messageCredit > 0L) && ( _bytesCredit > 0L );
    }

    public synchronized boolean useCreditForMessage(ServerMessage msg)
    {
        if(_messageCredit == 0L)
        {
            setSuspended(true);
            return false;
        }
        else
        {
            final long msgSize = msg.getSize();
            if(msgSize > _bytesCredit)
            {
                setSuspended(true);
                return false;
            }
            _messageCredit--;
            _bytesCredit -= msgSize;
            setSuspended(false);
            return true;
        }
        
    }

    public synchronized void setBytesCredit(long bytesCredit)
    {
        _bytesCredit = bytesCredit;
    }
}
