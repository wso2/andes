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
package org.wso2.andes.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.server.ack.UnacknowledgedMessageMap;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.store.TransactionLog;
import org.wso2.andes.server.txn.AutoCommitTransaction;
import org.wso2.andes.server.txn.ServerTransaction;

import java.util.Map;

public class ExtractResendAndRequeue implements UnacknowledgedMessageMap.Visitor
{
    private static final Log _log = LogFactory.getLog(ExtractResendAndRequeue.class);

    private final Map<Long, QueueEntry> _msgToRequeue;
    private final Map<Long, QueueEntry> _msgToResend;
    private final boolean _requeueIfUnabletoResend;
    private final UnacknowledgedMessageMap _unacknowledgedMessageMap;
    private final TransactionLog _transactionLog;

    public ExtractResendAndRequeue(UnacknowledgedMessageMap unacknowledgedMessageMap,
                                   Map<Long, QueueEntry> msgToRequeue,
                                   Map<Long, QueueEntry> msgToResend,
                                   boolean requeueIfUnabletoResend,
                                   TransactionLog txnLog)
    {
        _unacknowledgedMessageMap = unacknowledgedMessageMap;
        _msgToRequeue = msgToRequeue;
        _msgToResend = msgToResend;
        _requeueIfUnabletoResend = requeueIfUnabletoResend;
        _transactionLog = txnLog;
    }

    public boolean callback(final long deliveryTag, QueueEntry message) throws AMQException
    {

        message.setRedelivered();
        _msgToRequeue.put(deliveryTag, message);

        // false means continue processing
        return false;
    }


    private void dequeueEntry(final QueueEntry node)
    {
        ServerTransaction txn = new AutoCommitTransaction(_transactionLog);
        dequeueEntry(node, txn);
    }

    private void dequeueEntry(final QueueEntry node, ServerTransaction txn)
    {
        txn.dequeue(node.getQueue(), node.getMessage(),
                    new ServerTransaction.Action()
                    {

                        public void postCommit()
                        {
                            node.discard();
                        }

                        public void onRollback()
                        {

                        }
                    });
    }

    public void visitComplete()
    {
        _unacknowledgedMessageMap.clear();
    }

}
