package org.wso2.andes.server.subscription;

/*
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*/

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.logging.LogActor;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.queue.QueueEntry.SubscriptionAcquiredState;

public class MockSubscription implements Subscription
{

    private boolean _closed = false;
    private AMQShortString tag = new AMQShortString("mocktag");
    private AMQQueue queue = null;
    private StateListener _listener = null;
    private AMQQueue.Context _queueContext = null;
    private State _state = State.ACTIVE;
    private ArrayList<QueueEntry> messages = new ArrayList<QueueEntry>();
    private final Lock _stateChangeLock = new ReentrantLock();
    private List<QueueEntry> _acceptEntries = null;

    private final QueueEntry.SubscriptionAcquiredState _owningState = new QueueEntry.SubscriptionAcquiredState(this);
    private final QueueEntry.SubscriptionAssignedState _assignedState = new QueueEntry.SubscriptionAssignedState(this);


    private static final AtomicLong idGenerator = new AtomicLong(0);
    // Create a simple ID that increments for ever new Subscription
    private final long _subscriptionID = idGenerator.getAndIncrement();

    public MockSubscription()
    {
    }

    public MockSubscription(List<QueueEntry> acceptEntries)
    {
        _acceptEntries = acceptEntries;
    }

    public void close()
    {
        _closed = true;
        if (_listener != null)
        {
            _listener.stateChange(this, _state, State.CLOSED);
        }
        _state = State.CLOSED;
    }

    public boolean filtersMessages()
    {
        return false;
    }

    public AMQChannel getChannel()
    {
        return null;
    }

    public AMQShortString getConsumerTag()
    {
        return tag;
    }

    public long getSubscriptionID()
    {
        return _subscriptionID;
    }

    public AMQQueue.Context getQueueContext()
    {
        return _queueContext;
    }

    public SubscriptionAcquiredState getOwningState()
    {
        return _owningState;
    }

    public QueueEntry.SubscriptionAssignedState getAssignedState()
    {
        return _assignedState;
    }

    public LogActor getLogActor()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean isTransient()
    {
        return false;
    }

    public AMQQueue getQueue()
    {
        return queue;
    }

    public void getSendLock()
    {
        _stateChangeLock.lock();
    }

    public boolean hasInterest(QueueEntry entry)
    {
        if(_acceptEntries != null)
        {
            //simulate selector behaviour, only signal
            //interest in the dictated queue entries
            return _acceptEntries.contains(entry);
        }

        return true;
    }

    public boolean isActive()
    {
        return true;
    }

    public void confirmAutoClose()
    {

    }

    public void set(String key, Object value)
    {
    }

    public Object get(String key)
    {
        return null;
    }

    public boolean isAutoClose()
    {
        return false;
    }

    public boolean isBrowser()
    {
        return false;
    }

    public boolean isClosed()
    {
        return _closed;
    }

    public boolean acquires()
    {
        return true;
    }

    public boolean seesRequeues()
    {
        return true;
    }

    public boolean isSuspended()
    {
        return false;
    }

    public void queueDeleted(AMQQueue queue)
    {
    }

    public void releaseSendLock()
    {
        _stateChangeLock.unlock();
    }

    public void resend(QueueEntry entry) throws AMQException
    {
    }

    public void onDequeue(QueueEntry queueEntry)
    {
    }

    public void restoreCredit(QueueEntry queueEntry)
    {
    }

    public void send(QueueEntry entry) throws AMQException
    {
        if (messages.contains(entry))
        {
            entry.setRedelivered();
        }
        messages.add(entry);
    }

    public void setQueueContext(AMQQueue.Context queueContext)
    {
        _queueContext = queueContext;
    }

    public void setQueue(AMQQueue queue, boolean exclusive)
    {
        this.queue = queue;
    }

    public void setNoLocal(boolean noLocal)
    {        
    }

    public void setStateListener(StateListener listener)
    {
        this._listener = listener;
    }

    public State getState()
    {
        return _state;
    }

    public boolean wouldSuspend(QueueEntry msg)
    {
        return false;
    }

    public ArrayList<QueueEntry> getMessages()
    {
        return messages;
    }

    public boolean isSessionTransactional()
    {
        return false;
    }
}
