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

import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.server.logging.LogActor;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;

import java.util.UUID;

public interface Subscription
{
    LogActor getLogActor();

    boolean isTransient();

    public static enum State
    {
        ACTIVE,
        SUSPENDED,
        CLOSED
    }

    public static interface StateListener
    {
        public void stateChange(Subscription sub, State oldState, State newState);
    }

    AMQQueue getQueue();

    QueueEntry.SubscriptionAcquiredState getOwningState();
    QueueEntry.SubscriptionAssignedState getAssignedState();


    void setQueue(AMQQueue queue, boolean exclusive);

    void setNoLocal(boolean noLocal);

    AMQShortString getConsumerTag();

    long getSubscriptionID();

    boolean isSuspended();

    boolean hasInterest(QueueEntry msg);

    boolean isAutoClose();

    boolean isClosed();

    boolean acquires();

    boolean seesRequeues();

    void close();

    void send(QueueEntry msg) throws AMQException;

    UUID getIdOfUnderlyingChannel();

    void queueDeleted(AMQQueue queue);


    boolean wouldSuspend(QueueEntry msg);

    void getSendLock();

    void releaseSendLock();

    void onDequeue(final QueueEntry queueEntry);

    void restoreCredit(final QueueEntry queueEntry);

    void setStateListener(final StateListener listener);

    public State getState();

    AMQQueue.Context getQueueContext();

    void setQueueContext(AMQQueue.Context queueContext);


    boolean isActive();

    void confirmAutoClose();

    public void set(String key, Object value);

    public Object get(String key);

    boolean isSessionTransactional();

    /**
     * Used this method to get whether the subscription has Nolocal option
     *
     * @return isNoLocal true / false
     */
    public boolean isNoLocal();
}
