/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.common.AMQPFilterTypes;
import org.wso2.andes.common.ClientProperties;
import org.wso2.andes.configuration.qpid.ConfigStore;
import org.wso2.andes.configuration.qpid.ConfiguredObject;
import org.wso2.andes.configuration.qpid.SessionConfig;
import org.wso2.andes.configuration.qpid.SubscriptionConfig;
import org.wso2.andes.configuration.qpid.SubscriptionConfigType;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.kernel.AndesUtils;
import org.wso2.andes.kernel.ProtocolDeliveryFailureException;
import org.wso2.andes.kernel.SubscriptionAlreadyClosedException;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.filter.FilterManager;
import org.wso2.andes.server.filter.FilterManagerFactory;
import org.wso2.andes.server.flow.FlowCreditManager;
import org.wso2.andes.server.logging.LogActor;
import org.wso2.andes.server.logging.LogSubject;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.actors.SubscriptionActor;
import org.wso2.andes.server.logging.messages.SubscriptionMessages;
import org.wso2.andes.server.logging.subjects.SubscriptionLogSubject;
import org.wso2.andes.server.output.ProtocolOutputConverter;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Encapsulation of a subscription to a queue. <p/> Ties together the protocol session of a subscriber, the consumer tag
 * that was given out by the broker and the channel id. <p/>
 */
public abstract class SubscriptionImpl implements Subscription, FlowCreditManager.FlowCreditManagerListener,
        SubscriptionConfig
{


    private static Log log = LogFactory.getLog(SubscriptionImpl.class);

    private StateListener _stateListener = new StateListener()
                                            {

                                                public void stateChange(Subscription sub, State oldState, State newState)
                                                {

                                                }
                                            };


    private final AtomicReference<State> _state = new AtomicReference<State>(State.ACTIVE);
    private AMQQueue.Context _queueContext;

    private final ClientDeliveryMethod _deliveryMethod;
    private final RecordDeliveryMethod _recordMethod;

    private final QueueEntry.SubscriptionAcquiredState _owningState = new QueueEntry.SubscriptionAcquiredState(this);
    private final QueueEntry.SubscriptionAssignedState _assignedState = new QueueEntry.SubscriptionAssignedState(this);

    private final Map<String, Object> _properties = new ConcurrentHashMap<String, Object>();

    private final Lock _stateChangeLock;

    private static final AtomicLong idGenerator = new AtomicLong(0);
    // Create a simple ID that increments for ever new Subscription
    private final long _subscriptionID = idGenerator.getAndIncrement();
    private LogSubject _logSubject;
    private LogActor _logActor;
    private UUID _id;
    private final AtomicLong _deliveredCount = new AtomicLong(0);
    private long _createTime = System.currentTimeMillis();


    public static final class BrowserSubscription extends SubscriptionImpl
    {
        public BrowserSubscription(AMQChannel channel, AMQProtocolSession protocolSession,
                                   AMQShortString consumerTag, FieldTable filters,
                                   boolean noLocal, FlowCreditManager creditManager,
                                   ClientDeliveryMethod deliveryMethod,
                                   RecordDeliveryMethod recordMethod)
            throws AMQException
        {
            super(channel, protocolSession, consumerTag, filters, noLocal, creditManager, deliveryMethod, recordMethod);
        }


        public boolean isBrowser()
        {
            return true;
        }

        /**
         * This method can be called by each of the publisher threads. As a result all changes to the channel object must be
         * thread safe.
         *
         * @param msg   The message to send
         * @throws AMQException
         */
        @Override
        public void send(QueueEntry msg) throws AMQException
        {
            // We don't decrement the reference here as we don't want to consume the message
            // but we do want to send it to the client.

            synchronized (getChannel())
            {
                // Register the mapping between andes message Id and delivery protocol message Id
                AndesUtils.registerBrowserMessageId(msg.getMessageHeader().getMessageId(),
                        msg.getMessage().getMessageNumber());
                long deliveryTag = getChannel().getNextDeliveryTag();
                sendToClient(msg, deliveryTag);
            }

        }

        @Override
        public boolean wouldSuspend(QueueEntry msg)
        {
            return false;
        }

    }

    public static class NoAckSubscription extends SubscriptionImpl
    {
        public NoAckSubscription(AMQChannel channel, AMQProtocolSession protocolSession,
                                 AMQShortString consumerTag, FieldTable filters,
                                 boolean noLocal, FlowCreditManager creditManager,
                                   ClientDeliveryMethod deliveryMethod,
                                   RecordDeliveryMethod recordMethod)
            throws AMQException
        {
            super(channel, protocolSession, consumerTag, filters, noLocal, creditManager, deliveryMethod, recordMethod);
        }


        public boolean isBrowser()
        {
            return false;
        }

        @Override
        public boolean isExplicitAcknowledge()
        {
            return false;
        }

        /**
         * This method can be called by each of the publisher threads. As a result all changes to the channel object must be
         * thread safe.
         *
         * @param entry   The message to send
         * @throws AMQException
         */
        @Override
        public void send(QueueEntry entry) throws AMQException
        {
            // if we do not need to wait for client acknowledgements
            // we can decrement the reference count immediately.

            // By doing this _before_ the send we ensure that it
            // doesn't get sent if it can't be dequeued, preventing
            // duplicate delivery on recovery.

            // The send may of course still fail, in which case, as
            // the message is unacked, it will be lost.
            entry.dequeue();


            synchronized (getChannel())
            {
                long deliveryTag = getChannel().getNextDeliveryTag();

                sendToClient(entry, deliveryTag);

            }
            entry.dispose();


        }

        @Override
        public boolean wouldSuspend(QueueEntry msg)
        {
            return false;
        }

    }

    public static final class AckSubscription extends SubscriptionImpl
    {
        public AckSubscription(AMQChannel channel, AMQProtocolSession protocolSession,
                               AMQShortString consumerTag, FieldTable filters,
                               boolean noLocal, FlowCreditManager creditManager,
                                   ClientDeliveryMethod deliveryMethod,
                                   RecordDeliveryMethod recordMethod)
            throws AMQException
        {
            super(channel, protocolSession, consumerTag, filters, noLocal, creditManager, deliveryMethod, recordMethod);
        }


        public boolean isBrowser()
        {
            return false;
        }


        /**
         * This method can be called by each of the publisher threads. As a result all changes to
         * the channel object
         * must be
         * thread safe.
         *
         * @param entry The message to send
         * @throws AMQException
         */
        @Override
        public void send(QueueEntry entry) throws AMQException {

            try {

                long deliveryTag = getChannel().getNextDeliveryTag();

                /**
                 * This method will record that this message is delivered in AMQChannel level
                 * Requeueing and ack handling will be affected by this.
                 */
                recordMessageDelivery(entry, deliveryTag);

                //no point of trying to deliver if channel is closed ReQueue the message to
                // be resent
                // when channel is available
                if (getChannel().isClosing()) {
                    if ( log.isDebugEnabled()){
                        log.debug("channel getting closed therefore, not trying to deliver : "
                                + entry.getMessage().getMessageNumber()  );
                        
                    }
                    throw new SubscriptionAlreadyClosedException("Channel " + getChannel().getClientID() + " is already"
                            + " closed. Delivery failed message id = " + entry.getMessage().getMessageNumber());
                }

                if (log.isDebugEnabled()) {
                    log.debug("sent message : " + entry.getMessageHeader().getMessageId() + " JMSMessageId " + ": "
                            + entry.getMessageHeader().getMessageId() + " channel=" + getChannel().getChannelId());

                }

                sendToClient(entry, deliveryTag);

                //We do not need to keep the whole message as we save it in DB. As this can cause Out Of Memory issue
                //in a loaded environment
                entry.dispose();

            } catch (Exception e) {

                // Try and shed more light about the exact context of the error (only in debug mode)
                if (log.isDebugEnabled()) {
                    //undo any changes in the message tracker
                    log.warn("SEND FAILED >> Exception occurred while sending message out.");

                    if (e instanceof AMQException) {
                        if (((AMQException) e).getErrorCode().getCode() == AMQConstant
                                .MESSAGE_CONTENT_OBSOLETE.getCode()) {
                            log.debug("Message content for message id : " + entry.getMessage()
                                    .getMessageNumber() + " has been removed from store due to a " +
                                    "queue purge or a previous acknowledgement of the message.", e);
                        } else if (((AMQException) e).getErrorCode().getCode() == AMQConstant
                                .DELIVERY_TAG_REUSED.getCode()) {
                            log.debug("Delivery tag for message : " + entry.getMessage()
                                    .getMessageNumber() + " has been used earlier.", e);
                        }
                    } else {
                        // This is an unexpected scenario. Logged for reference.
                        log.debug("Unexpected error occurred when sending message : " + entry
                                .getMessage().getMessageNumber(), e);
                    }
                }

                throw new AMQException("SEND FAILED >> Exception occurred while sending message " +
                        "out message ID" + entry.getMessage().getMessageNumber()
                        + " " + e.getMessage(), e);
            }
        }
    }


    private static final Logger _logger = Logger.getLogger(SubscriptionImpl.class);

    private final AMQChannel _channel;

    private final AMQShortString _consumerTag;


    private boolean _noLocal;

    private final FlowCreditManager _creditManager;

    private FilterManager _filters;

    private final Boolean _autoClose;


    private static final String CLIENT_PROPERTIES_INSTANCE = ClientProperties.instance.toString();

    private AMQQueue _queue;
    private final AtomicBoolean _deleted = new AtomicBoolean(false);




    public SubscriptionImpl(AMQChannel channel , AMQProtocolSession protocolSession,
                            AMQShortString consumerTag, FieldTable arguments,
                            boolean noLocal, FlowCreditManager creditManager,
                            ClientDeliveryMethod deliveryMethod,
                            RecordDeliveryMethod recordMethod)
            throws AMQException
    {

        _channel = channel;
        _consumerTag = consumerTag;

        _creditManager = creditManager;
        creditManager.addStateListener(this);

        _noLocal = noLocal;


        _filters = FilterManagerFactory.createManager(arguments);

        _deliveryMethod = deliveryMethod;
        _recordMethod = recordMethod;


        _stateChangeLock = new ReentrantLock();


        if (arguments != null)
        {
            Object autoClose = arguments.get(AMQPFilterTypes.AUTO_CLOSE.getValue());
            if (autoClose != null)
            {
                _autoClose = (Boolean) autoClose;
            }
            else
            {
                _autoClose = false;
            }
        }
        else
        {
            _autoClose = false;
        }

    }

    public ConfigStore getConfigStore()
    {
        return getQueue().getConfigStore();
    }
    
    public Long getDelivered()
    {
        return _deliveredCount.get();
    }

    public synchronized void setQueue(AMQQueue queue, boolean exclusive)
    {
        if(getQueue() != null)
        {
            throw new IllegalStateException("Attempt to set queue for subscription " + this + " to " + queue + "when already set to " + getQueue());
        }
        _queue = queue;

        _id = getConfigStore().createId();
        getConfigStore().addConfiguredObject(this);

        _logSubject = new SubscriptionLogSubject(this);
        _logActor = new SubscriptionActor(CurrentActor.get().getRootMessageLogger(), this);

        if (CurrentActor.get().getRootMessageLogger().
                isMessageEnabled(CurrentActor.get(), _logSubject, SubscriptionMessages.CREATE_LOG_HIERARCHY))
        {
            // Get the string value of the filters
            String filterLogString = null;
            if (_filters != null && _filters.hasFilters())
            {
                filterLogString = _filters.toString();
            }

            if (isAutoClose())
            {
                if (filterLogString == null)
                {
                    filterLogString = "";
                }
                else
                {
                    filterLogString += ",";
                }
                filterLogString += "AutoClose";
            }

            if (isBrowser())
            {
                // We do not need to check for null here as all Browsers are AutoClose
                filterLogString +=",Browser";
            }

            CurrentActor.get().
                    message(_logSubject,
                            SubscriptionMessages.CREATE(filterLogString,
                                                          queue.isDurable() && exclusive,
                                                          filterLogString != null));
        }
    }

    public String toString()
    {
        String subscriber = "[channel=" + _channel +
                            ", consumerTag=" + _consumerTag +
                            ", session=" + getProtocolSession().getKey()  ;

        return subscriber + "]";
    }

    /**
     * This method can be called by each of the publisher threads. As a result all changes to the channel object must be
     * thread safe.
     *
     * @param msg   The message to send
     * @throws AMQException
     */
    abstract public void send(QueueEntry msg) throws AMQException;


    public boolean isSuspended()
    {
        return !isActive() || _channel.isSuspended() || _deleted.get();
    }

    /**
     * Callback indicating that a queue has been deleted.
     *
     * @param queue The queue to delete
     */
    public void queueDeleted(AMQQueue queue)
    {
        _deleted.set(true);
//        _channel.queueDeleted(queue);
    }

    public boolean filtersMessages()
    {
        return _filters != null || _noLocal;
    }

    public boolean hasInterest(QueueEntry entry)
    {




        //check that the message hasn't been rejected
        if (entry.isRejectedBy(this))
        {
            if (_logger.isDebugEnabled())
            {
                _logger.debug("Subscription:" + this + " rejected message:" + entry);
            }
//            return false;
        }

        if (_noLocal)
        {
         /* This noLocal case is handled in the upper layer (AMQPLocalSubscription)
            Commented below code for future references
         */

   /*         AMQMessage message = (AMQMessage) entry.getMessage();

            //todo - client id should be recorded so we don't have to handle
            // the case where this is null.
            final Object publisher = message.getPublisherIdentifier();

            // We don't want local messages so check to see if message is one we sent
            Object localInstance = getProtocolSession();

            if(publisher.equals(localInstance))
            {
                return false;
            }
*/

        }


        if (_logger.isDebugEnabled())
        {
            _logger.debug("(" + this + ") checking filters for message (" + entry);
        }
        return checkFilters(entry);

    }

    private String id = String.valueOf(System.identityHashCode(this));

    private String debugIdentity()
    {
        return id;
    }

    private boolean checkFilters(QueueEntry msg)
    {
        return (_filters == null) || _filters.allAllow(msg);
    }

    public boolean isAutoClose()
    {
        return _autoClose;
    }

    public FlowCreditManager getCreditManager()
    {
        return _creditManager;
    }


    public void close()
    {
        boolean closed = false;
        State state = getState();

        _stateChangeLock.lock();
        try
        {
            while(!closed && state != State.CLOSED)
            {
                closed = _state.compareAndSet(state, State.CLOSED);
                if(!closed)
                {
                    state = getState();
                }
                else
                {
                    _stateListener.stateChange(this,state, State.CLOSED);
                }
            }
            _creditManager.removeListener(this);
        }
        finally
        {
            _stateChangeLock.unlock();
        }
        getConfigStore().removeConfiguredObject(this);

        //Log Subscription closed
        CurrentActor.get().message(_logSubject, SubscriptionMessages.CLOSE());
    }

    public boolean isClosed()
    {
        return getState() == State.CLOSED;
    }


    public boolean wouldSuspend(QueueEntry msg)
    {
        return !_creditManager.useCreditForMessage(msg.getMessage());//_channel.wouldSuspend(msg.getMessage());
    }

    public void getSendLock()
    {
        _stateChangeLock.lock();
    }

    public void releaseSendLock()
    {
        _stateChangeLock.unlock();
    }

    public AMQChannel getChannel()
    {
        return _channel;
    }

    public AMQShortString getConsumerTag()
    {
        return _consumerTag;
    }

    public long getSubscriptionID()
    {
        return _subscriptionID;
    }

    public AMQProtocolSession getProtocolSession()
    {
        return _channel.getProtocolSession();
    }

    public LogActor getLogActor()
    {
        return _logActor;
    }

    public AMQQueue getQueue()
    {
        return _queue;
    }

    public void onDequeue(final QueueEntry queueEntry) {
        restoreCredit(queueEntry);
    }

    public void restoreCredit(final QueueEntry queueEntry)
    {
        _creditManager.restoreCredit(1, queueEntry.getSize());
    }



    public void creditStateChanged(boolean hasCredit)
    {

        if(hasCredit)
        {
            if(_state.compareAndSet(State.SUSPENDED, State.ACTIVE))
            {
                _stateListener.stateChange(this, State.SUSPENDED, State.ACTIVE);
            }
            else
            {
                // this is a hack to get round the issue of increasing bytes credit
                _stateListener.stateChange(this, State.ACTIVE, State.ACTIVE);
            }
        }
        else
        {
            if(_state.compareAndSet(State.ACTIVE, State.SUSPENDED))
            {
                _stateListener.stateChange(this, State.ACTIVE, State.SUSPENDED);
            }
        }
        CurrentActor.get().message(_logSubject,SubscriptionMessages.STATE(_state.get().toString()));
    }

    public State getState()
    {
        return _state.get();
    }


    public void setStateListener(final StateListener listener)
    {
        _stateListener = listener;
    }


    public AMQQueue.Context getQueueContext()
    {
        return _queueContext;
    }

    public void setQueueContext(AMQQueue.Context context)
    {
        _queueContext = context;
    }


    protected void sendToClient(final QueueEntry entry, final long deliveryTag)
            throws AMQException
    {
        _deliveryMethod.deliverToClient(this,entry,deliveryTag);
        _deliveredCount.incrementAndGet();
    }


    protected void recordMessageDelivery(final QueueEntry entry, final long deliveryTag)
    {
        _recordMethod.recordMessageDelivery(this,entry,deliveryTag);
    }


    public boolean isActive()
    {
        return getState() == State.ACTIVE;
    }

    public QueueEntry.SubscriptionAcquiredState getOwningState()
    {
        return _owningState;
    }

    public QueueEntry.SubscriptionAssignedState getAssignedState()
    {
        return _assignedState;
    }


    public void confirmAutoClose()
    {
        ProtocolOutputConverter converter = getChannel().getProtocolSession().getProtocolOutputConverter();
        converter.confirmConsumerAutoClose(getChannel().getChannelId(), getConsumerTag());
    }

    public boolean acquires()
    {
        return !isBrowser();
    }

    public boolean seesRequeues()
    {
        return !isBrowser();
    }

    public boolean isTransient()
    {
        return false;
    }

    public void set(String key, Object value)
    {
        _properties.put(key, value);
    }

    public Object get(String key)
    {
        return _properties.get(key);
    }


    public void setNoLocal(boolean noLocal)
    {
        _noLocal = noLocal;
    }

    abstract boolean isBrowser();

    public String getCreditMode()
    {
        return "WINDOW";
    }

    public SessionConfig getSessionConfig()
    {
        return getChannel();
    }

    public boolean isBrowsing()
    {
        return isBrowser();
    }

    public boolean isExplicitAcknowledge()
    {
        return true;
    }

    public UUID getId()
    {
        return _id;
    }

    public boolean isDurable()
    {
        return false;
    }

    public SubscriptionConfigType getConfigType()
    {
        return SubscriptionConfigType.getInstance();
    }

    public boolean isExclusive()
    {
        return getQueue().hasExclusiveSubscriber();
    }

    public ConfiguredObject getParent()
    {
        return getSessionConfig();
    }

    public String getName()
    {
        return String.valueOf(_consumerTag);
    }

    public Map<String, Object> getArguments()
    {
        return null;
    }

    public boolean isSessionTransactional()
    {
        return _channel.isTransactional();
    }
    
    public long getCreateTime()
    {
        return _createTime;
    }

    @Override
    public boolean isNoLocal() {
        return _noLocal;
    }
}
