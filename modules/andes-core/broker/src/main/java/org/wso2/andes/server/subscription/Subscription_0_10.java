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
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.amqp.QpidAMQPBridge;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.BasicContentHeaderProperties;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.binding.*;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.configuration.*;
import org.wso2.andes.server.filter.FilterManager;
import org.wso2.andes.server.flow.CreditCreditManager;
import org.wso2.andes.server.flow.FlowCreditManager;
import org.wso2.andes.server.flow.FlowCreditManager_0_10;
import org.wso2.andes.server.flow.WindowCreditManager;
import org.wso2.andes.server.logging.LogActor;
import org.wso2.andes.server.logging.LogSubject;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.actors.GenericActor;
import org.wso2.andes.server.logging.messages.SubscriptionMessages;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.message.MessageTransferMessage;
import org.wso2.andes.server.message.ServerMessage;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.transport.ServerSession;
import org.wso2.andes.server.txn.AutoCommitTransaction;
import org.wso2.andes.server.txn.ServerTransaction;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.transport.*;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.wso2.andes.server.logging.subjects.LogSubjectFormat.QUEUE_FORMAT;
import static org.wso2.andes.server.logging.subjects.LogSubjectFormat.SUBSCRIPTION_FORMAT;

public class Subscription_0_10 implements Subscription, FlowCreditManager.FlowCreditManagerListener, SubscriptionConfig, LogSubject
{

    private static final AtomicLong idGenerator = new AtomicLong(0);
    // Create a simple ID that increments for ever new Subscription
    private final long _subscriptionID = idGenerator.getAndIncrement();

    private final QueueEntry.SubscriptionAcquiredState _owningState = new QueueEntry.SubscriptionAcquiredState(this);
    private final QueueEntry.SubscriptionAssignedState _assignedState = new QueueEntry.SubscriptionAssignedState(this);

    private final Lock _stateChangeLock = new ReentrantLock();

    private final AtomicReference<State> _state = new AtomicReference<State>(State.ACTIVE);
    private AMQQueue.Context _queueContext;
    private final AtomicBoolean _deleted = new AtomicBoolean(false);


    private FlowCreditManager_0_10 _creditManager;

    private StateListener _stateListener = new StateListener()
                                            {

                                                public void stateChange(Subscription sub, State oldState, State newState)
                                                {
                                                    CurrentActor.get().message(SubscriptionMessages.STATE(newState.toString()));    
                                                }
                                            };
    private AMQQueue _queue;
    private final String _destination;
    private boolean _noLocal;
    private final FilterManager _filters;
    private final MessageAcceptMode _acceptMode;
    private final MessageAcquireMode _acquireMode;
    private MessageFlowMode _flowMode;
    private final ServerSession _session;
    private AtomicBoolean _stopped = new AtomicBoolean(true);
    private ConcurrentHashMap<Integer, QueueEntry> _sentMap = new ConcurrentHashMap<Integer, QueueEntry>();
    private static final Struct[] EMPTY_STRUCT_ARRAY = new Struct[0];

    private LogActor _logActor;
    private Map<String, Object> _properties = new ConcurrentHashMap<String, Object>();
    private UUID _id;
    private String _traceExclude;
    private String _trace;
    private long _createTime = System.currentTimeMillis();
    private final AtomicLong _deliveredCount = new AtomicLong(0);
    private final Map<String, Object> _arguments;


    public Subscription_0_10(ServerSession session, String destination, MessageAcceptMode acceptMode,
                             MessageAcquireMode acquireMode,
                             MessageFlowMode flowMode,
                             FlowCreditManager_0_10 creditManager,
                             FilterManager filters,Map<String, Object> arguments)
    {
        _session = session;
        _destination = destination;
        _acceptMode = acceptMode;
        _acquireMode = acquireMode;
        _creditManager = creditManager;
        _flowMode = flowMode;
        _filters = filters;
        _creditManager.addStateListener(this);
        _arguments = arguments == null ? Collections.<String, Object> emptyMap() :
                                         Collections.<String, Object> unmodifiableMap(arguments);
        _state.set(_creditManager.hasCredit() ? State.ACTIVE : State.SUSPENDED);

    }

    public void setNoLocal(boolean noLocal)
    {
        _noLocal = noLocal;
    }

    public AMQQueue getQueue()
    {
        return _queue;
    }

    public QueueEntry.SubscriptionAcquiredState getOwningState()
    {
        return _owningState;
    }

    public QueueEntry.SubscriptionAssignedState getAssignedState()
    {
        return _assignedState;
    }

    public void setQueue(AMQQueue queue, boolean exclusive)
    {
        if(getQueue() != null)
        {
            throw new IllegalStateException("Attempt to set queue for subscription " + this + " to " + queue + "when already set to " + getQueue());
        }
        _queue = queue;
        Map<String, Object> arguments = queue.getArguments() == null ? Collections.EMPTY_MAP : queue.getArguments();
        _traceExclude = (String) arguments.get("qpid.trace.exclude");
        _trace = (String) arguments.get("qpid.trace.id");
        _id = getConfigStore().createId();
        getConfigStore().addConfiguredObject(this);
        String filterLogString = null;

        _logActor = GenericActor.getInstance(this);
        if (CurrentActor.get().getRootMessageLogger().isMessageEnabled(_logActor, this, SubscriptionMessages.CREATE_LOG_HIERARCHY))
        {
            filterLogString = getFilterLogString();
            CurrentActor.get().message(this, SubscriptionMessages.CREATE(filterLogString, queue.isDurable() && exclusive,
                    filterLogString.length() > 0));
        }
 
    }

    public AMQShortString getConsumerTag()
    {
        return new AMQShortString(_destination);
    }

    public boolean isSuspended()
    {
        return !isActive() || _deleted.get(); // TODO check for Session suspension
    }

    public boolean hasInterest(QueueEntry entry)
    {



        //check that the message hasn't been rejected
        if (entry.isRejectedBy(this))
        {

            return false;
        }



        if (_noLocal
            && (entry.getMessage() instanceof MessageTransferMessage)
            && ((MessageTransferMessage)entry.getMessage()).getSession() == _session)
        {
            return false;
        }


        return checkFilters(entry);


    }

    private boolean checkFilters(QueueEntry entry)
    {
        return (_filters == null) || _filters.allAllow(entry);
    }

    public boolean isAutoClose()
    {
        // no such thing in 0-10
        return false;
    }

    public boolean isClosed()
    {
        return getState() == State.CLOSED;
    }

    public boolean isBrowser()
    {
        return _acquireMode == MessageAcquireMode.NOT_ACQUIRED;
    }

    public boolean seesRequeues()
    {
        return _acquireMode != MessageAcquireMode.NOT_ACQUIRED || _acceptMode == MessageAcceptMode.EXPLICIT;
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
            getConfigStore().removeConfiguredObject(this);
            CurrentActor.get().message(getLogSubject(), SubscriptionMessages.CLOSE());
        }
        finally
        {
            _stateChangeLock.unlock();
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
    }


    private class AddMessageDispositionListnerAction implements Runnable
    {
        public MessageTransfer _xfr;
        public ServerSession.MessageDispositionChangeListener _action;

        public void run()
        {
            if(_action != null)
            {
                _session.onMessageDispositionChange(_xfr, _action);
            }
        }
    }

    private final AddMessageDispositionListnerAction _postIdSettingAction = new AddMessageDispositionListnerAction();

    public void send(final QueueEntry entry) throws AMQException
    {
        ServerMessage serverMsg = entry.getMessage();


        MessageTransfer xfr;

        DeliveryProperties deliveryProps;
        MessageProperties messageProps = null;

        if(serverMsg instanceof MessageTransferMessage)
        {

            MessageTransferMessage msg = (MessageTransferMessage) serverMsg;


            Struct[] headers;
            if(msg.getHeader() == null)
            {
                headers = EMPTY_STRUCT_ARRAY;
            }
            else
            {
                headers = msg.getHeader().getStructs();
            }

            ArrayList<Struct> newHeaders = new ArrayList<Struct>(headers.length);
            DeliveryProperties origDeliveryProps = null;
            for(Struct header : headers)
            {
                if(header instanceof DeliveryProperties)
                {
                    origDeliveryProps = (DeliveryProperties) header;
                }
                else
                {
                    if(header instanceof MessageProperties)
                    {
                        messageProps = (MessageProperties) header;
                    }
                    newHeaders.add(header);
                }
            }

            deliveryProps = new DeliveryProperties();
            if(origDeliveryProps != null)
            {
                if(origDeliveryProps.hasDeliveryMode())
                {
                    deliveryProps.setDeliveryMode(origDeliveryProps.getDeliveryMode());
                }
                if(origDeliveryProps.hasExchange())
                {
                    deliveryProps.setExchange(origDeliveryProps.getExchange());
                }
                if(origDeliveryProps.hasExpiration())
                {
                    deliveryProps.setExpiration(origDeliveryProps.getExpiration());
                }
                if(origDeliveryProps.hasPriority())
                {
                    deliveryProps.setPriority(origDeliveryProps.getPriority());
                }
                if(origDeliveryProps.hasRoutingKey())
                {
                    deliveryProps.setRoutingKey(origDeliveryProps.getRoutingKey());
                }
                if(origDeliveryProps.hasTimestamp())
                {
                    deliveryProps.setTimestamp(origDeliveryProps.getTimestamp());
                }


            }

            deliveryProps.setRedelivered(entry.isRedelivered());

            newHeaders.add(deliveryProps);

            if(_trace != null && messageProps == null)
            {
                messageProps = new MessageProperties();
                newHeaders.add(messageProps);
            }

            Header header = new Header(newHeaders);

            xfr = new MessageTransfer(_destination,_acceptMode,_acquireMode,header,msg.getBody());
        }
        else if(serverMsg instanceof AMQMessage)
        {
            AMQMessage message_0_8 = (AMQMessage) serverMsg;
            deliveryProps = new DeliveryProperties();
            messageProps = new MessageProperties();

            int size = (int) message_0_8.getSize();
            ByteBuffer body = ByteBuffer.allocate(size);
            message_0_8.getContent(body, 0);
            body.flip();

            Struct[] headers = new Struct[] { deliveryProps, messageProps };

            BasicContentHeaderProperties properties =
                    (BasicContentHeaderProperties) message_0_8.getContentHeaderBody().getProperties();
            final AMQShortString exchange = message_0_8.getMessagePublishInfo().getExchange();
            if(exchange != null)
            {
                deliveryProps.setExchange(exchange.toString());
            }
            deliveryProps.setExpiration(message_0_8.getExpiration());
            deliveryProps.setImmediate(message_0_8.isImmediate());
            deliveryProps.setPriority(MessageDeliveryPriority.get(properties.getPriority()));
            deliveryProps.setRedelivered(entry.isRedelivered());
            deliveryProps.setRoutingKey(message_0_8.getRoutingKey());
            deliveryProps.setTimestamp(properties.getTimestamp());

            messageProps.setContentEncoding(properties.getEncodingAsString());
            messageProps.setContentLength(size);
            if(properties.getAppId() != null)
            {
                messageProps.setAppId(properties.getAppId().getBytes());
            }
            messageProps.setContentType(properties.getContentTypeAsString());
            if(properties.getCorrelationId() != null)
            {
                messageProps.setCorrelationId(properties.getCorrelationId().getBytes());
            }

            // TODO - ReplyTo

            if(properties.getUserId() != null)
            {
                messageProps.setUserId(properties.getUserId().getBytes());
            }

            FieldTable fieldTable = properties.getHeaders();

            final Map<String, Object> appHeaders = FieldTable.convertToMap(fieldTable);


            messageProps.setApplicationHeaders(appHeaders);

            Header header = new Header(headers);
            xfr = new MessageTransfer(_destination,_acceptMode,_acquireMode,header, body);
        }
        else
        {

            deliveryProps = new DeliveryProperties();
            messageProps = new MessageProperties();

            int size = (int) serverMsg.getSize();
            ByteBuffer body = ByteBuffer.allocate(size);
            serverMsg.getContent(body, 0);
            body.flip();

            Struct[] headers = new Struct[] { deliveryProps, messageProps };


            deliveryProps.setExpiration(serverMsg.getExpiration());
            deliveryProps.setImmediate(serverMsg.isImmediate());
            deliveryProps.setPriority(MessageDeliveryPriority.get(serverMsg.getMessageHeader().getPriority()));
            deliveryProps.setRedelivered(entry.isRedelivered());
            deliveryProps.setRoutingKey(serverMsg.getRoutingKey());
            deliveryProps.setTimestamp(serverMsg.getMessageHeader().getTimestamp());

            messageProps.setContentEncoding(serverMsg.getMessageHeader().getEncoding());
            messageProps.setContentLength(size);
            messageProps.setContentType(serverMsg.getMessageHeader().getMimeType());
            if(serverMsg.getMessageHeader().getCorrelationId() != null)
            {
                messageProps.setCorrelationId(serverMsg.getMessageHeader().getCorrelationId().getBytes());
            }


            // TODO - ReplyTo


            final Map<String, Object> appHeaders = new HashMap<String, Object>();

            /*properties.getHeaders().processOverElements(
                    new FieldTable.FieldTableElementProcessor()
                    {

                        public boolean processElement(String propertyName, AMQTypedValue value)
                        {
                            Object val = value.getValue();
                            if(val instanceof AMQShortString)
                            {
                                val = val.toString();
                            }
                            appHeaders.put(propertyName, val);
                            return true;
                        }

                        public Object getResult()
                        {
                            return appHeaders;
                        }
                    });


            messageProps.setApplicationHeaders(appHeaders);
*/
            Header header = new Header(headers);
            xfr = new MessageTransfer(_destination,_acceptMode,_acquireMode,header, body);
        }

        boolean excludeDueToFederation = false;

        if(_trace != null)
        {
            if(!messageProps.hasApplicationHeaders())
            {
                messageProps.setApplicationHeaders(new HashMap<String,Object>());
            }
            Map<String,Object> appHeaders = messageProps.getApplicationHeaders();
            String trace = (String) appHeaders.get("x-qpid.trace");
            if(trace == null)
            {
                trace = _trace;
            }
            else
            {
                if(_traceExclude != null)
                {
                    excludeDueToFederation = Arrays.asList(trace.split(",")).contains(_traceExclude);
                }
                trace+=","+_trace;
            }
            appHeaders.put("x-qpid.trace",trace);
        }

        if(!excludeDueToFederation)
        {
            if(_acceptMode == MessageAcceptMode.NONE && _acquireMode != MessageAcquireMode.PRE_ACQUIRED)
            {
                xfr.setCompletionListener(new MessageAcceptCompletionListener(this, _session, entry, _flowMode == MessageFlowMode.WINDOW));
            }
            else if(_flowMode == MessageFlowMode.WINDOW)
            {
                xfr.setCompletionListener(new Method.CompletionListener()
                                            {
                                                public void onComplete(Method method)
                                                {
                                                    restoreCredit(entry);
                                                }
                                            });
            }


            _postIdSettingAction._xfr = xfr;
            if(_acceptMode == MessageAcceptMode.EXPLICIT)
            {
                _postIdSettingAction._action = new ExplicitAcceptDispositionChangeListener(entry, this);
            }
            else if(_acquireMode != MessageAcquireMode.PRE_ACQUIRED)
            {
                _postIdSettingAction._action = new ImplicitAcceptDispositionChangeListener(entry, this);
            }
            else
            {
                _postIdSettingAction._action = null;
            }

            _session.sendMessage(xfr, _postIdSettingAction);
            _deliveredCount.incrementAndGet();
            if(_acceptMode == MessageAcceptMode.NONE && _acquireMode == MessageAcquireMode.PRE_ACQUIRED)
            {
                forceDequeue(entry, false);
            }
        }
        else
        {
            forceDequeue(entry, _flowMode == MessageFlowMode.WINDOW);

        }
    }

    private void forceDequeue(final QueueEntry entry, final boolean restoreCredit)
    {
        ServerTransaction txn = new AutoCommitTransaction(getQueue().getVirtualHost().getTransactionLog());
        txn.dequeue(entry.getQueue(),entry.getMessage(),
                                new ServerTransaction.Action()
                            {
                                public void postCommit()
                                {
                                    if(restoreCredit)
                                    {
                                        restoreCredit(entry);
                                    }
                                    entry.discard();
                                }

                                public void onRollback()
                                {

                                }
                            });
    }

    void reject(QueueEntry entry)
    {
        entry.setRedelivered();
        entry.routeToAlternate();

    }

    void release(QueueEntry entry)
    {
        entry.setRedelivered();
        entry.release();
    }

    public void queueDeleted(AMQQueue queue)
    {
        _deleted.set(true);
    }

    public boolean wouldSuspend(QueueEntry msg)
    {
        return !_creditManager.useCreditForMessage(msg.getMessage());
    }

    public void getSendLock()
    {
        _stateChangeLock.lock();
    }

    public void releaseSendLock()
    {
        _stateChangeLock.unlock();
    }

    public void restoreCredit(QueueEntry queueEntry)
    {
        _creditManager.restoreCredit(1, queueEntry.getSize());
    }

    public void onDequeue(QueueEntry queueEntry) {
    }

    public void setStateListener(StateListener listener)
    {
        _stateListener = listener;
    }

    public State getState()
    {
        return _state.get();
    }

    public AMQQueue.Context getQueueContext()
    {
        return _queueContext;
    }

    public void setQueueContext(AMQQueue.Context queueContext)
    {
        _queueContext = queueContext;
    }

    public boolean isActive()
    {
        return getState() == State.ACTIVE;
    }

    public void confirmAutoClose()
    {
        //No such thing in 0-10
    }

    public void set(String key, Object value)
    {
        _properties.put(key, value);
    }

    public Object get(String key)
    {
        return _properties.get(key);
    }


    public FlowCreditManager_0_10 getCreditManager()
    {
        return _creditManager;
    }


    public void stop()
    {
        if(_state.compareAndSet(State.ACTIVE, State.SUSPENDED))
        {
            _stateListener.stateChange(this, State.ACTIVE, State.SUSPENDED);
        }
        _stopped.set(true);
        FlowCreditManager_0_10 creditManager = getCreditManager();
        creditManager.clearCredit();
    }

    public void addCredit(MessageCreditUnit unit, long value)
    {
        FlowCreditManager_0_10 creditManager = getCreditManager();

        switch (unit)
        {
            case MESSAGE:

                creditManager.addCredit(value, 0L);
                break;
            case BYTE:
                creditManager.addCredit(0l, value);
                break;
        }

        _stopped.set(false);

        if(creditManager.hasCredit())
        {
            if(_state.compareAndSet(State.SUSPENDED, State.ACTIVE))
            {
                _stateListener.stateChange(this, State.SUSPENDED, State.ACTIVE);
            }
        }

    }

    public void setFlowMode(MessageFlowMode flowMode)
    {


        _creditManager.removeListener(this);

        switch(flowMode)
        {
            case CREDIT:
                _creditManager = new CreditCreditManager(0l,0l);
                break;
            case WINDOW:
                _creditManager = new WindowCreditManager(0l,0l);
                break;
            default:
                throw new RuntimeException("Unknown message flow mode: " + flowMode);
        }
        _flowMode = flowMode;
        if(_state.compareAndSet(State.ACTIVE, State.SUSPENDED))
        {
            _stateListener.stateChange(this, State.ACTIVE, State.SUSPENDED);
        }

        _creditManager.addStateListener(this);

    }

    public boolean isStopped()
    {
        return _stopped.get();
    }

    public boolean acquires()
    {
        return _acquireMode == MessageAcquireMode.PRE_ACQUIRED;
    }

    public void acknowledge(QueueEntry entry)
    {
        // TODO Fix Store Context / cleanup
        if(entry.isAcquiredBy(this))
        {
            entry.discard();
        }
    }

    public void flush() throws AMQException
    {
        _queue.flushSubscription(this);
        stop();
    }

    public long getSubscriptionID()
    {
        return _subscriptionID;
    }

    public LogActor getLogActor()
    {
        return _logActor;
    }

    public boolean isTransient()
    {
        return false;
    }

    ServerSession getSession()
    {
        return _session;
    }


    public SessionConfig getSessionConfig()
    {
        return getSession();
    }

    public boolean isBrowsing()
    {
        return _acquireMode == MessageAcquireMode.NOT_ACQUIRED;
    }

    public boolean isExclusive()
    {
        return getQueue().hasExclusiveSubscriber();
    }

    public ConfiguredObject getParent()
    {
        return getSessionConfig();
    }

    public boolean isDurable()
    {
        return false;
    }

    public SubscriptionConfigType getConfigType()
    {
        return SubscriptionConfigType.getInstance();
    }

    public boolean isExplicitAcknowledge()
    {
        return _acceptMode == MessageAcceptMode.EXPLICIT;
    }

    public String getCreditMode()
    {
        return _flowMode.toString();
    }

    public UUID getId()
    {
        return _id;
    }

    public String getName()
    {
        return _destination;
    }

    public Map<String, Object> getArguments()
    {
        return _arguments;
    }

    public boolean isSessionTransactional()
    {
        return _session.isTransactional();
    }

    public long getCreateTime()
    {
        return _createTime;
    }

    public String toLogString()
    {
        String queueInfo = MessageFormat.format(QUEUE_FORMAT, _queue.getVirtualHost().getName(), 
                  _queue.getNameShortString());
        String result = "[" + MessageFormat.format(SUBSCRIPTION_FORMAT, getSubscriptionID()) + "("
                // queueString is "vh(/{0})/qu({1}) " so need to trim
                + queueInfo.substring(0, queueInfo.length() - 1) + ")" + "] ";
        return result;
    }

    private String getFilterLogString()
    {
        StringBuilder filterLogString = new StringBuilder();
        String delimiter = ", ";
        boolean hasEntries = false;
        if (_filters != null && _filters.hasFilters())
        {
            filterLogString.append(_filters.toString());
            hasEntries = true;
        }

        if (isBrowser())
        {
            if (hasEntries)
            {
                filterLogString.append(delimiter);
            }
            filterLogString.append("Browser");
            hasEntries = true;
        }

        if (isDurable())
        {
            if (hasEntries)
            {
                filterLogString.append(delimiter);
            }
            filterLogString.append("Durable");
            hasEntries = true;
        }

        return filterLogString.toString();
    }

    public LogSubject getLogSubject()
    {
        return (LogSubject) this;
    }

}
