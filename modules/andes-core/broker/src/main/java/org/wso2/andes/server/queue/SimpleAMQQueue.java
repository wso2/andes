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
import org.wso2.andes.AMQSecurityException;
import org.wso2.andes.amqp.AMQPAuthorizationManager;
import org.wso2.andes.amqp.QpidAndesBridge;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.configuration.qpid.*;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.pool.ReadWriteRunnable;
import org.wso2.andes.pool.ReferenceCountingExecutorService;
import org.wso2.andes.security.AuthorizeAction;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.configuration.qpid.plugins.ConfigurationPlugin;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.logging.LogActor;
import org.wso2.andes.server.logging.LogSubject;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.actors.QueueActor;
import org.wso2.andes.server.logging.messages.QueueMessages;
import org.wso2.andes.server.logging.subjects.QueueLogSubject;
import org.wso2.andes.server.management.ManagedObject;
import org.wso2.andes.server.message.ServerMessage;
import org.wso2.andes.server.protocol.AMQSessionModel;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.security.AuthorizationHolder;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionImpl;
import org.wso2.andes.server.subscription.SubscriptionList;
import org.wso2.andes.server.txn.AutoCommitTransaction;
import org.wso2.andes.server.txn.LocalTransaction;
import org.wso2.andes.server.txn.ServerTransaction;
import org.wso2.andes.kernel.AndesConstants;
import org.wso2.andes.server.virtualhost.VirtualHost;

import javax.management.JMException;
import javax.security.auth.Subject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SimpleAMQQueue implements AMQQueue, Subscription.StateListener
{
    private static final Logger _logger = Logger.getLogger(SimpleAMQQueue.class);


    private final VirtualHost _virtualHost;

    private final AMQShortString _name;
    private final String _resourceName;

    /** null means shared */
    private final AMQShortString _owner;

    private AuthorizationHolder _authorizationHolder;

    private boolean _exclusive = false;
    private AMQSessionModel _exclusiveOwner;


    private final boolean _durable;

    /** If true, this queue is deleted when the last subscriber is removed */
    private final boolean _autoDelete;

    private Exchange _alternateExchange;

    /** Used to track bindings to exchanges so that on deletion they can easily be cancelled. */



    protected final QueueEntryList _entries;

    protected final SubscriptionList _subscriptionList = new SubscriptionList(this);

    private final AtomicReference<SubscriptionList.SubscriptionNode> _lastSubscriptionNode = new AtomicReference<SubscriptionList.SubscriptionNode>(_subscriptionList.getHead());

    private volatile Subscription _exclusiveSubscriber;

    private ProtocolType _protocolType;



    private final AtomicInteger _atomicQueueCount = new AtomicInteger(0);

    private final AtomicLong _atomicQueueSize = new AtomicLong(0L);

    private final AtomicInteger _activeSubscriberCount = new AtomicInteger();

    private final AtomicLong _totalMessagesReceived = new AtomicLong();

    private final AtomicLong _dequeueCount = new AtomicLong();
    private final AtomicLong _dequeueSize = new AtomicLong();
    private final AtomicLong _enqueueSize = new AtomicLong();
    private final AtomicLong _persistentMessageEnqueueSize = new AtomicLong();
    private final AtomicLong _persistentMessageDequeueSize = new AtomicLong();
    private final AtomicLong _persistentMessageEnqueueCount = new AtomicLong();
    private final AtomicLong _persistentMessageDequeueCount = new AtomicLong();
    private final AtomicInteger _counsumerCountHigh = new AtomicInteger(0);
    private final AtomicLong _msgTxnEnqueues = new AtomicLong(0);
    private final AtomicLong _byteTxnEnqueues = new AtomicLong(0);
    private final AtomicLong _msgTxnDequeues = new AtomicLong(0);
    private final AtomicLong _byteTxnDequeues = new AtomicLong(0);
    private final AtomicLong _unackedMsgCount = new AtomicLong(0);
    private final AtomicLong _unackedMsgCountHigh = new AtomicLong(0);

    private final AtomicInteger _bindingCountHigh = new AtomicInteger();

    /** max allowed size(KB) of a single message */
    public long _maximumMessageSize = ApplicationRegistry.getInstance().getConfiguration().getMaximumMessageSize();

    /** max allowed number of messages on a queue. */
    public long _maximumMessageCount = ApplicationRegistry.getInstance().getConfiguration().getMaximumMessageCount();

    /** max queue depth for the queue */
    public long _maximumQueueDepth = ApplicationRegistry.getInstance().getConfiguration().getMaximumQueueDepth();

    /** maximum message age before alerts occur */
    public long _maximumMessageAge = ApplicationRegistry.getInstance().getConfiguration().getMaximumMessageAge();

    /** the minimum interval between sending out consecutive alerts of the same type */
    public long _minimumAlertRepeatGap = ApplicationRegistry.getInstance().getConfiguration().getMinimumAlertRepeatGap();

    private long _capacity = ApplicationRegistry.getInstance().getConfiguration().getCapacity();

    private long _flowResumeCapacity = ApplicationRegistry.getInstance().getConfiguration().getFlowResumeCapacity();

    private final Set<NotificationCheck> _notificationChecks = EnumSet.noneOf(NotificationCheck.class);


    static final int MAX_ASYNC_DELIVERIES = 10;


    private final AtomicLong _stateChangeCount = new AtomicLong(Long.MIN_VALUE);
    private AtomicReference<Runnable> _asynchronousRunner = new AtomicReference<Runnable>(null);
    private final Executor _asyncDelivery;
    private AtomicInteger _deliveredMessages = new AtomicInteger();
    private AtomicBoolean _stopped = new AtomicBoolean(false);

    private final ConcurrentMap<AMQChannel, Boolean> _blockedChannels = new ConcurrentHashMap<AMQChannel, Boolean>();

    private final AtomicBoolean _deleted = new AtomicBoolean(false);
    private final List<Task> _deleteTaskList = new CopyOnWriteArrayList<Task>();


    private LogSubject _logSubject;
    private LogActor _logActor;

    private AMQQueueMBean _managedObject;
    private static final String SUB_FLUSH_RUNNER = "SUB_FLUSH_RUNNER";
    private boolean _nolocal;

    private final AtomicBoolean _overfull = new AtomicBoolean(false);
    private boolean _deleteOnNoConsumers;
    private final CopyOnWriteArrayList<Binding> _bindings = new CopyOnWriteArrayList<Binding>();
    private UUID _id;
    private final Map<String, Object> _arguments;

    //TODO : persist creation time
    private long _createTime = System.currentTimeMillis();
    private ConfigurationPlugin _queueConfiguration;



    protected SimpleAMQQueue(AMQShortString name, boolean durable, AMQShortString owner, boolean autoDelete, boolean exclusive, VirtualHost virtualHost, Map<String,Object> arguments)
    {
        this(name, durable, owner, autoDelete, exclusive, virtualHost,new SimpleQueueEntryList.Factory(), arguments);
    }

    public SimpleAMQQueue(String queueName, boolean durable, String owner, boolean autoDelete, boolean exclusive, VirtualHost virtualHost, Map<String, Object> arguments)
    {
        this(queueName, durable, owner, autoDelete, exclusive, virtualHost, new SimpleQueueEntryList.Factory(), arguments);
    }

    public SimpleAMQQueue(String queueName, boolean durable, String owner, boolean autoDelete, boolean exclusive, VirtualHost virtualHost, QueueEntryListFactory entryListFactory, Map<String, Object> arguments)
    {
        this(queueName == null ? null : new AMQShortString(queueName), durable, owner == null ? null : new AMQShortString(owner), autoDelete, exclusive, virtualHost, entryListFactory, arguments);
    }

    protected SimpleAMQQueue(AMQShortString name,
                             boolean durable,
                             AMQShortString owner,
                             boolean autoDelete,
                             boolean exclusive,
                             VirtualHost virtualHost,
                             QueueEntryListFactory entryListFactory,
                             Map<String,Object> arguments)
    {

        if (name == null)
        {
            throw new IllegalArgumentException("Queue name must not be null");
        }

        if (virtualHost == null)
        {
            throw new IllegalArgumentException("Virtual Host must not be null");
        }

        _name = name;
        _resourceName = String.valueOf(name);
        _durable = durable;
        _owner = owner;
        _autoDelete = autoDelete;
        _exclusive = exclusive;
        _virtualHost = virtualHost;
        _entries = entryListFactory.createQueueEntryList(this);
        _arguments = arguments;

        _id = virtualHost.getConfigStore().createId();

        _asyncDelivery = ReferenceCountingExecutorService.getInstance().acquireExecutorService();

        _logSubject = new QueueLogSubject(this);
        _logActor = new QueueActor(this, CurrentActor.get().getRootMessageLogger());

        // Log the correct creation message

        // Extract the number of priorities for this Queue.
        // Leave it as 0 if we are a SimpleQueueEntryList
        int priorities = 0;
        if (entryListFactory instanceof PriorityQueueList.Factory)
        {
            priorities = ((PriorityQueueList)_entries).getPriorities();
        }

        // Log the creation of this Queue.
        // The priorities display is toggled on if we set priorities > 0
        CurrentActor.get().message(_logSubject,
                                   QueueMessages.CREATED(String.valueOf(_owner),
                                                          priorities,
                                                          _owner != null,
                                                          autoDelete,
                                                          durable, !durable,
                                                          priorities > 0));

        getConfigStore().addConfiguredObject(this);

        try
        {
            _managedObject = new AMQQueueMBean(this);
            _managedObject.register();
        }
        catch (JMException e)
        {
            _logger.error("AMQQueue MBean creation has failed ", e);
        }

        resetNotifications();

    }

    public void resetNotifications()
    {
        // This ensure that the notification checks for the configured alerts are created.
        setMaximumMessageAge(_maximumMessageAge);
        setMaximumMessageCount(_maximumMessageCount);
        setMaximumMessageSize(_maximumMessageSize);
        setMaximumQueueDepth(_maximumQueueDepth);
    }

    // ------ Getters and Setters

    public void execute(ReadWriteRunnable runnable)
    {
        _asyncDelivery.execute(runnable);
    }

    public AMQShortString getNameShortString()
    {
        return _name;
    }

    public void setNoLocal(boolean nolocal)
    {
        _nolocal = nolocal;
    }

    public UUID getId()
    {
        return _id;
    }

    public QueueConfigType getConfigType()
    {
        return QueueConfigType.getInstance();
    }

    public ConfiguredObject getParent()
    {
        return getVirtualHost();
    }

    public boolean isDurable()
    {
        return _durable;
    }

    public boolean isExclusive()
    {
        return _exclusive;
    }
    
    public void setExclusive(boolean exclusive) throws AMQException
    {
        _exclusive = exclusive;

        if(isDurable())
        {
            getVirtualHost().getDurableConfigurationStore().updateQueue(this);
        }
    }

    public Exchange getAlternateExchange()
    {
        return _alternateExchange;
    }

    public void setAlternateExchange(Exchange exchange)
    {
        if(_alternateExchange != null)
        {
            _alternateExchange.removeReference(this);
        }
        if(exchange != null)
        {
            exchange.addReference(this);
        }
        _alternateExchange = exchange;
    }

    public Map<String, Object> getArguments()
    {
        return _arguments;
    }

    public boolean isAutoDelete()
    {
        return _autoDelete;
    }

    public AMQShortString getOwner()
    {
        return _owner;
    }

    public AuthorizationHolder getAuthorizationHolder()
    {
        return _authorizationHolder;
    }

    public void setAuthorizationHolder(final AuthorizationHolder authorizationHolder)
    {
        _authorizationHolder = authorizationHolder;
    }


    public VirtualHost getVirtualHost()
    {
        return _virtualHost;
    }

    public String getName()
    {
        return getNameShortString().toString();
    }

    // ------ Manage Subscriptions

    /**
     * Register a subscription against this queue.
     *
     * @param subscription The subscription to register
     * @param exclusive Is the subscription exclusive
     * @param authorizeSubject The Subject which is used to authorize the subscription for the queue
     * @throws AMQSecurityException
     * @throws ExistingExclusiveSubscription
     * @throws ExistingSubscriptionPreventsExclusive
     */
    public synchronized void registerSubscription(final Subscription subscription, final boolean exclusive,
                                                  Subject authorizeSubject)
            throws AMQSecurityException, ExistingExclusiveSubscription, ExistingSubscriptionPreventsExclusive
    {

        //If the owner is DLC we should not allow subscriptions
        //However browser subscriptions should be allowed
        if (DLCQueueUtils.isDeadLetterQueue(getResourceName()) && !(subscription instanceof SubscriptionImpl
                .BrowserSubscription)) {
            throw new AMQSecurityException("Subscription to " + AndesConstants.DEAD_LETTER_QUEUE_SUFFIX + " Queue is " +
                    "Not Allowed !, Please use a Different Alias");
        }

        // Access control
        if (subscription instanceof SubscriptionImpl.BrowserSubscription) {
            if (!AMQPAuthorizationManager.isAuthorized(AuthorizeAction.BROWSE, this, authorizeSubject)) {
                throw new AMQSecurityException("Permission denied");
            }
        } else {
            if (!AMQPAuthorizationManager.isAuthorized(AuthorizeAction.SUBSCRIBE, this, authorizeSubject)) {
                throw new AMQSecurityException("Permission denied");
            }
        }

        Boolean sharedSubscribersAllowed = AndesConfigurationManager.readValue
                (AndesConfiguration.ALLOW_SHARED_SHARED_SUBSCRIBERS);

        if (hasExclusiveSubscriber())
        {
            if(sharedSubscribersAllowed) {
                if(!(this.checkIfBoundToTopicExchange() && this.isDurable())) {
                    throw new ExistingExclusiveSubscription();
                }
            } else {
                throw new ExistingExclusiveSubscription();
            }
        }

        if (exclusive && !subscription.isTransient())
        {
            if (getConsumerCount() != 0)
            {
                if(sharedSubscribersAllowed) {
                    if(!(this.checkIfBoundToTopicExchange() && this.isDurable())) {
                        throw new ExistingSubscriptionPreventsExclusive();
                    }
                }  else {
                    throw new ExistingSubscriptionPreventsExclusive();
                }
            }
            else
            {
                _exclusiveSubscriber = subscription;
            }
        }

        _activeSubscriberCount.incrementAndGet();
        subscription.setStateListener(this);
        subscription.setQueueContext(new QueueContext(_entries.getHead()));

        if (!isDeleted())
        {
            subscription.setQueue(this, exclusive);
            if(_nolocal)
            {
                subscription.setNoLocal(_nolocal);
            }
            _subscriptionList.add(subscription);
            
            //Increment consumerCountHigh if necessary. (un)registerSubscription are both
            //synchronized methods so we don't need additional synchronization here
            if(_counsumerCountHigh.get() < getConsumerCount())
            {
                _counsumerCountHigh.incrementAndGet();
            }
            
            if (isDeleted())
            {
                subscription.queueDeleted(this);
            }
        }
        else
        {
            // TODO
        }

        deliverAsync(subscription);

    }

    public synchronized void unregisterSubscription(final Subscription subscription) throws AMQException
    {
        if (subscription == null)
        {
            throw new NullPointerException("subscription argument is null");
        }

        boolean removed = _subscriptionList.remove(subscription);

        if (removed)
        {
            subscription.close();
            // No longer can the queue have an exclusive consumer
            setExclusiveSubscriber(null);
            subscription.setQueueContext(null);

            // auto-delete queues must be deleted if there are no remaining subscribers

            if (_autoDelete && getDeleteOnNoConsumers() && !subscription.isTransient() && getConsumerCount() == 0  )
            {
                if (_logger.isInfoEnabled())
                {
                    _logger.debug("Auto-deleteing queue:" + this);
                }

                delete();

                // we need to manually fire the event to the removed subscription (which was the last one left for this
                // queue. This is because the delete method uses the subscription set which has just been cleared
                subscription.queueDeleted(this);
            }
        }

    }

    public boolean getDeleteOnNoConsumers()
    {
        return _deleteOnNoConsumers;
    }

    public void setDeleteOnNoConsumers(boolean b)
    {
        _deleteOnNoConsumers = b;
    }

    public void addBinding(final Binding binding)
    {
        _bindings.add(binding);
        int bindingCount = _bindings.size();
        int bindingCountHigh;
        while(bindingCount > (bindingCountHigh = _bindingCountHigh.get()))
        {
            if(_bindingCountHigh.compareAndSet(bindingCountHigh, bindingCount))
            {
                break;
            }
        }
        
        reconfigure();
    }
    
    private void reconfigure()
    {
        //Reconfigure the queue for to reflect this new binding.
        ConfigurationPlugin config = getVirtualHost().getConfiguration().getQueueConfiguration(this);

        if (_logger.isDebugEnabled())
        {
            _logger.debug("Reconfiguring queue(" + this + ") with config:" + config + " was "+ _queueConfiguration);
        }

        if (config != null)
        {
            // Reconfigure with new config.
            configure(config);
        }
    }

    public int getBindingCountHigh()
    {
        return _bindingCountHigh.get();
    }

    public void removeBinding(final Binding binding)
    {
        _bindings.remove(binding);
        
        reconfigure();
    }

    public List<Binding> getBindings()
    {
        return Collections.unmodifiableList(_bindings);
    }

    public int getBindingCount()
    {
        return getBindings().size();
    }

    public LogSubject getLogSubject()
    {
        return _logSubject;
    }

    // ------ Enqueue / Dequeue
    public void enqueue(ServerMessage message) throws AMQException
    {
        enqueue(message, null);
    }

    public void enqueue(ServerMessage message, PostEnqueueAction action) throws AMQException
    {
        incrementTxnEnqueueStats(message);
        incrementQueueCount();
        incrementQueueSize(message);
        _totalMessagesReceived.incrementAndGet();


        QueueEntry entry;
        Subscription exclusiveSub = _exclusiveSubscriber;

        if (exclusiveSub != null)
        {
            exclusiveSub.getSendLock();

            try
            {
                entry = _entries.add(message);
               // commenting this code since we do not need this in the andes delivery path - But it is for
               //queue delivery. For JMS Topics, it goes through here. So un commenting the code and it will invoke
               // the deliverToSubscription method only for jms topic. But there can be a performance hit in this place.
               // For the moment we need to support topics also
                for (Binding binding : _virtualHost.getExchangeRegistry().getExchange("amq.topic").getBindings()) {
                    if (binding.getQueue().getName().equalsIgnoreCase(entry.getQueue().getName())) {
                        deliverToSubscription(exclusiveSub, entry);
                        break;
                    }
                }
            }
            finally
            {
                exclusiveSub.releaseSendLock();
            }
        }
        else
        {
            entry = _entries.add(message);
            /*

            iterate over subscriptions and if any is at the end of the queue and can deliver this message, then deliver the message

             */
            SubscriptionList.SubscriptionNode node = _lastSubscriptionNode.get();
            SubscriptionList.SubscriptionNode nextNode = node.getNext();
            if (nextNode == null)
            {
                nextNode = _subscriptionList.getHead().getNext();
            }
            while (nextNode != null)
            {
                if (_lastSubscriptionNode.compareAndSet(node, nextNode))
                {
                    break;
                }
                else
                {
                    node = _lastSubscriptionNode.get();
                    nextNode = node.getNext();
                    if (nextNode == null)
                    {
                        nextNode = _subscriptionList.getHead().getNext();
                    }
                }
            }

            // always do one extra loop after we believe we've finished
            // this catches the case where we *just* miss an update
            int loops = 2;

            while (entry.isAvailable() && loops != 0)
            {
                if (nextNode == null)
                {
                    loops--;
                    nextNode = _subscriptionList.getHead();
                }
                else
                {
                    // if subscription at end, and active, offer
                  //  Subscription sub = nextNode.getSubscription();
                 //  deliverToSubscription(sub, entry);
                }
                nextNode = nextNode.getNext();

            }
        }


        if (entry.isAvailable())
        {
            checkSubscriptionsNotAheadOfDelivery(entry);

            deliverAsync();
        }

        if(_managedObject != null)
        {
            _managedObject.checkForNotification(entry.getMessage());
        }

        if(action != null)
        {
            action.onEnqueue(entry);
        }

    }

    private void deliverToSubscription(final Subscription sub, final QueueEntry entry)
            throws AMQException
    {

        sub.getSendLock();
        try
        {
            if (subscriptionReadyAndHasInterest(sub, entry)
                && !sub.isSuspended())
            {
                if (!sub.wouldSuspend(entry))
                {
                    if (sub.acquires() && !entry.acquire(sub))
                    {
                        // restore credit here that would have been taken away by wouldSuspend since we didn't manage
                        // to acquire the entry for this subscription
                        sub.onDequeue(entry);
                    }
                    else
                    {
                        deliverMessage(sub, entry);
                    }
                }
            }
        }
        finally
        {
            sub.releaseSendLock();
        }
    }

    protected void checkSubscriptionsNotAheadOfDelivery(final QueueEntry entry)
    {
        // This method is only required for queues which mess with ordering
        // Simple Queues don't :-)
    }

    private void incrementQueueSize(final ServerMessage message)
    {
        long size = message.getSize();
        getAtomicQueueSize().addAndGet(size);
        _enqueueSize.addAndGet(size);
        if(message.isPersistent() && isDurable())
        {
            _persistentMessageEnqueueSize.addAndGet(size);
            _persistentMessageEnqueueCount.incrementAndGet();
        }
    }

    private void incrementQueueCount()
    {
        getAtomicQueueCount().incrementAndGet();
    }
    
    private void incrementTxnEnqueueStats(final ServerMessage message)
    {
        SessionConfig session = message.getSessionConfig();
        
        if(session !=null && session.isTransactional())
        {
            _msgTxnEnqueues.incrementAndGet();
            _byteTxnEnqueues.addAndGet(message.getSize());
        }
    }
    
    private void incrementTxnDequeueStats(QueueEntry entry)
    {      
        _msgTxnDequeues.incrementAndGet();
        _byteTxnDequeues.addAndGet(entry.getSize());
    }

    private void deliverMessage(final Subscription sub, final QueueEntry entry)
            throws AMQException
    {
        setLastSeenEntry(sub, entry);

        _deliveredMessages.incrementAndGet();
        incrementUnackedMsgCount();

        sub.send(entry);
    }

    private boolean subscriptionReadyAndHasInterest(final Subscription sub, final QueueEntry entry) throws AMQException
    {
        return sub.hasInterest(entry) && (getNextAvailableEntry(sub) == entry);
    }


    private void setLastSeenEntry(final Subscription sub, final QueueEntry entry)
    {
        QueueContext subContext = (QueueContext) sub.getQueueContext();
        QueueEntry releasedEntry = subContext._releasedEntry;

        QueueContext._lastSeenUpdater.set(subContext, entry);
        if(releasedEntry == entry)
        {
           QueueContext._releasedUpdater.compareAndSet(subContext, releasedEntry, null);
        }
    }

    private void updateSubRequeueEntry(final Subscription sub, final QueueEntry entry)
    {

        QueueContext subContext = (QueueContext) sub.getQueueContext();
        if(subContext != null)
        {
            QueueEntry oldEntry;

            while((oldEntry  = subContext._releasedEntry) == null || oldEntry.compareTo(entry) > 0)
            {
                if(QueueContext._releasedUpdater.compareAndSet(subContext, oldEntry, entry))
                {
                    break;
                }
            }
        }
    }

    public void requeue(QueueEntry entry)
    {

        SubscriptionList.SubscriptionNodeIterator subscriberIter = _subscriptionList.iterator();
        // iterate over all the subscribers, and if they are in advance of this queue entry then move them backwards
        while (subscriberIter.advance() && entry.isAvailable())
        {
            Subscription sub = subscriberIter.getNode().getSubscription();

            // we don't make browsers send the same stuff twice
            if (sub.seesRequeues())
            {
                updateSubRequeueEntry(sub, entry);
            }
        }

        deliverAsync();

    }

    public void dequeue(QueueEntry entry, Subscription sub)
    {
        decrementQueueCount();
        decrementQueueSize(entry);
        if (entry.acquiredBySubscription())
        {
            _deliveredMessages.decrementAndGet();
        }
        
        if(sub != null && sub.isSessionTransactional())
        {
            incrementTxnDequeueStats(entry);
        }

        checkCapacity();

    }

    private void decrementQueueSize(final QueueEntry entry)
    {
        final ServerMessage message = entry.getMessage();
        long size = message.getSize();
        getAtomicQueueSize().addAndGet(-size);
        _dequeueSize.addAndGet(size);
        if(message.isPersistent() && isDurable())
        {
            _persistentMessageDequeueSize.addAndGet(size);
            _persistentMessageDequeueCount.incrementAndGet();
        }
    }

    void decrementQueueCount()
    {
        getAtomicQueueCount().decrementAndGet();
        _dequeueCount.incrementAndGet();
    }

    public boolean resend(final QueueEntry entry, final Subscription subscription) throws AMQException
    {
        /* TODO : This is wrong as the subscription may be suspended, we should instead change the state of the message
                  entry to resend and move back the subscription pointer. */

        subscription.getSendLock();
        try
        {
            if (!subscription.isClosed())
            {
                deliverMessage(subscription, entry);
                return true;
            }
            else
            {
                return false;
            }
        }
        finally
        {
            subscription.releaseSendLock();
        }
    }

    public int getConsumerCount()
    {
        return _subscriptionList.size();
    }
    
    public int getConsumerCountHigh()
    {
        return _counsumerCountHigh.get();
    }

    public int getActiveConsumerCount()
    {
        return _activeSubscriberCount.get();
    }

    public boolean isUnused()
    {
        return getConsumerCount() == 0;
    }

    public boolean isEmpty()
    {
        return getMessageCount() == 0;
    }

    public int getMessageCount()
    {
        return getAtomicQueueCount().get();
    }

    public long getQueueDepth()
    {
        return getAtomicQueueSize().get();
    }

    public int getUndeliveredMessageCount()
    {
        int count = getMessageCount() - _deliveredMessages.get();
        if (count < 0)
        {
            return 0;
        }
        else
        {
            return count;
        }
    }

    public long getReceivedMessageCount()
    {
        return _totalMessagesReceived.get();
    }

    public long getOldestMessageArrivalTime()
    {
        QueueEntry entry = getOldestQueueEntry();
        return entry == null ? Long.MAX_VALUE : entry.getMessage().getArrivalTime();
    }

    protected QueueEntry getOldestQueueEntry()
    {
        return _entries.next(_entries.getHead());
    }

    public boolean isDeleted()
    {
        return _deleted.get();
    }

    public List<QueueEntry> getMessagesOnTheQueue()
    {
        ArrayList<QueueEntry> entryList = new ArrayList<QueueEntry>();
        QueueEntryIterator queueListIterator = _entries.iterator();
        while (queueListIterator.advance())
        {
            QueueEntry node = queueListIterator.getNode();
            if (node != null && !node.isDispensed())
            {
                entryList.add(node);
            }
        }
        return entryList;

    }

    public void stateChange(Subscription sub, Subscription.State oldState, Subscription.State newState) {
        if (oldState == Subscription.State.ACTIVE && newState != Subscription.State.ACTIVE) {
            _activeSubscriberCount.decrementAndGet();

            try {
                if (newState == Subscription.State.CLOSED) {

                    //Tell Andes Kernel to close the subscription
                    QpidAndesBridge.closeAMQPSubscription(sub.getQueue(), sub);
                }

            } catch (AndesException e) {
                throw new RuntimeException(e);
            }

        } else if (newState == Subscription.State.ACTIVE) {
            if (oldState != Subscription.State.ACTIVE) {
                _activeSubscriberCount.incrementAndGet();

            }

            deliverAsync(sub);
        }
    }

    public int compareTo(final AMQQueue o)
    {
        return _name.compareTo(o.getNameShortString());
    }

    public AtomicInteger getAtomicQueueCount()
    {
        return _atomicQueueCount;
    }

    public AtomicLong getAtomicQueueSize()
    {
        return _atomicQueueSize;
    }

    public boolean hasExclusiveSubscriber()
    {
        return _exclusiveSubscriber != null;
    }

    private void setExclusiveSubscriber(Subscription exclusiveSubscriber)
    {
        _exclusiveSubscriber = exclusiveSubscriber;
    }

    public static interface QueueEntryFilter
    {
        public boolean accept(QueueEntry entry);

        public boolean filterComplete();
    }

    public List<QueueEntry> getMessagesOnTheQueue(final long fromMessageId, final long toMessageId)
    {
        return getMessagesOnTheQueue(new QueueEntryFilter()
        {

            public boolean accept(QueueEntry entry)
            {
                final long messageId = entry.getMessage().getMessageNumber();
                return messageId >= fromMessageId && messageId <= toMessageId;
            }

            public boolean filterComplete()
            {
                return false;
            }
        });
    }

    public QueueEntry getMessageOnTheQueue(final long messageId)
    {
        List<QueueEntry> entries = getMessagesOnTheQueue(new QueueEntryFilter()
        {
            private boolean _complete;

            public boolean accept(QueueEntry entry)
            {
                _complete = entry.getMessage().getMessageNumber() == messageId;
                return _complete;
            }

            public boolean filterComplete()
            {
                return _complete;
            }
        });
        return entries.isEmpty() ? null : entries.get(0);
    }

    public List<QueueEntry> getMessagesOnTheQueue(QueueEntryFilter filter)
    {
        ArrayList<QueueEntry> entryList = new ArrayList<QueueEntry>();
        QueueEntryIterator queueListIterator = _entries.iterator();
        while (queueListIterator.advance() && !filter.filterComplete())
        {
            QueueEntry node = queueListIterator.getNode();
            if (!node.isDispensed() && filter.accept(node))
            {
                entryList.add(node);
            }
        }
        return entryList;

    }

    /**
     * Returns a list of QueEntries from a given range of queue positions, eg messages 5 to 10 on the queue.
     *
     * The 'queue position' index starts from 1. Using 0 in 'from' will be ignored and continue from 1.
     * Using 0 in the 'to' field will return an empty list regardless of the 'from' value.
     * @param fromPosition
     * @param toPosition
     * @return
     */
    public List<QueueEntry> getMessagesRangeOnTheQueue(final long fromPosition, final long toPosition)
    {
        return getMessagesOnTheQueue(new QueueEntryFilter()
                                        {
                                            private long position = 0;

                                            public boolean accept(QueueEntry entry)
                                            {
                                                position++;
                                                return (position >= fromPosition) && (position <= toPosition);
                                            }

                                            public boolean filterComplete()
                                            {
                                                return position >= toPosition;
                                            }
                                        });

    }

    public void moveMessagesToAnotherQueue(final long fromMessageId,
                                           final long toMessageId,
                                           String queueName,
                                           ServerTransaction txn) throws IllegalArgumentException
    {

        final AMQQueue toQueue = getVirtualHost().getQueueRegistry().getQueue(new AMQShortString(queueName));
        if (toQueue == null)
        {
            throw new IllegalArgumentException("Queue '" + queueName + "' is not registered with the virtualhost.");
        }
        else if (toQueue == this)
        {
            throw new IllegalArgumentException("The destination queue cant be the same as the source queue");
        }

        List<QueueEntry> entries = getMessagesOnTheQueue(new QueueEntryFilter()
        {

            public boolean accept(QueueEntry entry)
            {
                final long messageId = entry.getMessage().getMessageNumber();
                return (messageId >= fromMessageId)
                       && (messageId <= toMessageId)
                       && entry.acquire();
            }

            public boolean filterComplete()
            {
                return false;
            }
        });



        // Move the messages in on the message store.
        for (final QueueEntry entry : entries)
        {
            final ServerMessage message = entry.getMessage();
            txn.enqueue(toQueue, message,
                        new ServerTransaction.Action()
                        {

                            public void postCommit()
                            {
                                try
                                {
                                    toQueue.enqueue(message);
                                }
                                catch (AMQException e)
                                {
                                    throw new RuntimeException(e);
                                }
                            }

                            public void onRollback()
                            {
                                entry.release();
                            }
                        });
            txn.dequeue(this, message,
                        new ServerTransaction.Action()
                        {

                            public void postCommit()
                            {
                                entry.discard();
                            }

                            public void onRollback()
                            {

                            }
                        });

        }

    }

    public void copyMessagesToAnotherQueue(final long fromMessageId,
                                           final long toMessageId,
                                           String queueName,
                                           final ServerTransaction txn) throws IllegalArgumentException
    {
        final AMQQueue toQueue = getVirtualHost().getQueueRegistry().getQueue(new AMQShortString(queueName));
        if (toQueue == null)
        {
            throw new IllegalArgumentException("Queue '" + queueName + "' is not registered with the virtualhost.");
        }
        else if (toQueue == this)
        {
            throw new IllegalArgumentException("The destination queue cant be the same as the source queue");
        }

        List<QueueEntry> entries = getMessagesOnTheQueue(new QueueEntryFilter()
        {

            public boolean accept(QueueEntry entry)
            {
                final long messageId = entry.getMessage().getMessageNumber();
                return ((messageId >= fromMessageId)
                    && (messageId <= toMessageId));
            }

            public boolean filterComplete()
            {
                return false;
            }
        });


        // Move the messages in on the message store.
        for (QueueEntry entry : entries)
        {
            final ServerMessage message = entry.getMessage();

            txn.enqueue(toQueue, message, new ServerTransaction.Action()
            {
                public void postCommit()
                {
                    try
                    {
                        toQueue.enqueue(message);
                    }
                    catch (AMQException e)
                    {
                        throw new RuntimeException(e);
                    }
                }

                public void onRollback()
                {

                }
            });

        }

    }

    public void removeMessagesFromQueue(long fromMessageId, long toMessageId)
    {

        QueueEntryIterator queueListIterator = _entries.iterator();

        while (queueListIterator.advance())
        {
            QueueEntry node = queueListIterator.getNode();

            final ServerMessage message = node.getMessage();
            if(message != null)
            {
                final long messageId = message.getMessageNumber();

                if ((messageId >= fromMessageId)
                    && (messageId <= toMessageId)
                    && node.acquire())
                {
                    dequeueEntry(node);
                }
            }
        }

    }

    public void purge(final long request) throws AMQException
    {
        clear(request);
    }

    public long getCreateTime()
    {
        return _createTime;
    }

    // ------ Management functions

    public void deleteMessageFromTop()
    {
        QueueEntryIterator queueListIterator = _entries.iterator();
        boolean noDeletes = true;

        while (noDeletes && queueListIterator.advance())
        {
            QueueEntry node = queueListIterator.getNode();
            if (node.acquire())
            {
                dequeueEntry(node);
                noDeletes = false;
            }

        }
    }

    public long clearQueue() throws AMQException
    {         
        return clear(0l);
    }

    private long clear(final long request) throws AMQSecurityException
    {
        //Perform ACLs
        if (!getVirtualHost().getSecurityManager().authorisePurge(this))
        {
            throw new AMQSecurityException("Permission denied: queue " + getName());
        }
        
        QueueEntryIterator queueListIterator = _entries.iterator();
        long count = 0;

        ServerTransaction txn = new LocalTransaction(getVirtualHost().getTransactionLog());

        while (queueListIterator.advance())
        {
            QueueEntry node = queueListIterator.getNode();
            if (node.acquire())
            {
                dequeueEntry(node, txn);
                if(++count == request)
                {
                    break;
                }
            }

        }

        txn.commit();

        return count;
    }

    private void dequeueEntry(final QueueEntry node)
    {
        ServerTransaction txn = new AutoCommitTransaction(getVirtualHost().getTransactionLog());
        dequeueEntry(node, txn);
    }

    private void dequeueEntry(final QueueEntry node, ServerTransaction txn)
    {
        txn.dequeue(this, node.getMessage(),
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

    public void addQueueDeleteTask(final Task task)
    {
        _deleteTaskList.add(task);
    }

    public void removeQueueDeleteTask(final Task task)
    {
        _deleteTaskList.remove(task);
    }

    // TODO list all thrown exceptions
    public int delete() throws AMQSecurityException, AMQException
    {
        // Check access
        if (!_virtualHost.getSecurityManager().authoriseDelete(this))
        {
            throw new AMQSecurityException("Permission denied: " + getName());
        }
        
        if (!_deleted.getAndSet(true))
        {

            for (Binding b : getBindings())
            {
                _virtualHost.getBindingFactory().removeBinding(b);
            }

            SubscriptionList.SubscriptionNodeIterator subscriptionIter = _subscriptionList.iterator();

            while (subscriptionIter.advance())
            {
                Subscription s = subscriptionIter.getNode().getSubscription();
                if (s != null)
                {
                    s.queueDeleted(this);
                }
            }

            _virtualHost.getQueueRegistry().unregisterQueue(_name);
            getConfigStore().removeConfiguredObject(this);

            List<QueueEntry> entries = getMessagesOnTheQueue(new QueueEntryFilter()
            {

                public boolean accept(QueueEntry entry)
                {
                    return entry.acquire();
                }

                public boolean filterComplete()
                {
                    return false;
                }
            });

            ServerTransaction txn = new LocalTransaction(getVirtualHost().getTransactionLog());

            if(_alternateExchange != null)
            {

                InboundMessageAdapter adapter = new InboundMessageAdapter();
                for(final QueueEntry entry : entries)
                {
                    adapter.setEntry(entry);
                    final List<? extends BaseQueue> rerouteQueues = _alternateExchange.route(adapter);
                    final ServerMessage message = entry.getMessage();
                    if(rerouteQueues != null && rerouteQueues.size() != 0)
                    {
                        txn.enqueue(rerouteQueues, entry.getMessage(),
                                    new ServerTransaction.Action()
                                    {

                                        public void postCommit()
                                        {
                                            try
                                            {
                                                for(BaseQueue queue : rerouteQueues)
                                                {
                                                    queue.enqueue(message);
                                                }
                                            }
                                            catch (AMQException e)
                                            {
                                                throw new RuntimeException(e);
                                            }

                                        }

                                        public void onRollback()
                                        {

                                        }
                                    });
                        txn.dequeue(this, entry.getMessage(),
                                    new ServerTransaction.Action()
                                    {

                                        public void postCommit()
                                        {
                                            entry.discard();
                                        }

                                        public void onRollback()
                                        {
                                        }
                                    });
                    }

                }

                _alternateExchange.removeReference(this);
            }
            else
            {
                // TODO log discard

                for(final QueueEntry entry : entries)
                {
                    final ServerMessage message = entry.getMessage();
                    if(message != null)
                    {
                        txn.dequeue(this, message,
                                    new ServerTransaction.Action()
                                    {

                                        public void postCommit()
                                        {
                                            entry.discard();
                                        }

                                        public void onRollback()
                                        {
                                        }
                                    });
                    }
                }
            }

            txn.commit();


            if(_managedObject!=null)
            {
                _managedObject.unregister();
            }

            for (Task task : _deleteTaskList)
            {
                task.doTask(this);
            }

            _deleteTaskList.clear();
            stop();

            //Log Queue Deletion
            CurrentActor.get().message(_logSubject, QueueMessages.DELETED());

        }
        return getMessageCount();

    }

    public void stop()
    {
        if (!_stopped.getAndSet(true))
        {
            ReferenceCountingExecutorService.getInstance().releaseExecutorService();
        }
    }

    public void checkCapacity(AMQChannel channel)
    {
        if(_capacity != 0l)
        {
            if(_atomicQueueSize.get() > _capacity)
            {
                _overfull.set(true);
                //Overfull log message
                _logActor.message(_logSubject, QueueMessages.OVERFULL(_atomicQueueSize.get(), _capacity));

                if(_blockedChannels.putIfAbsent(channel, Boolean.TRUE)==null)
                {
                    channel.block(this);
                }

                if(_atomicQueueSize.get() <= _flowResumeCapacity)
                {

                    //Underfull log message
                    _logActor.message(_logSubject, QueueMessages.UNDERFULL(_atomicQueueSize.get(), _flowResumeCapacity));

                   channel.unblock(this);
                   _blockedChannels.remove(channel);

                }

            }



        }
    }

    private void checkCapacity()
    {
        if(_capacity != 0L)
        {
            if(_overfull.get() && _atomicQueueSize.get() <= _flowResumeCapacity)
            {
                if(_overfull.compareAndSet(true,false))
                {//Underfull log message
                    _logActor.message(_logSubject, QueueMessages.UNDERFULL(_atomicQueueSize.get(), _flowResumeCapacity));
                }


                for(AMQChannel c : _blockedChannels.keySet())
                {
                    c.unblock(this);
                    _blockedChannels.remove(c);
                }
            }
        }
    }


    public void deliverAsync()
    {
         // Here i m removing this code as queue processing is handled in a different path in andes.


//        QueueRunner runner = new QueueRunner(this, _stateChangeCount.incrementAndGet());
//
//        if (_asynchronousRunner.compareAndSet(null, runner))
//        {
//            _asyncDelivery.execute(runner);
//        }
    }

    public void deliverAsync(Subscription sub)
    {
        //With the cassandra based implementation this is still needed if we are using
        //enqueue() path for topic delivery. It try to deliver all undelivered messages
        //got accumilated when a durable topic subscriber is there and update lastSeenEntry
        //necessarily to properly return getNextAvailableEntry()
        //TODO : remove this flusher when our own topic delivery implementation is done.
        if(sub.getQueue().checkIfBoundToTopicExchange()) {
            SubFlushRunner flusher = (SubFlushRunner) sub.get(SUB_FLUSH_RUNNER);
            if(flusher == null)
            {
                flusher = new SubFlushRunner(sub);
                sub.set(SUB_FLUSH_RUNNER, flusher);
            }
            _asyncDelivery.execute(flusher);
        } else {
            return;
        }
    }

    public void flushSubscription(Subscription sub) throws AMQException
    {
        // Access control
        if (!getVirtualHost().getSecurityManager().authoriseConsume(this))
        {
            throw new AMQSecurityException("Permission denied: " + getName());
        }
        flushSubscription(sub, Long.MAX_VALUE);
    }

    public boolean flushSubscription(Subscription sub, long iterations) throws AMQException
    {
        boolean atTail = false;

        while (!sub.isSuspended() && !atTail && iterations != 0)
        {
            try
            {
                sub.getSendLock();
                atTail = attemptDelivery(sub);
                if (atTail && sub.isAutoClose())
                {
                    unregisterSubscription(sub);

                    sub.confirmAutoClose();

                }
                else if (!atTail)
                {
                    iterations--;
                }
            }
            finally
            {
                sub.releaseSendLock();
            }
        }

        // if there's (potentially) more than one subscription the others will potentially not have been advanced to the
        // next entry they are interested in yet.  This would lead to holding on to references to expired messages, etc
        // which would give us memory "leak".

        if (!hasExclusiveSubscriber())
        {
            advanceAllSubscriptions();
        }
        return atTail;
    }

    /**
     * Attempt delivery for the given subscription.
     *
     * Looks up the next node for the subscription and attempts to deliver it.
     *
     * @param sub
     * @return true if we have completed all possible deliveries for this sub.
     * @throws AMQException
     */
    private boolean attemptDelivery(Subscription sub) throws AMQException
    {
        boolean atTail = false;

        boolean subActive = sub.isActive() && !sub.isSuspended();
        if (subActive)
        {

            QueueEntry node  = getNextAvailableEntry(sub);

            if (node != null && node.isAvailable())
            {
                if (sub.hasInterest(node))
                {
                    if (!sub.wouldSuspend(node))
                    {
                        if (sub.acquires() && !node.acquire(sub))
                        {
                            sub.onDequeue(node);
                        }
                        else
                        {
                            deliverMessage(sub, node);
                        }

                    }
                    else // Not enough Credit for message and wouldSuspend
                    {
                        //QPID-1187 - Treat the subscription as suspended for this message
                        // and wait for the message to be removed to continue delivery.
                        subActive = false;
                        node.addStateChangeListener(new QueueEntryListener(sub));
                    }
                }

            }
            atTail = (node == null) || (_entries.next(node) == null);
        }
        return atTail || !subActive;
    }

    protected void advanceAllSubscriptions() throws AMQException
    {
        SubscriptionList.SubscriptionNodeIterator subscriberIter = _subscriptionList.iterator();
        while (subscriberIter.advance())
        {
            SubscriptionList.SubscriptionNode subNode = subscriberIter.getNode();
            Subscription sub = subNode.getSubscription();
            if(sub.acquires())
            {
                getNextAvailableEntry(sub);
            }
            else
            {
                // TODO
            }
        }
    }

    private QueueEntry getNextAvailableEntry(final Subscription sub)
            throws AMQException
    {
        QueueContext context = (QueueContext) sub.getQueueContext();
        if(context != null)
        {
            QueueEntry lastSeen = context._lastSeenEntry;
            QueueEntry releasedNode = context._releasedEntry;

            QueueEntry node = (releasedNode != null && lastSeen.compareTo(releasedNode)>=0) ? releasedNode : _entries.next(lastSeen);

            boolean expired = false;
            while (node != null && (!node.isAvailable() || (expired = node.expired()) || !sub.hasInterest(node)))
            {
                if (expired)
                {
                    expired = false;
                    if (node.acquire())
                    {
                        dequeueEntry(node);
                    }
                }

                if(QueueContext._lastSeenUpdater.compareAndSet(context, lastSeen, node))
                {
                    QueueContext._releasedUpdater.compareAndSet(context, releasedNode, null);
                }

                lastSeen = context._lastSeenEntry;
                releasedNode = context._releasedEntry;
                node = (releasedNode != null && lastSeen.compareTo(releasedNode)>0) ? releasedNode : _entries.next(lastSeen);
            }
            return node;
        }
        else
        {
            return null;
        }
    }


    /**
     * Used by queue Runners to asynchronously deliver messages to consumers.
     *
     * A queue Runner is started whenever a state change occurs, e.g when a new
     * message arrives on the queue and cannot be immediately delivered to a
     * subscription (i.e. asynchronous delivery is required). Unless there are
     * SubFlushRunners operating (due to subscriptions unsuspending) which are
     * capable of accepting/delivering all messages then these messages would
     * otherwise remain on the queue.
     *
     * processQueue should be running while there are messages on the queue AND
     * there are subscriptions that can deliver them. If there are no
     * subscriptions capable of delivering the remaining messages on the queue
     * then processQueue should stop to prevent spinning.
     *
     * Since processQueue is runs in a fixed size Executor, it should not run
     * indefinitely to prevent starving other tasks of CPU (e.g jobs to process
     * incoming messages may not be able to be scheduled in the thread pool
     * because all threads are working on clearing down large queues). To solve
     * this problem, after an arbitrary number of message deliveries the
     * processQueue job stops iterating, resubmits itself to the executor, and
     * ends the current instance
     *
     * @param runner the Runner to schedule
     * @throws AMQException
     */
    public void processQueue(QueueRunner runner) throws AMQException
    {
        long stateChangeCount;
        long previousStateChangeCount = Long.MIN_VALUE;
        boolean deliveryIncomplete = true;

        boolean lastLoop = false;
        int iterations = MAX_ASYNC_DELIVERIES;

        _asynchronousRunner.compareAndSet(runner, null);

        // For every message enqueue/requeue the we fire deliveryAsync() which
        // increases _stateChangeCount. If _sCC changes whilst we are in our loop
        // (detected by setting previousStateChangeCount to stateChangeCount in the loop body)
        // then we will continue to run for a maximum of iterations.
        // So whilst delivery/rejection is going on a processQueue thread will be running
         int attempt = 0;
        while (iterations != 0 && ((previousStateChangeCount != (stateChangeCount = _stateChangeCount.get())) || deliveryIncomplete) && _asynchronousRunner.compareAndSet(null, runner))
        {
            // we want to have one extra loop after every subscription has reached the point where it cannot move
            // further, just in case the advance of one subscription in the last loop allows a different subscription to
            // move forward in the next iteration

            if (previousStateChangeCount != stateChangeCount)
            {
                //further asynchronous delivery is required since the
                //previous loop. keep going if iteration slicing allows.
                lastLoop = false;
            }

            previousStateChangeCount = stateChangeCount;
            boolean allSubscriptionsDone = true;
            boolean subscriptionDone;

            SubscriptionList.SubscriptionNodeIterator subscriptionIter = _subscriptionList.iterator();
            //iterate over the subscribers and try to advance their pointer
            while (subscriptionIter.advance())
            {
                Subscription sub = subscriptionIter.getNode().getSubscription();
                sub.getSendLock();
                try
                {
                    //attempt delivery. returns true if no further delivery currently possible to this sub
                    subscriptionDone = false;//attemptDelivery(sub);
                    if (subscriptionDone)
                    {
                        //close autoClose subscriptions if we are not currently intent on continuing
                        if (lastLoop && sub.isAutoClose())
                        {
                            unregisterSubscription(sub);

                            sub.confirmAutoClose();
                        }
                    }
                    else
                    {
                        //this subscription can accept additional deliveries, so we must 
                        //keep going after this (if iteration slicing allows it)
                        allSubscriptionsDone = false;
                        lastLoop = false;
                        iterations--;
                    }
                }
                finally
                {
                    sub.releaseSendLock();
                }
            }

            if(allSubscriptionsDone && lastLoop)
            {
                //We have done an extra loop already and there are again
                //again no further delivery attempts possible, only
                //keep going if state change demands it.
                deliveryIncomplete = false;
            }
            else if(allSubscriptionsDone)
            {
                //All subscriptions reported being done, but we have to do
                //an extra loop if the iterations are not exhausted and
                //there is still any work to be done
                deliveryIncomplete = _subscriptionList.size() != 0;
                lastLoop = true;
            }
            else
            {
                //some subscriptions can still accept more messages,
                //keep going if iteration count allows.
                lastLoop = false;
                deliveryIncomplete = true;
            }

            _asynchronousRunner.set(null);
        }

        // If iterations == 0 then the limiting factor was the time-slicing rather than available messages or credit
        // therefore we should schedule this runner again (unless someone beats us to it :-) ).
        if (iterations == 0 && _asynchronousRunner.compareAndSet(null, runner))
        {
            if (_logger.isDebugEnabled())
            {
                _logger.debug("Rescheduling runner:" + runner);
            }
            _asyncDelivery.execute(runner);
        }
    }

    public void checkMessageStatus() throws AMQException
    {

        QueueEntryIterator queueListIterator = _entries.iterator();

        while (queueListIterator.advance())
        {
            QueueEntry node = queueListIterator.getNode();
            // Only process nodes that are not currently deleted and not dequeued
            if (!node.isDispensed())
            {
                // If the node has exired then aquire it
                if (node.expired() && node.acquire())
                {
                    // Then dequeue it.
                    dequeueEntry(node);
                }
                else
                {
                    if (_managedObject != null)
                    {
                        // There is a chance that the node could be deleted by
                        // the time the check actually occurs. So verify we
                        // can actually get the message to perform the check.
                        ServerMessage msg = node.getMessage();
                        if (msg != null)
                        {
                            _managedObject.checkForNotification(msg);
                        }
                    }
                }
            }
        }

    }

    public long getMinimumAlertRepeatGap()
    {
        return _minimumAlertRepeatGap;
    }

    public void setMinimumAlertRepeatGap(long minimumAlertRepeatGap)
    {
        _minimumAlertRepeatGap = minimumAlertRepeatGap;
    }

    public long getMaximumMessageAge()
    {
        return _maximumMessageAge;
    }

    public void setMaximumMessageAge(long maximumMessageAge)
    {
        _maximumMessageAge = maximumMessageAge;
        if (maximumMessageAge == 0L)
        {
            _notificationChecks.remove(NotificationCheck.MESSAGE_AGE_ALERT);
        }
        else
        {
            _notificationChecks.add(NotificationCheck.MESSAGE_AGE_ALERT);
        }
    }

    public long getMaximumMessageCount()
    {
        return _maximumMessageCount;
    }

    public void setMaximumMessageCount(final long maximumMessageCount)
    {
        _maximumMessageCount = maximumMessageCount;
        if (maximumMessageCount == 0L)
        {
            _notificationChecks.remove(NotificationCheck.MESSAGE_COUNT_ALERT);
        }
        else
        {
            _notificationChecks.add(NotificationCheck.MESSAGE_COUNT_ALERT);
        }

    }

    public long getMaximumQueueDepth()
    {
        return _maximumQueueDepth;
    }

    // Sets the queue depth, the max queue size
    public void setMaximumQueueDepth(final long maximumQueueDepth)
    {
        _maximumQueueDepth = maximumQueueDepth;
        if (maximumQueueDepth == 0L)
        {
            _notificationChecks.remove(NotificationCheck.QUEUE_DEPTH_ALERT);
        }
        else
        {
            _notificationChecks.add(NotificationCheck.QUEUE_DEPTH_ALERT);
        }

    }

    public long getMaximumMessageSize()
    {
        return _maximumMessageSize;
    }

    public void setMaximumMessageSize(final long maximumMessageSize)
    {
        _maximumMessageSize = maximumMessageSize;
        if (maximumMessageSize == 0L)
        {
            _notificationChecks.remove(NotificationCheck.MESSAGE_SIZE_ALERT);
        }
        else
        {
            _notificationChecks.add(NotificationCheck.MESSAGE_SIZE_ALERT);
        }
    }

    public long getCapacity()
    {
        return _capacity;
    }

    public void setCapacity(long capacity)
    {
        _capacity = capacity;
    }

    public long getFlowResumeCapacity()
    {
        return _flowResumeCapacity;
    }

    public void setFlowResumeCapacity(long flowResumeCapacity)
    {
        _flowResumeCapacity = flowResumeCapacity;

        checkCapacity();
    }

    public boolean isOverfull()
    {
        return _overfull.get();
    }

    public Set<NotificationCheck> getNotificationChecks()
    {
        return _notificationChecks;
    }

    public ManagedObject getManagedObject()
    {
        return _managedObject;
    }

    private final class QueueEntryListener implements QueueEntry.StateChangeListener
    {

        private final Subscription _sub;

        public QueueEntryListener(final Subscription sub)
        {
            _sub = sub;
        }

        public boolean equals(Object o)
        {
            assert o != null;
            assert o instanceof QueueEntryListener;
            return _sub == ((QueueEntryListener) o)._sub;
        }

        public int hashCode()
        {
            return System.identityHashCode(_sub);
        }

        public void stateChanged(QueueEntry entry, QueueEntry.State oldSate, QueueEntry.State newState)
        {
            entry.removeStateChangeListener(this);
            deliverAsync(_sub);
        }
    }

    public List<Long> getMessagesOnTheQueue(int num)
    {
        return getMessagesOnTheQueue(num, 0);
    }

    public List<Long> getMessagesOnTheQueue(int num, int offset)
    {
        ArrayList<Long> ids = new ArrayList<Long>(num);
        QueueEntryIterator it = _entries.iterator();
        for (int i = 0; i < offset; i++)
        {
            it.advance();
        }

        for (int i = 0; i < num && !it.atTail(); i++)
        {
            it.advance();
            ids.add(it.getNode().getMessage().getMessageNumber());
        }
        return ids;
    }

    public AMQSessionModel getExclusiveOwningSession()
    {
        return _exclusiveOwner;
    }

    public void setExclusiveOwningSession(AMQSessionModel exclusiveOwner)
    {
        _exclusive = true;
        _exclusiveOwner = exclusiveOwner;
    }


    public void configure(ConfigurationPlugin config)
    {
        if (config != null)
        {
            if (config instanceof QueueConfiguration)
            {

                setMaximumMessageAge(((QueueConfiguration)config).getMaximumMessageAge());
                setMaximumQueueDepth(((QueueConfiguration)config).getMaximumQueueDepth());
                setMaximumMessageSize(((QueueConfiguration)config).getMaximumMessageSize());
                setMaximumMessageCount(((QueueConfiguration)config).getMaximumMessageCount());
                setMinimumAlertRepeatGap(((QueueConfiguration)config).getMinimumAlertRepeatGap());
                _capacity = ((QueueConfiguration)config).getCapacity();
                _flowResumeCapacity = ((QueueConfiguration)config).getFlowResumeCapacity();
            }

            _queueConfiguration = config;

        }
    }


    public ConfigurationPlugin getConfiguration()
    {
        return _queueConfiguration;
    }

    public String getResourceName()
    {
        return _resourceName;
    }


    public ConfigStore getConfigStore()
    {
        return getVirtualHost().getConfigStore();
    }

    public long getMessageDequeueCount()
    {
        return  _dequeueCount.get();
    }

    public long getTotalEnqueueSize()
    {
        return _enqueueSize.get();
    }

    public long getTotalDequeueSize()
    {
        return _dequeueSize.get();
    }
    
    public long getByteTxnEnqueues()
    {
        return _byteTxnEnqueues.get();
    }
    
    public long getByteTxnDequeues()
    {
        return _byteTxnDequeues.get();
    }
    
    public long getMsgTxnEnqueues()
    {
        return _msgTxnEnqueues.get();
    }
    
    public long getMsgTxnDequeues()
    {
        return _msgTxnDequeues.get();
    }

    public long getPersistentByteEnqueues()
    {
        return _persistentMessageEnqueueSize.get();
    }

    public long getPersistentByteDequeues()
    {
        return _persistentMessageDequeueSize.get();
    }

    public long getPersistentMsgEnqueues()
    {
        return _persistentMessageEnqueueCount.get();
    }

    public long getPersistentMsgDequeues()
    {
        return _persistentMessageDequeueCount.get();
    }


    @Override
    public String toString()
    {
        return String.valueOf(getNameShortString());
    }

    public long getUnackedMessageCountHigh()
    {
        return _unackedMsgCountHigh.get();
    }
    
    public long getUnackedMessageCount()
    {
        return _unackedMsgCount.get();
    }
    
    public void decrementUnackedMsgCount()
    {
        _unackedMsgCount.decrementAndGet();
    }
    
    private void incrementUnackedMsgCount()
    {
        long unackedMsgCount = _unackedMsgCount.incrementAndGet();
        
        long unackedMsgCountHigh;
        while(unackedMsgCount > (unackedMsgCountHigh = _unackedMsgCountHigh.get()))
        {
            if(_unackedMsgCountHigh.compareAndSet(unackedMsgCountHigh, unackedMsgCount))
            {
                break;
            }
        }
    }

    public LogActor getLogActor()
    {
        return _logActor;
    }

    public boolean checkIfBoundToTopicExchange() {
        boolean result = false;
        List<Binding> bindingList = this.getBindings();
        for(Binding aBinding : bindingList) {
            if(aBinding.getExchange().getName().equalsIgnoreCase("amq.topic")) {
                result = true;
                break;
            }
        }

        return result;
    }

    public ProtocolType getProtocolType() {
        return _protocolType;
    }

    public void setProtocolType(ProtocolType protocolType) {
        this._protocolType = protocolType;
    }
}
