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

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQSecurityException;
import org.wso2.andes.amqp.QpidAndesBridge;
import org.wso2.andes.configuration.qpid.ConfigStore;
import org.wso2.andes.configuration.qpid.ConfiguredObject;
import org.wso2.andes.configuration.qpid.ConnectionConfig;
import org.wso2.andes.configuration.qpid.SessionConfig;
import org.wso2.andes.configuration.qpid.SessionConfigType;
import org.wso2.andes.framing.AMQMethodBody;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.BasicContentHeaderProperties;
import org.wso2.andes.framing.ContentBody;
import org.wso2.andes.framing.ContentHeaderBody;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.framing.abstraction.ContentChunk;
import org.wso2.andes.framing.abstraction.MessagePublishInfo;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.FlowControlListener;
import org.wso2.andes.kernel.disruptor.inbound.InboundTransactionEvent;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.ack.UnacknowledgedMessageMap;
import org.wso2.andes.server.ack.UnacknowledgedMessageMapImpl;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.flow.FlowCreditManager;
import org.wso2.andes.server.flow.Pre0_10CreditManager;
import org.wso2.andes.server.logging.LogActor;
import org.wso2.andes.server.logging.LogSubject;
import org.wso2.andes.server.logging.actors.AMQPChannelActor;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.messages.ChannelMessages;
import org.wso2.andes.server.logging.subjects.ChannelLogSubject;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.message.MessageMetaData;
import org.wso2.andes.server.message.MessageReference;
import org.wso2.andes.server.message.ServerMessage;
import org.wso2.andes.server.output.ProtocolOutputConverter;
import org.wso2.andes.server.protocol.AMQConnectionModel;
import org.wso2.andes.server.protocol.AMQProtocolEngine;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.protocol.AMQSessionModel;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.BaseQueue;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.server.queue.IncomingMessage;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.store.MessageStore;
import org.wso2.andes.server.store.StorableMessageMetaData;
import org.wso2.andes.server.store.StoredMessage;
import org.wso2.andes.server.subscription.ClientDeliveryMethod;
import org.wso2.andes.server.subscription.RecordDeliveryMethod;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionFactoryImpl;
import org.wso2.andes.server.txn.AutoCommitTransaction;
import org.wso2.andes.server.txn.LocalTransaction;
import org.wso2.andes.server.txn.ServerTransaction;
import org.wso2.andes.server.virtualhost.AMQChannelMBean;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.store.StoredAMQPMessage;

import javax.management.JMException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class AMQChannel implements SessionConfig, AMQSessionModel
{
    public static final int DEFAULT_PREFETCH = 5000;

    private static final Logger _logger = Logger.getLogger(AMQChannel.class);

    private static final boolean MSG_AUTH =
            ApplicationRegistry.getInstance().getConfiguration().getMsgAuth();

    private final int _channelId;


    private final Pre0_10CreditManager _creditManager = new Pre0_10CreditManager(0l,0l);

    /**
     * Andes channel related to this local channel
     */
    private final AndesChannel andesChannel;

    /**
     * The delivery tag is unique per channel. This is pre-incremented before putting into the deliver frame so that
     * value of this represents the <b>last</b> tag sent out
     */
    private AtomicLong _deliveryTag = new AtomicLong(0);

    /** A channel has a default queue (the last declared) that is used when no queue name is explicitly set */
    private AMQQueue _defaultQueue;

    /** This tag is unique per subscription to a queue. The server returns this in response to a basic.consume request. */
    private AtomicInteger _consumerTag = new AtomicInteger(0);

    /**
     * The current message - which may be partial in the sense that not all frames have been received yet - which has
     * been received by this channel. As the frames are received the message gets updated and once all frames have been
     * received the message can then be routed.
     */
    private IncomingMessage _currentMessage;

    /** Maps from consumer tag to subscription instance. Allows us to unsubscribe from a queue. */
    protected final Map<AMQShortString, Subscription> _tag2SubscriptionMap = new HashMap<AMQShortString, Subscription>();

    private final MessageStore _messageStore;

    // Default map to store unacked messages. This will replaced according to the delivery strategy when a subscriber
    // is registered to this channel
    private UnacknowledgedMessageMap _unacknowledgedMessageMap = new UnacknowledgedMessageMapImpl(DEFAULT_PREFETCH,
            this, false);

    // Set of messages being acknoweledged in the current transaction
    private SortedSet<QueueEntry> _acknowledgedMessages = new TreeSet<QueueEntry>();

    private final AtomicBoolean _suspended = new AtomicBoolean(false);

    private ServerTransaction _transaction;

    /**
     * This transaction object handles incoming transactional messages (not acknowledgments)
     * to Andes core
     */
    private InboundTransactionEvent andesTransactionEvent;

    /**
     * This specifies the beginning of a transaction initiated by a select command
     */
    private boolean beginPublisherTransaction;

    private final AtomicLong _txnStarts = new AtomicLong(0);
    private final AtomicLong _txnCommits = new AtomicLong(0);
    private final AtomicLong _txnRejects = new AtomicLong(0);
    private final AtomicLong _txnCount = new AtomicLong(0);
    private final AtomicLong _txnUpdateTime = new AtomicLong(0);

    private final AMQProtocolSession _session;
    private AtomicBoolean _closing = new AtomicBoolean(false);

    private final ConcurrentMap<AMQQueue, Boolean> _blockingQueues = new ConcurrentHashMap<AMQQueue, Boolean>();

    private final AtomicBoolean _blocking = new AtomicBoolean(false);

    /**
     * Message ID that was rejected last until the current time.
     */
    private long lastRejectedMessageId = -1;

    /**
     * Message ID that was rollbacked last until the current time.
     */
    private long lastRollbackedMessageId = -1;

    private LogActor _actor;
    private LogSubject _logSubject;
    private volatile boolean _rollingBack;

    private static final Runnable NULL_TASK = new Runnable() { public void run() {} };
    private List<QueueEntry> _resendList = new ArrayList<QueueEntry>();
    private static final
    AMQShortString IMMEDIATE_DELIVERY_REPLY_TEXT = new AMQShortString("Immediate delivery is not possible.");
    private final UUID _id;
    private long _createTime = System.currentTimeMillis();

    private AMQChannelMBean _managedObject;

    public AMQChannel(AMQProtocolSession session, int channelId, MessageStore messageStore)
            throws AMQException {
        _session = session;
        _channelId = channelId;

        _actor = new AMQPChannelActor(this, session.getLogActor().getRootMessageLogger());
        _logSubject = new ChannelLogSubject(this);
        _id = getConfigStore().createId();
        _actor.message(ChannelMessages.CREATE());

        getConfigStore().addConfiguredObject(this);

        _messageStore = messageStore;

        // by default the session is non-transactional
        _transaction = new AutoCommitTransaction(_messageStore);

        // message tracking related to this channel is initialised
        Andes.getInstance().clientConnectionCreated(_id);
        beginPublisherTransaction = false;
        andesChannel = Andes.getInstance().createChannel(new FlowControlListener() {
            @Override
            public void block() {
                if (!isSubscriptionChannel()) {
                    blockChannel();
                }
            }

            @Override
            public void unblock() {
                if (!isSubscriptionChannel()) {
                    unblockChannel();
                }
            }
        });

        try {
            _managedObject = new AMQChannelMBean(this);
            _managedObject.register();
        } catch (JMException e) {
            _logger.error("Error in creating AMQChannelMBean", e);
        }
    }

    public ConfigStore getConfigStore()
    {
        return getVirtualHost().getConfigStore();
    }

    /** Sets this channel to be part of a local transaction */
    public void setLocalTransactional() throws AMQException {
        _transaction = new LocalTransaction(_messageStore);
        _txnStarts.incrementAndGet();
        beginPublisherTransaction = true;
    }

    public boolean isTransactional()
    {
        // this does not look great but there should only be one "non-transactional"
        // transactional context, while there could be several transactional ones in
        // theory
        return !(_transaction instanceof AutoCommitTransaction);
    }

    public boolean inTransaction()
    {
        return isTransactional() && _txnUpdateTime.get() > 0 && _transaction.getTransactionStartTime() > 0;
    }

    private void incrementOutstandingTxnsIfNecessary()
    {
        if(isTransactional())
        {
            //There can currently only be at most one outstanding transaction
            //due to only having LocalTransaction support. Set value to 1 if 0.
            _txnCount.compareAndSet(0,1);
        }
    }

    private void decrementOutstandingTxnsIfNecessary()
    {
        if(isTransactional())
        {
            //There can currently only be at most one outstanding transaction
            //due to only having LocalTransaction support. Set value to 0 if 1.
            _txnCount.compareAndSet(1,0);
        }
    }

    public Long getTxnStarts()
    {
        return _txnStarts.get();
    }

    public Long getTxnCommits()
    {
        return _txnCommits.get();
    }

    public Long getTxnRejects()
    {
        return _txnRejects.get();
    }

    public Long getTxnCount()
    {
        return _txnCount.get();
    }

    public int getChannelId()
    {
        return _channelId;
    }

    /**
     * Set frame to publish messages.
     *
     * @param info     Publishing information of the message.
     * @param exchange The exchange message is publishing
     * @throws AMQSecurityException
     */
    public void setPublishFrame(MessagePublishInfo info, final Exchange exchange) throws AMQSecurityException {
        if (!getVirtualHost().getSecurityManager().authorisePublish(info.isImmediate(),
                info.getRoutingKey().asString(), exchange.getName()) ||
                DLCQueueUtils.isDeadLetterQueue(info.getRoutingKey().asString())) {
            throw new AMQSecurityException("Permission denied: " + exchange.getName());
        }
        _currentMessage = new IncomingMessage(info);
        _currentMessage.setExchange(exchange);
    }

    public void publishContentHeader(ContentHeaderBody contentHeaderBody)
            throws AMQException
    {
        if (_currentMessage == null)
        {
            throw new AMQException("Received content header without previously receiving a BasicPublish frame");
        }
        else
        {
            if (_logger.isDebugEnabled())
            {
                _logger.debug("Content header received on channel " + _channelId);
            }

            _currentMessage.setContentHeaderBody(contentHeaderBody);
            _currentMessage.setPublisherSessionID(_session.getSessionID());
            _currentMessage.setExpiration();
            _currentMessage.setArrivalTime();

            MessageMetaData mmd = _currentMessage.headersReceived();
            //TODO find a proper way to get the IP of the client
            mmd.set_clientIP(_session.toString().substring(0,_session.toString().indexOf(':')));

            final StoredMessage<MessageMetaData> handle = this.addAMQPMessage(mmd);
            if( handle instanceof StoredAMQPMessage){
                ((StoredAMQPMessage) handle).setChannelID(this.getId().toString());
            }
            _currentMessage.setStoredMessage(handle);

            routeCurrentMessage();

            deliverCurrentMessageIfComplete();
        }
    }

    private void deliverCurrentMessageIfComplete()
            throws AMQException
    {
        // check and deliver if header says body length is zero
        if (_currentMessage.allContentReceived())
        {
            try
            {
                //Srinath - we will do this later
                //_currentMessage.getStoredMessage().flushToStore();

                final ArrayList<? extends BaseQueue> destinationQueues = _currentMessage.getDestinationQueues();

                if(!checkMessageUserId(_currentMessage.getContentHeader()))
                {
                    _transaction.addPostTransactionAction(new WriteReturnAction(AMQConstant.ACCESS_REFUSED, "Access Refused", _currentMessage));
                }
                else
                {
                    if(destinationQueues == null || _currentMessage.getDestinationQueues().isEmpty())
                    {
                        if (_currentMessage.isMandatory() || _currentMessage.isImmediate())
                        {
                            if(!AndesContext.getInstance().isClusteringEnabled()) {
                                _transaction.addPostTransactionAction(new WriteReturnAction(AMQConstant.NO_ROUTE, "No Route for message", _currentMessage));
                            }
                        }
                        else
                        {
                            //no need to sync whole topic bindings across cluster. Thus this log has no sense in clustered mode
                            if(!AndesContext.getInstance().isClusteringEnabled()) {
                                _logger.warn("MESSAGE DISCARDED: No routes for message - " + createAMQMessage(_currentMessage));
                            }
                        }

                    }
                    else
                    {
                        /**
                         *
                         * Following code keep messages in memory, which is useless to Andes. So we are removing this. Keeping the commented line so we know
                         * we might need to add these back when we do the transactions properly.
                         * _transaction.enqueue(destinationQueues, _currentMessage, new MessageDeliveryAction(_currentMessage, destinationQueues, isTransactional()));
                         * incrementOutstandingTxnsIfNecessary();
                         * updateTransactionalActivity();
                         */
                    }
                }

                //TODO
                //check queue size from here and reject the request

                //Set channel details
                //Substring to remove leading slash character from address
                if (null != ((AMQProtocolEngine) this._session).getAddress()) {
                    andesChannel.setIdentifier(
                            ((AMQProtocolEngine) this._session).getAddress().substring(1));
                } else {
                    andesChannel.setIdentifier("AMQP-Unknown");
                }
                andesChannel.setDestination(this._currentMessage.getRoutingKey());

                //need to bind this to the inner class, as _currentMessage
                final IncomingMessage incomingMessage = _currentMessage;

                try {
                    /*
                     * All we have to do is to write content, metadata,
                     * and add the message id to the global queue
                     * Content are already added to the same work queue
                     * adding metadata and message to global queue
                     * happen here
                     */
                    if (beginPublisherTransaction) {
                        andesTransactionEvent = Andes.getInstance().newTransaction(andesChannel);
                        beginPublisherTransaction = false;
                    }
                    QpidAndesBridge.messageReceived(incomingMessage, getId(), andesChannel, andesTransactionEvent);

                } catch (Throwable e) {
                    _logger.error("Error processing completed messages, Close the session " + getSessionName(), e);
                    // We mark the session as closed due to error
                    if (_session instanceof AMQProtocolEngine) {
                        ((AMQProtocolEngine) _session).closeProtocolSession();
                    }
                }

            } finally {
                long bodySize = _currentMessage.getSize();
                long timestamp = ((BasicContentHeaderProperties) _currentMessage.getContentHeader().getProperties()).getTimestamp();
                _session.registerMessageReceived(bodySize, timestamp);
                _currentMessage = null;
            }
        }

    }

    public void publishContentBody(ContentBody contentBody) throws AMQException
    {
        if (_currentMessage == null)
        {
            throw new AMQException("Received content body without previously receiving a JmsPublishBody");
        }

        if (_logger.isDebugEnabled())
        {
            _logger.debug(debugIdentity() + "Content body received on channel " + _channelId);
        }

        try
        {
            // returns true iff the message was delivered (i.e. if all data was
            // received
            final ContentChunk contentChunk =
                    _session.getMethodRegistry().getProtocolVersionMethodConverter().convertToContentChunk(contentBody);

            _currentMessage.addContentBodyFrame(contentChunk);

            deliverCurrentMessageIfComplete();
        }
        catch (AMQException e)
        {
            // we want to make sure we don't keep a reference to the message in the
            // event of an error
            _currentMessage = null;
            throw e;
        }
    }

    protected void routeCurrentMessage() throws AMQException
    {
        _currentMessage.route();
    }

    public long getNextDeliveryTag()
    {
        return _deliveryTag.incrementAndGet();
    }


    public long getCurrentDeliveryTag() {
        return _deliveryTag.get();
    }

    public int getNextConsumerTag()
    {
        return _consumerTag.incrementAndGet();
    }


    public Subscription getSubscription(AMQShortString subscription)
    {
        return _tag2SubscriptionMap.get(subscription);
    }

    /**
     * Subscribe to a queue. We register all subscriptions in the channel so that if the channel is closed we can clean
     * up all subscriptions, even if the client does not explicitly unsubscribe from all queues.
     *
     * @param tag       the tag chosen by the client (if null, server will generate one)
     * @param queue     the queue to subscribe to
     * @param acks      Are acks enabled for this subscriber
     * @param filters   Filters to apply to this subscriber
     *
     * @param noLocal   Flag stopping own messages being receivied.
     * @param exclusive Flag requesting exclusive access to the queue
     * @return the consumer tag. This is returned to the subscriber and used in subsequent unsubscribe requests
     *
     * @throws AMQException                  if something goes wrong
     */
    public AMQShortString subscribeToQueue(AMQShortString tag, AMQQueue queue, boolean acks,
                                           FieldTable filters, boolean noLocal, boolean exclusive) throws AMQException {
        if (tag == null)
        {
            tag = new AMQShortString("sgen_" + getNextConsumerTag());
        }

        if (_tag2SubscriptionMap.size() > 0)
        {
            // According to Andes architecture, a channel can contain only one consumer, hence restricting it here
            throw new AMQException("Creating multiple consumers per channel is not allowed by Andes");
        }

        // Inside Andes, it is considered as there's only one subscriber per channel. Hence this will run only once.
        _unacknowledgedMessageMap = new UnacknowledgedMessageMapImpl(DEFAULT_PREFETCH, this, queue.isDurable());

        Subscription subscription =
                SubscriptionFactoryImpl.INSTANCE.createSubscription(_channelId, _session, tag, acks, filters, noLocal, _creditManager);

        try {
            //tell Andes Kernel to register a subscription
            queue.registerSubscription(subscription, exclusive);
            QpidAndesBridge.createAMQPSubscription(subscription, queue);
        } catch (AMQException e) {
            _tag2SubscriptionMap.remove(tag);
            throw e;
        }

        // So to keep things straight we put before the call and catch all exceptions from the register and tidy up.
        // We add before we register as the Async Delivery process may AutoClose the subscriber
        // so calling _cT2QM.remove before we have done put which was after the register succeeded.
        // So to keep things straight we put before the call and catch all exceptions from the register and tidy up.

        _tag2SubscriptionMap.put(tag, subscription);

        return tag;
    }

    public boolean isSubscriptionChannel (){
        return (_tag2SubscriptionMap.size()) > 0 ? true : false;
    }


    /**
     * Unsubscribe a consumer from a queue.
     * @param consumerTag
     * @return true if the consumerTag had a mapped queue that could be unregistered.
     * @throws AMQException
     */
    public boolean unsubscribeConsumer(AMQShortString consumerTag) throws AMQException
    {

        Subscription sub = _tag2SubscriptionMap.remove(consumerTag);
        if (sub != null)
        {
            //TODO- hasitha - we need to review this? do not we need to iterate over bindings?
            try
            {
//            	List<Binding> bindingList = sub.getQueue().getBindings();
//				if(bindingList !=null && !bindingList.isEmpty())
//				for(Binding b : bindingList) {
//				    Exchange exchange = b.getExchange();
//				    if (exchange.getName().equalsIgnoreCase("amq.direct")) {
//				    	String subscriptionID = String.valueOf(subscription.getSubscription().getSubscriptionID());
//				        String destinationQueue = b.getBindingKey();
//				    }
//				}
                sub.getSendLock();
                sub.getQueue().unregisterSubscription(sub);
//			    ClusteringEnabledSubscriptionManager csm = ClusterResourceHolder.getInstance().getSubscriptionManager();
//			    csm.closeLocalSubscription(sub.getQueue().getResourceName(), String.valueOf(sub.getSubscriptionID()));
//            }catch(AndesException e){
//            	throw new AMQException(AMQConstant.INTERNAL_ERROR, e.getMessage(),  e);
            }finally
            {
                sub.releaseSendLock();
            }
            return true;
        }
        else
        {
            _logger.warn("Attempt to unsubscribe consumer with tag '"+consumerTag+"' which is not registered.");
        }
        return false;
    }

    /**
     * Called from the protocol session to close this channel and clean up. T
     *
     * @throws AMQException if there is an error during closure
     */
    public void close() throws AMQException
    {
        if(!_closing.compareAndSet(false, true)) {
            //Channel is already closing
            _logger.debug("Channel " + _channelId + " is already closing. Hence dropping close request.");
            return;
        }

        try {
            CurrentActor.get().message(_logSubject, ChannelMessages.CLOSE());

            unsubscribeAllConsumers();
            _transaction.rollback();


            if (null != andesTransactionEvent) {
                andesTransactionEvent.close();
            }

            try {
                requeue();
            } catch (AMQException e) {
                _logger.error("Caught AMQException whilst attempting to reque:" + e);
            }

            getConfigStore().removeConfiguredObject(this);

            forgetMessages4Channel();
            if (_managedObject != null) {
                _managedObject.unregister();
            }
        } catch (AndesException e) {
            throw new AMQException("Exception occurred while closing channel " + _channelId, e);
        } finally {
            QpidAndesBridge.channelIsClosing(this.getId());
            Andes.getInstance().deleteChannel(andesChannel);
        }

    }

    private void unsubscribeAllConsumers() throws AMQException
    {
        if (_logger.isInfoEnabled())
        {
            if (!_tag2SubscriptionMap.isEmpty())
            {
                _logger.info("Unsubscribing all consumers on channel " + toString());
            }
            else
            {
                _logger.info("No consumers to unsubscribe on channel " + toString());
            }
        }

        for (Map.Entry<AMQShortString, Subscription> me : _tag2SubscriptionMap.entrySet())
        {
            if (_logger.isInfoEnabled())
            {
                _logger.info("Unsubscribing consumer '" + me.getKey() + "' on channel " + toString());
            }

            Subscription sub = me.getValue();

            try
            {
                sub.getSendLock();
                sub.getQueue().unregisterSubscription(sub);
            }
            finally
            {
                sub.releaseSendLock();
            }

        }

        _tag2SubscriptionMap.clear();
    }

    /**
     * Add a message to the channel-based list of unacknowledged messages
     *
     * @param entry       the record of the message on the queue that was delivered
     * @param deliveryTag the delivery tag used when delivering the message (see protocol spec for description of the
     *                    delivery tag)
     * @param subscription The consumer that is to acknowledge this message.
     */
    public void addUnacknowledgedMessage(QueueEntry entry, long deliveryTag, Subscription subscription)
    {
        if (_logger.isDebugEnabled())
        {
            if (entry.getQueue() == null)
            {
                _logger.debug("Adding unacked message with a null queue:" + entry);
            }
            else
            {
                if (_logger.isDebugEnabled())
                {
                    _logger.debug(debugIdentity() + " Adding unacked message(" + entry.getMessage().toString() + " DT:" + deliveryTag
                                  + ") with a queue(" + entry.getQueue() + ") for " + subscription);
                }
            }
        }


        /**Todo -Shammi- We are not using unack map in Queue scenario. Because of that this map is growing unnecessarily in Queues and
         * created OOM exceptions.
         * But we need to have this map for topics, other wise it will create "https://wso2.org/jira/browse/MB-70"  */
        _unacknowledgedMessageMap.add(deliveryTag, entry);

    }

    private final String id = "(" + System.identityHashCode(this) + ")";

    public String debugIdentity()
    {
        return _channelId + id;
    }

    /**
     * Called to attempt re-delivery all outstanding unacknowledged messages on the channel. May result in delivery to
     * this same channel or to other subscribers.
     *
     * @throws org.wso2.andes.AMQException if the requeue fails
     */
    public void requeue() throws AMQException
    {
        // we must create a new map since all the messages will get a new delivery tag when they are redelivered
        Collection<QueueEntry> messagesToBeDelivered = _unacknowledgedMessageMap.cancelAllMessages();

        if (!messagesToBeDelivered.isEmpty())
        {
            if (_logger.isInfoEnabled())
            {
                _logger.info("Requeuing " + messagesToBeDelivered.size() + " unacked messages. for " + toString());
            }

        }

        for (QueueEntry unacked : messagesToBeDelivered)
        {
            if (!unacked.isQueueDeleted())
            {
                // Mark message redelivered
                unacked.setRedelivered();

                // Ensure message is released for redelivery
                unacked.release();

            }
            else
            {
                unacked.discard();
            }
        }

    }

    /**
     * Requeue a single message
     *
     * @param deliveryTag The message to requeue
     *
     * @throws AMQException If something goes wrong.
     */
    public void requeue(long deliveryTag) throws AMQException
    {
        QueueEntry unacked = _unacknowledgedMessageMap.remove(deliveryTag);

        if (unacked != null)
        {
            // Mark message redelivered
            unacked.setRedelivered();

            // Ensure message is released for redelivery
            if (!unacked.isQueueDeleted())
            {

                // Ensure message is released for redelivery
                unacked.release();

            }
            else
            {
                _logger.warn(System.identityHashCode(this) + " Requested requeue of message(" + unacked
                             + "):" + deliveryTag + " but no queue defined and no DeadLetter queue so DROPPING message.");

                unacked.discard();
            }
        }
        else
        {
            _logger.warn("Requested requeue of message:" + deliveryTag + " but no such delivery tag exists."
                         + _unacknowledgedMessageMap.size());

        }

    }

    /**
     * Called to resend all outstanding unacknowledged messages to this same channel.
     *
     * @param requeue Are the messages to be requeued or dropped.
     *
     * @throws AMQException When something goes wrong.
     */
    public void resend(final boolean requeue) throws AMQException
    {


        final Map<Long, QueueEntry> msgToRequeue = new LinkedHashMap<Long, QueueEntry>();
        final Map<Long, QueueEntry> msgToResend = new LinkedHashMap<Long, QueueEntry>();

        if (_logger.isDebugEnabled())
        {
            _logger.debug("unacked map Size:" + _unacknowledgedMessageMap.size());
        }

        // Process the Unacked-Map.
        // Marking messages who still have a consumer for to be resent
        // and those that don't to be requeued.
        _unacknowledgedMessageMap.visit(new ExtractResendAndRequeue(_unacknowledgedMessageMap,
                                                                    msgToRequeue,
                                                                    msgToResend,
                                                                    requeue,
                                                                    _messageStore));


        // Process Messages to Resend
        if (_logger.isDebugEnabled())
        {
            if (!msgToResend.isEmpty())
            {
                _logger.debug("Preparing (" + msgToResend.size() + ") message to resend.");
            }
            else
            {
                _logger.debug("No message to resend.");
            }
        }

        for (Map.Entry<Long, QueueEntry> entry : msgToResend.entrySet())
        {
            QueueEntry message = entry.getValue();
            long deliveryTag = entry.getKey();



            ServerMessage msg = message.getMessage();
            AMQQueue queue = message.getQueue();

            // Our Java Client will always suspend the channel when resending!
            // If the client has requested the messages be resent then it is
            // their responsibility to ensure that thay are capable of receiving them
            // i.e. The channel hasn't been server side suspended.
            // if (isSuspended())
            // {
            // _logger.info("Channel is suspended so requeuing");
            // //move this message to requeue
            // msgToRequeue.add(message);
            // }
            // else
            // {
            // release to allow it to be delivered

            // Without any details from the client about what has been processed we have to mark
            // all messages in the unacked map as redelivered.
            message.setRedelivered();

            Subscription sub = message.getDeliveredSubscription();

            if (sub != null)
            {

                if(!queue.resend(message,sub))
                {
                    msgToRequeue.put(deliveryTag, message);
                }
            }
            else
            {

                if (_logger.isInfoEnabled())
                {
                    _logger.info("DeliveredSubscription not recorded so just requeueing(" + message.toString()
                                 + ")to prevent loss");
                }
                // move this message to requeue
                msgToRequeue.put(deliveryTag, message);
            }
        } // for all messages
        // } else !isSuspend

        if (_logger.isInfoEnabled())
        {
            if (!msgToRequeue.isEmpty())
            {
                _logger.info("Preparing (" + msgToRequeue.size() + ") message to requeue to.");
            }
        }

        // Process Messages to Requeue at the front of the queue
        for (Map.Entry<Long, QueueEntry> entry : msgToRequeue.entrySet())
        {
            QueueEntry message = entry.getValue();
            long deliveryTag = entry.getKey();
            _unacknowledgedMessageMap.remove(deliveryTag);

            message.setRedelivered();
            message.release();

        }
    }

    public boolean isMessagesAcksProcessing = false;
    /**
     * Acknowledge one or more messages.
     *
     * @param deliveryTag the last delivery tag
     * @param multiple    if true will acknowledge all messages up to an including the delivery tag. if false only
     *                    acknowledges the single message specified by the delivery tag
     *
     * @throws AMQException if the delivery tag is unknown (e.g. not outstanding) on this channel
     */
    public void acknowledgeMessage(long deliveryTag, boolean multiple) throws AMQException
    {
        isMessagesAcksProcessing =  true;
        Collection<QueueEntry> ackedMessages = getAckedMessages(deliveryTag, multiple);
        _transaction.dequeue(ackedMessages, new MessageAcknowledgeAction(ackedMessages));

        for (QueueEntry entry : ackedMessages) {
            /**
             * When the message is acknowledged it is informed to Andes Kernel
             */
            QpidAndesBridge.ackReceived(this.getId(), entry.getMessage().getMessageNumber());
        }

        updateTransactionalActivity();
        isMessagesAcksProcessing = false;
    }

    private Collection<QueueEntry> getAckedMessages(long deliveryTag, boolean multiple)
    {

        Map<Long, QueueEntry> ackedMessageMap = new LinkedHashMap<Long,QueueEntry>();
        synchronized (_unacknowledgedMessageMap) {
            _unacknowledgedMessageMap.collect(deliveryTag, multiple, ackedMessageMap);
            _unacknowledgedMessageMap.remove(ackedMessageMap);
        }
        return ackedMessageMap.values();
    }

    /**
     * Used only for testing purposes.
     *
     * @return the map of unacknowledged messages
     */
    public UnacknowledgedMessageMap getUnacknowledgedMessageMap()
    {
        return _unacknowledgedMessageMap;
    }

    /**
     * Called from the ChannelFlowHandler to suspend this Channel
     * @param suspended boolean, should this Channel be suspended
     */
    public void setSuspended(boolean suspended)
    {
        boolean wasSuspended = _suspended.getAndSet(suspended);
        if (wasSuspended != suspended)
        {
            // Log Flow Started before we start the subscriptions
            if (!suspended)
            {
                _actor.message(_logSubject, ChannelMessages.FLOW("Started"));
            }


            // This section takes two different approaches to perform to perform
            // the same function. Ensuring that the Subscription has taken note
            // of the change in Channel State

            // Here we have become unsuspended and so we ask each the queue to
            // perform an Async delivery for each of the subscriptions in this
            // Channel. The alternative would be to ensure that the subscription
            // had received the change in suspension state. That way the logic
            // behind decieding to start an async delivery was located with the
            // Subscription.
            if (wasSuspended)
            {
                // may need to deliver queued messages
                for (Subscription s : _tag2SubscriptionMap.values())
                {
                    s.getQueue().deliverAsync(s);
                }
            }


            // Here we have become suspended so we need to ensure that each of
            // the Subscriptions has noticed this change so that we can be sure
            // they are not still sending messages. Again the code here is a
            // very simplistic approach to ensure that the change of suspension
            // has been noticed by each of the Subscriptions. Unlike the above
            // case we don't actually need to do anything else.
            if (!wasSuspended)
            {
                // may need to deliver queued messages
                for (Subscription s : _tag2SubscriptionMap.values())
                {
                    try
                    {
                        s.getSendLock();
                    }
                    finally
                    {
                        s.releaseSendLock();
                    }
                }
            }


            // Log Suspension only after we have confirmed all suspensions are
            // stopped.
            if (suspended)
            {
                _actor.message(_logSubject, ChannelMessages.FLOW("Stopped"));
            }

        }
    }

    public boolean isSuspended()
    {
        return _suspended.get()  || _closing.get() || _session.isClosing();
    }

    public void commit() throws AMQException
    {
        if (!isTransactional())
        {
            throw new AMQException("Fatal error: commit called on non-transactional channel");
        }

        _transaction.commit();
        if(null != andesTransactionEvent) {
            try {
                andesTransactionEvent.commit();
            } catch (AndesException e) {
                throw new AMQException(AMQConstant.INTERNAL_ERROR,
                        "Error occurred while committing transaction.", e);
            }
        }
        _txnCommits.incrementAndGet();
        _txnStarts.incrementAndGet();
        decrementOutstandingTxnsIfNecessary();
    }

    public void rollback() throws AMQException
    {
        rollback(NULL_TASK);
    }

    public void rollback(Runnable postRollbackTask) throws AMQException
    {
        if (!isTransactional())
        {
            throw new AMQException("Fatal error: commit called on non-transactional channel");
        }

        // stop all subscriptions
        _rollingBack = true;
        boolean requiresSuspend = _suspended.compareAndSet(false,true);

        // ensure all subscriptions have seen the change to the channel state
        for(Subscription sub : _tag2SubscriptionMap.values())
        {
            sub.getSendLock();
            sub.releaseSendLock();
        }

        if(null != andesTransactionEvent) {
            try {
                andesTransactionEvent.rollback();
            } catch (AndesException e) {
                throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error occurred while rollback ", e);
            }
        }

        try
        {
            _transaction.rollback();
        }
        finally
        {
            _rollingBack = false;

            _txnRejects.incrementAndGet();
            _txnStarts.incrementAndGet();
            decrementOutstandingTxnsIfNecessary();
        }

        postRollbackTask.run();

        for(QueueEntry entry : _resendList)
        {
            Subscription sub = entry.getDeliveredSubscription();
            if(sub == null || sub.isClosed())
            {
                entry.release();
            }
            else
            {
                sub.getQueue().resend(entry, sub);
            }
        }
        _resendList.clear();

        if(requiresSuspend)
        {
            _suspended.set(false);
            for(Subscription sub : _tag2SubscriptionMap.values())
            {
                sub.getQueue().deliverAsync(sub);
            }

        }


    }

    /**
     * Update last transaction activity timestamp
     */
    private void updateTransactionalActivity()
    {
        if (isTransactional())
        {
            _txnUpdateTime.set(System.currentTimeMillis());
        }
    }

    public String toString()
    {
        return "["+_session.toString()+":"+_channelId+"]";
    }

    public void setDefaultQueue(AMQQueue queue)
    {
        _defaultQueue = queue;
    }

    public AMQQueue getDefaultQueue()
    {
        return _defaultQueue;
    }


    public boolean isClosing()
    {
        return _closing.get();
    }

    public AMQProtocolSession getProtocolSession()
    {
        return _session;
    }

    public FlowCreditManager getCreditManager()
    {
        return _creditManager;
    }

    public void setCredit(final long prefetchSize, final int prefetchCount)
    {
        _actor.message(ChannelMessages.PREFETCH_SIZE(prefetchSize, prefetchCount));
        _creditManager.setCreditLimits(prefetchSize, prefetchCount);
    }

    public MessageStore getMessageStore()
    {
        return _messageStore;
    }

    private final ClientDeliveryMethod _clientDeliveryMethod = new ClientDeliveryMethod()
    {

        public void deliverToClient(final Subscription sub, final QueueEntry entry, final long deliveryTag)
                throws AMQException
        {
            getProtocolSession().getProtocolOutputConverter().writeDeliver(entry, getChannelId(),
                                                                           deliveryTag, sub.getConsumerTag());
            _session.registerMessageDelivered(entry.getMessage().getSize());
        }

    };

    public ClientDeliveryMethod getClientDeliveryMethod()
    {
        return _clientDeliveryMethod;
    }

    private final RecordDeliveryMethod _recordDeliveryMethod = new RecordDeliveryMethod()
    {

        public void recordMessageDelivery(final Subscription sub, final QueueEntry entry, final long deliveryTag)
        {
            addUnacknowledgedMessage(entry, deliveryTag, sub);
        }
    };

    public RecordDeliveryMethod getRecordDeliveryMethod()
    {
        return _recordDeliveryMethod;
    }


    private AMQMessage createAMQMessage(IncomingMessage incomingMessage)
            throws AMQException
    {

        AMQMessage message = new AMQMessage(incomingMessage.getStoredMessage());

        message.setExpiration(incomingMessage.getExpiration());
        message.setClientIdentifier(_session);
        message.setPublisherSessionID(_session.getSessionID());
        return message;
    }

    private boolean checkMessageUserId(ContentHeaderBody header)
    {
        AMQShortString userID =
                header.getProperties() instanceof BasicContentHeaderProperties
                ? ((BasicContentHeaderProperties) header.getProperties()).getUserId()
                : null;

        return (!MSG_AUTH || _session.getAuthorizedPrincipal().getName().equals(userID == null? "" : userID.toString()));

    }

    public Object getID()
    {
        return _channelId;
    }

    public AMQConnectionModel getConnectionModel()
    {
        return _session;
    }

    public String getClientID()
    {
        return String.valueOf(_session.getContextKey());
    }

    public LogSubject getLogSubject()
    {
        return _logSubject;
    }

    private class MessageDeliveryAction implements ServerTransaction.Action
    {
        private IncomingMessage _incommingMessage;
        private ArrayList<? extends BaseQueue> _destinationQueues;

        public MessageDeliveryAction(IncomingMessage currentMessage,
                                     ArrayList<? extends BaseQueue> destinationQueues,
                                     boolean transactional)
        {
            _incommingMessage = currentMessage;
            _destinationQueues = destinationQueues;
        }

        public void postCommit()
        {
            try
            {
                final boolean immediate = _incommingMessage.isImmediate();

                final AMQMessage amqMessage = createAMQMessage(_incommingMessage);
                MessageReference ref = amqMessage.newReference();

                for(final BaseQueue queue : _destinationQueues)
                {
                    BaseQueue.PostEnqueueAction action;

                    if(immediate)
                    {
                        action = new ImmediateAction(queue);
                    }
                    else
                    {
                        action = null;
                    }

                    queue.enqueue(amqMessage, action);

                    if(queue instanceof AMQQueue)
                    {
                        ((AMQQueue)queue).checkCapacity(AMQChannel.this);
                    }

                }
                ref.release();
            }
            catch (AMQException e)
            {
                // TODO
                throw new RuntimeException(e);
            }





        }

        public void onRollback()
        {
            // Maybe keep track of entries that were created and then delete them here in case of failure
            // to in memory enqueue
        }

        private class ImmediateAction implements BaseQueue.PostEnqueueAction
        {
            private final BaseQueue _queue;

            public ImmediateAction(BaseQueue queue)
            {
                _queue = queue;
            }

            public void onEnqueue(QueueEntry entry)
            {
                if (!entry.getDeliveredToConsumer() && entry.acquire())
                {


                    ServerTransaction txn = new LocalTransaction(_messageStore);
                    Collection<QueueEntry> entries = new ArrayList<QueueEntry>(1);
                    entries.add(entry);
                    final AMQMessage message = (AMQMessage) entry.getMessage();
                    txn.dequeue(_queue, entry.getMessage(),
                                new MessageAcknowledgeAction(entries)
                                {
                                    @Override
                                    public void postCommit()
                                    {
                                        try
                                        {
                                            final
                                            ProtocolOutputConverter outputConverter =
                                                    _session.getProtocolOutputConverter();

                                            outputConverter.writeReturn(message.getMessagePublishInfo(),
                                                                        message.getContentHeaderBody(),
                                                                        message,
                                                                        _channelId,
                                                                        AMQConstant.NO_CONSUMERS.getCode(),
                                                                        IMMEDIATE_DELIVERY_REPLY_TEXT);
                                        }
                                        catch (AMQException e)
                                        {
                                            throw new RuntimeException(e);
                                        }
                                        super.postCommit();
                                    }
                                }
                    );
                    txn.commit();


                }

            }
        }
    }

    private class MessageAcknowledgeAction implements ServerTransaction.Action
    {
        private final Collection<QueueEntry> _ackedMessages;


        public MessageAcknowledgeAction(Collection<QueueEntry> ackedMessages)
        {
            _ackedMessages = ackedMessages;
        }

        public void postCommit()
        {
            try
            {
                for(QueueEntry entry : _ackedMessages)
                {
                    entry.discard();
                }
            }
            finally
            {
                _acknowledgedMessages.clear();
            }

        }

        public void onRollback()
        {
            // explicit rollbacks resend the message after the rollback-ok is sent
            if(_rollingBack)
            {
                _resendList.addAll(_ackedMessages);
            }
            else
            {
                try
                {
                    for(QueueEntry entry : _ackedMessages)
                    {
                        entry.release();
                    }
                }
                finally
                {
                    _acknowledgedMessages.clear();
                }
            }

        }
    }

    private class WriteReturnAction implements ServerTransaction.Action
    {
        private final AMQConstant _errorCode;
        private final IncomingMessage _message;
        private final String _description;

        public WriteReturnAction(AMQConstant errorCode,
                                 String description,
                                 IncomingMessage message)
        {
            _errorCode = errorCode;
            _message = message;
            _description = description;
        }

        public void postCommit()
        {
            try
            {
                _session.getProtocolOutputConverter().writeReturn(_message.getMessagePublishInfo(),
                                                                  _message.getContentHeader(),
                                                                  _message,
                                                                  _channelId,
                                                                  _errorCode.getCode(),
                                                                  new AMQShortString(_description));
            }
            catch (AMQException e)
            {
                //TODO
                throw new RuntimeException(e);
            }

        }

        public void onRollback()
        {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }


    public LogActor getLogActor()
    {
        return _actor;
    }

    public void block(AMQQueue queue)
    {
        //if(_blockingQueues.putIfAbsent(queue, Boolean.TRUE) == null)
        //{
        if(_blocking.compareAndSet(false, true))
        {
            _actor.message(_logSubject, ChannelMessages.FLOW_ENFORCED(queue.getNameShortString().toString()));
            flow(false);
        }
        //}
    }

    public void blockChannel() {
        if(!isClosing() || _blocking.compareAndSet(Boolean.FALSE, Boolean.TRUE))
        {
            flow(false);
        }
    }

    public void unblock(AMQQueue queue)
    {
        //if(_blockingQueues.remove(queue))
        //{
        if(_blocking.compareAndSet(true,false))
        {
            _actor.message(_logSubject, ChannelMessages.FLOW_REMOVED());
            flow(true);
        }
        //}
    }

    public void unblockChannel() {
        if(!isClosing() || _blocking.compareAndSet(Boolean.TRUE, Boolean.FALSE))
        {
            flow(true);
        }
    }

    private void flow(boolean flow)
    {
        if (_logger.isDebugEnabled()) {
            _logger.debug("Communicating the Server Side Flow Control Ack To The Client");
        }
        MethodRegistry methodRegistry = _session.getMethodRegistry();
        AMQMethodBody responseBody = methodRegistry.createChannelFlowBody(flow);
        _session.writeFrame(responseBody.generateFrame(_channelId));
    }

    public boolean getBlocking()
    {
        return _blocking.get();
    }

    public VirtualHost getVirtualHost()
    {
        return getProtocolSession().getVirtualHost();
    }


    public ConfiguredObject getParent()
    {
        return getVirtualHost();
    }

    public SessionConfigType getConfigType()
    {
        return SessionConfigType.getInstance();
    }

    public int getChannel()
    {
        return getChannelId();
    }

    public boolean isAttached()
    {
        return true;
    }

    public long getDetachedLifespan()
    {
        return 0;
    }

    public ConnectionConfig getConnectionConfig()
    {
        return (AMQProtocolEngine)getProtocolSession();
    }

    public Long getExpiryTime()
    {
        return null;
    }

    public Long getMaxClientRate()
    {
        return null;
    }

    public boolean isDurable()
    {
        return false;
    }

    public UUID getId()
    {
        return _id;
    }

    public String getSessionName()
    {
        return getConnectionConfig().getAddress() + "/" + getChannelId();
    }

    public long getCreateTime()
    {
        return _createTime;
    }

    public void mgmtClose() throws AMQException
    {
        _session.mgmtCloseChannel(_channelId);
    }

    public void checkTransactionStatus(long openWarn, long openClose, long idleWarn, long idleClose) throws AMQException
    {
        if (inTransaction())
        {
            long currentTime = System.currentTimeMillis();
            long openTime = currentTime - _transaction.getTransactionStartTime();
            long idleTime = currentTime - _txnUpdateTime.get();

            // Log a warning on idle or open transactions
            if (idleWarn > 0L && idleTime > idleWarn)
            {
                CurrentActor.get().message(_logSubject, ChannelMessages.IDLE_TXN(idleTime));
                _logger.warn("IDLE TRANSACTION ALERT " + _logSubject.toString() + " " + idleTime + " ms");
            }
            else if (openWarn > 0L && openTime > openWarn)
            {
                CurrentActor.get().message(_logSubject, ChannelMessages.OPEN_TXN(openTime));
                _logger.warn("OPEN TRANSACTION ALERT " + _logSubject.toString() + " " + openTime + " ms");
            }

            // Close connection for idle or open transactions that have timed out
            if (idleClose > 0L && idleTime > idleClose)
            {
                getConnectionModel().closeSession(this, AMQConstant.RESOURCE_ERROR, "Idle transaction timed out");
            }
            else if (openClose > 0L && openTime > openClose)
            {
                getConnectionModel().closeSession(this, AMQConstant.RESOURCE_ERROR, "Open transaction timed out");
            }
        }
    }

    public void forgetMessages4Channel(){
        while (isMessagesAcksProcessing){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public <T extends StorableMessageMetaData> StoredMessage<T> addAMQPMessage(T metaData){
        long mid = 0; // Message IDs will be given By Andes
        if (_logger.isDebugEnabled()) {
            _logger.debug("MessageID generated:" + mid);
        }
        return new StoredAMQPMessage(mid, metaData);
    }

    /**
     * Set the message ID of the last rejected message.
     * Currently called by QpidAndesBridge.rejectMessage.
     * @param lastRejectedMessageId
     */
    public void setLastRejectedMessageId(long lastRejectedMessageId) {
        this.lastRejectedMessageId = lastRejectedMessageId;
    }

    /**
     * Mark the lastRejectedMessageID as the lastRollbackedID.
     * Called upon receiving a rollback request from client. (TxRollbackHandler.methodReceived)
     */
    public void setLastRollbackedMessageId() {
        if (_logger.isDebugEnabled()) {
            _logger.debug("set LastRollbackedMessageId to : " + lastRejectedMessageId);
        }
        this.lastRollbackedMessageId = this.lastRejectedMessageId;
    }

    /**
     * Reset lastRollbackedMessageId upon a commit request from client. (TxCommitHandler.methodReceived)
     */
    public void resetLastRollbackedMessageId() {
        if (_logger.isDebugEnabled()) {
            _logger.debug("reset LastRollbackedMessageId to -1");
        }
        this.lastRollbackedMessageId = -1;
    }

    /**
     * This is to handle the extra messages beyond the actual rollback point, which are rejected from the client side
     * message buffer.
     * @param messageId message Id
     * @return true if the message is placed after the last rollbacked message.
     */
    public boolean isMessageBeyondLastRollback(long messageId) {

        if (lastRollbackedMessageId < 0) {
            // (1) Message is either within the last committed transaction or,
            // (2) this channel is not session-transacted.
            return false;
        } else {
            // True if the message is placed after the last rollbacked message, in which case it's redelivery should
            // not be counted.
            return (messageId > lastRollbackedMessageId);
        }
    }


}
