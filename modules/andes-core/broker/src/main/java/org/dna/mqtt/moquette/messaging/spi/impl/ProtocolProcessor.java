package org.dna.mqtt.moquette.messaging.spi.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.IgnoreExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.dsl.Disruptor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dna.mqtt.moquette.messaging.spi.IMatchingCondition;
import org.dna.mqtt.moquette.messaging.spi.IStorageService;
import org.dna.mqtt.moquette.messaging.spi.impl.events.MessagingEvent;
import org.dna.mqtt.moquette.messaging.spi.impl.events.OutputMessagingEvent;
import org.dna.mqtt.moquette.messaging.spi.impl.events.PublishEvent;
import org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription;
import org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.SubscriptionsStore;
import org.dna.mqtt.moquette.proto.messages.*;
import org.dna.mqtt.moquette.server.ConnectionDescriptor;
import org.dna.mqtt.moquette.server.Constants;
import org.dna.mqtt.moquette.server.IAuthenticator;
import org.dna.mqtt.moquette.server.ServerChannel;
import org.dna.mqtt.wso2.AndesMQTTBridge;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.MQTTUserAuthenticationScheme;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.distruptor.inbound.PubAckHandler;
import org.wso2.andes.mqtt.MQTTException;
import org.wso2.andes.mqtt.MQTTUtils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.wso2.andes.configuration.enums.AndesConfiguration.TRANSPORTS_MQTT_DELIVERY_BUFFER_SIZE;
import static org.wso2.andes.configuration.enums.AndesConfiguration.TRANSPORTS_MQTT_USER_ATHENTICATION;

public class ProtocolProcessor implements EventHandler<ValueEvent>, PubAckHandler {

    private static Log log = LogFactory.getLog(ProtocolProcessor.class);

    private Map<String, ConnectionDescriptor> m_clientIDs = new HashMap<String, ConnectionDescriptor>();
    private SubscriptionsStore subscriptions;
    private IStorageService m_storageService;
    private IAuthenticator m_authenticator;

    private RingBuffer<ValueEvent> m_ringBuffer;

    /**
     * Indicates (via configuration) that server should always expect credentials from users.
     */
    private boolean isAuthenticationRequired;
    
    ProtocolProcessor() {
    }

    /**
     * @param subscriptions  the subscription store where are stored all the existing
     *                       clients subscriptions.
     * @param storageService the persistent store to use for save/load of messages
     *                       for QoS1 and QoS2 handling.
     * @param authenticator  the authenticator used in connect messages
     */
    void init(SubscriptionsStore subscriptions, IStorageService storageService,
              IAuthenticator authenticator) {
        //m_clientIDs = clientIDs;
        this.subscriptions = subscriptions;
        m_authenticator = authenticator;
        m_storageService = storageService;
        
        isAuthenticationRequired = 
                    AndesConfigurationManager.readValue(TRANSPORTS_MQTT_USER_ATHENTICATION) == MQTTUserAuthenticationScheme.REQUIRED;
        
        Integer RingBufferSize = AndesConfigurationManager.readValue(TRANSPORTS_MQTT_DELIVERY_BUFFER_SIZE);
        
        // Init the output Disruptor
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("Disruptor MQTT Protocol Processor thread %d").build();
        ExecutorService executor = Executors.newCachedThreadPool(namedThreadFactory);

        Disruptor<ValueEvent> disruptor = new Disruptor<ValueEvent>(
                ValueEvent.EVENT_FACTORY,
                RingBufferSize,
                executor);
        
        disruptor.handleExceptionsWith(new IgnoreExceptionHandler());
        SequenceBarrier barrier = disruptor.getRingBuffer().newBarrier();
        BatchEventProcessor<ValueEvent> m_eventProcessor = new BatchEventProcessor<ValueEvent>(
                disruptor.getRingBuffer(), barrier, this);
        
        disruptor.handleEventsWith(m_eventProcessor);

        m_ringBuffer = disruptor.start();
        //Will initialize the bridge
        //Andes Specific
        initAndesBridge(subscriptions, storageService);
    }

    /*Will shake hands with the kernal*/
    /*Andes Specific*/
    private void initAndesBridge(SubscriptionsStore subscriptions, IStorageService storageService) {
        //bridge = new AndesMQTTBridge(this);
        // bridge = AndesMQTTBridge.getBridgeInstance(this);
        //Will create the bridge and intiialise the protocol
        try {
            AndesMQTTBridge.initMQTTProtocolProcessor(this);
        } catch (MQTTException e) {
            final String message = "Error occured when initializing MQTT connection with Andes ";
            log.error(message + e.getMessage(), e);
        }
        //Should clear all the stored messages
        subscriptions.clearAllSubscriptions();


    }

    void processConnect(ServerChannel session, ConnectMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("processConnect for client " + msg.getClientID());
        }
        if (msg.getProcotolVersion() != 0x03) {
            log.warn("processConnect sent bad proto ConnAck");
            ConnAckMessage badProto = new ConnAckMessage();
            badProto.setReturnCode(ConnAckMessage.UNNACEPTABLE_PROTOCOL_VERSION);
            session.write(badProto);
            session.close(false);
            return;
        }

        if (msg.getClientID() == null || msg.getClientID().length() > 23) {
            ConnAckMessage okResp = new ConnAckMessage();
            okResp.setReturnCode(ConnAckMessage.IDENTIFIER_REJECTED);
            session.write(okResp);
            return;
        }

        //if an old client with the same ID already exists close its session.
        if (m_clientIDs.containsKey(msg.getClientID())) {
            //clean the subscriptions if the old used a cleanSession = true
            ServerChannel oldSession = m_clientIDs.get(msg.getClientID()).getSession();
            boolean cleanSession = (Boolean) oldSession.getAttribute(Constants.CLEAN_SESSION);
            if (cleanSession) {
                //cleanup topic subscriptions
                processRemoveAllSubscriptions(msg.getClientID());
            }

            m_clientIDs.get(msg.getClientID()).getSession().close(false);
        }

        ConnectionDescriptor connDescr = new ConnectionDescriptor(msg.getClientID(), session, msg.isCleanSession());
        m_clientIDs.put(msg.getClientID(), connDescr);

        int keepAlive = msg.getKeepAlive();
        if (log.isDebugEnabled()) {
            log.debug("Connect with keepAlive " + keepAlive);
        }
        session.setAttribute(Constants.KEEP_ALIVE, keepAlive);
        session.setAttribute(Constants.CLEAN_SESSION, msg.isCleanSession());
        //used to track the client in the subscription and publishing phases.
        session.setAttribute(Constants.ATTR_CLIENTID, msg.getClientID());

        session.setIdleTime(Math.round(keepAlive * 1.5f));

        //Handle will flag
        if (msg.isWillFlag()) {
            AbstractMessage.QOSType willQos = AbstractMessage.QOSType.values()[msg.getWillQos()];
            byte[] willPayload = msg.getWillMessage().getBytes();
            ByteBuffer bb = (ByteBuffer) ByteBuffer.allocate(willPayload.length).put(willPayload).flip();
            PublishEvent pubEvt = new PublishEvent(msg.getWillTopic(), willQos,
                    bb, msg.isWillRetain(), msg.getClientID(), session);
            processPublish(pubEvt);
        }

       
       //Server enforces user authentication but user doesn't supply credentials
       // NOTE: this is just a interim solution for a potential security threat.
       if ( isAuthenticationRequired && (! msg.isUserFlag())) {
           ConnAckMessage okResp = new ConnAckMessage();
           okResp.setReturnCode(ConnAckMessage.BAD_USERNAME_OR_PASSWORD);
           session.write(okResp);
           return;
       }

       //handle user authentication
        if (msg.isUserFlag()) {
            String pwd = null;
            if (msg.isPasswordFlag()) {
                pwd = msg.getPassword();
            }
            if (!m_authenticator.checkValid(msg.getUsername(), pwd)) {
                ConnAckMessage okResp = new ConnAckMessage();
                okResp.setReturnCode(ConnAckMessage.BAD_USERNAME_OR_PASSWORD);
                session.write(okResp);
                return;
            }
        }

        subscriptions.activate(msg.getClientID());

        //handle clean session flag
        if (msg.isCleanSession()) {
            //remove all prev subscriptions
            //cleanup topic subscriptions
            processRemoveAllSubscriptions(msg.getClientID());
        }

        ConnAckMessage okResp = new ConnAckMessage();
        okResp.setReturnCode(ConnAckMessage.CONNECTION_ACCEPTED);
        if (log.isDebugEnabled()) {
            log.debug("processConnect sent OK ConnAck for client " + msg.getClientID());
        }
        session.write(okResp);

        if (log.isDebugEnabled()) {
            log.debug("Connected client ID " + msg.getClientID() + " with clean session " + msg.isCleanSession());
        }

        if (!msg.isCleanSession()) {
            //force the republish of stored QoS1 and QoS2
            republishStored(msg.getClientID());
        }
    }

    private void republishStored(String clientID) {
        if (log.isTraceEnabled()) {
            log.trace("republishStored invoked for client " + clientID);
        }
        List<PublishEvent> publishedEvents = m_storageService.retrivePersistedPublishes(clientID);
        if (publishedEvents == null) {
            log.info("No stored messages for client " + clientID);
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("republishing stored messages to client " + clientID);
        }
        for (PublishEvent pubEvt : publishedEvents) {
            sendPublish(pubEvt.getClientID(), pubEvt.getTopic(), pubEvt.getQos(),
                    pubEvt.getMessage(), false, pubEvt.getMessageID());
        }
    }

    void processPubAck(String clientID, int messageID) {
        //Remove the message from message store
        //TODO removed the storage service
        // m_storageService.cleanPersistedPublishMessage(clientID, messageID);
        //Will inform the cluser that the message was removed
        try {
            AndesMQTTBridge.getBridgeInstance().onAckReceived(clientID, messageID);
        } catch (MQTTException e) {
            final String message = "Error while reciving ack from the client " + clientID + " for message " + messageID;
            log.error(message, e);
        }
    }

    private void processRemoveAllSubscriptions(String clientID) {
        log.info("cleaning old saved subscriptions for client " + clientID);
        subscriptions.removeForClient(clientID);
        //remove also the messages stored of type QoS1/2
        //TODO removed storage service
        // m_storageService.cleanPersistedPublishes(clientID);
    }

    protected void processPublish(PublishEvent evt) {
        if (log.isDebugEnabled()) {
            log.debug("processPublish invoked with " + evt);
        }
        final String topic = evt.getTopic();
        final AbstractMessage.QOSType qos = evt.getQos();
        final ByteBuffer message = evt.getMessage();
        boolean retain = evt.isRetain();
        //Added to maintain the state of the publishing clients
        String clientID = evt.getClientID();

        String publishKey;

        // For QOS 2 send publisher received
        if (qos == AbstractMessage.QOSType.EXACTLY_ONCE) {
            publishKey = String.format("%s%d", evt.getClientID(), evt.getMessageID());
            //store the message in temp store
            m_storageService.persistQoS2Message(publishKey, evt);
            sendPubRec(evt.getClientID(), evt.getMessageID());
        }

        AndesMQTTBridge.onMessagePublished(topic, qos.ordinal(), message, retain,
                evt.getMessageID(), clientID, this);

        if (retain) {
            //TODO call the cluster specifc store here
            // m_storageService.storeRetained(topic, message, qos);
        }
    }

    /**
     * Method written by WSO2. Since we would be sequentially writing the message to the subscribers
     *
     * @param topic       the name of the topic the message was published
     * @param qos         the level of qos the message was published this could be either 0,1 or 2
     * @param origMessage the content of the message
     * @param retain      should this message retain
     * @param messageID   the unique identifier of the message
     */
    public void publishToSubscriber(String topic, AbstractMessage.QOSType qos, ByteBuffer origMessage,
                                    boolean retain, Integer messageID, String mqttClientID) {
        Subscription subscription = subscriptions.getSubscriptions(topic, mqttClientID);


        if (qos.ordinal() > subscription.getRequestedQos().ordinal()) {
            qos = subscription.getRequestedQos();
        }

        ByteBuffer message = origMessage.duplicate();

        if (qos == AbstractMessage.QOSType.MOST_ONE && subscription.isActive()) {
            //QoS 0
            sendPublish(subscription.getClientId(), topic, qos, message, false);
        } else {
            //QoS 1 or 2
            //if the target subscription is not clean session and is not connected => store it
            if (!subscription.isCleanSession() && !subscription.isActive()) {
                //clone the event with matching clientID
                PublishEvent newPublishEvt = new PublishEvent(topic, qos, message, retain, subscription.getClientId(),
                        messageID, null);
                m_storageService.storePublishForFuture(newPublishEvt);
            } else {
                //if QoS 2 then store it in temp memory
                //TODO need to address the situation of the subscriber failing to receive the message in QOS 2
          /*      if (qos == AbstractMessage.QOSType.EXACTLY_ONCE) {
                    String publishKey = String.format("%s%d", subscription.getClientId(), messageID);
                    PublishEvent newPublishEvt = new PublishEvent(topic, qos, message, retain,
                            subscription.getClientId(), messageID, null);
                    m_storageService.addInFlight(newPublishEvt, publishKey);
                }*/
                //publish
                if (subscription.isActive()) {
                    //Change done by WSO2 will be overloading the method
                    sendPublish(subscription.getClientId(), topic, qos, message, false, messageID);
                }
            }
        }
    }

    /**
     * Flood the subscribers with the message to notify. MessageID is optional and should only used for QoS 1 and 2
     */
    public void publish2Subscribers(String topic, AbstractMessage.QOSType qos, ByteBuffer origMessage, boolean retain,
                                    Integer messageID) {
        if (log.isDebugEnabled()) {
            log.debug("publish2Subscribers republishing to existing subscribers that matches the topic " + topic);
            log.debug("content " + DebugUtils.payload2Str(origMessage));
            log.debug("subscription tree " + subscriptions.dumpTree());
        }
        for (final Subscription sub : subscriptions.matches(topic)) {
            if (qos.ordinal() > sub.getRequestedQos().ordinal()) {
                qos = sub.getRequestedQos();
            }

            ByteBuffer message = origMessage.duplicate();
     /*       log.debug("Broker republishing to client <{}> topic <{}> qos <{}>, active {}",
                    sub.getClientId(), sub.getTopic(), qos, sub.isActive());*/

            if (qos == AbstractMessage.QOSType.MOST_ONE && sub.isActive()) {
                //QoS 0
                sendPublish(sub.getClientId(), topic, qos, message, false);
            } else {
                //QoS 1 or 2
                //if the target subscription is not clean session and is not connected => store it
                if (!sub.isCleanSession() && !sub.isActive()) {
                    //clone the event with matching clientID
                    PublishEvent newPublishEvt = new PublishEvent(topic, qos, message, retain, sub.getClientId(),
                            messageID, null);
                    m_storageService.storePublishForFuture(newPublishEvt);
                } else {
                    //if QoS 2 then store it in temp memory
                    if (qos == AbstractMessage.QOSType.EXACTLY_ONCE) {
                        String publishKey = String.format("%s%d", sub.getClientId(), messageID);
                        PublishEvent newPublishEvt = new PublishEvent(topic, qos, message, retain, sub.getClientId(),
                                messageID, null);
                        m_storageService.addInFlight(newPublishEvt, publishKey);
                    }
                    //publish
                    if (sub.isActive()) {
                        sendPublish(sub.getClientId(), topic, qos, message, false);
                    }
                }
            }
        }
    }

    private void sendPublish(String clientId, String topic, AbstractMessage.QOSType qos, ByteBuffer message,
                             boolean retained) {
        //TODO pay attention to the message ID can't be 0 and it's the message sent to subscriber
        int messageID = 1;
        sendPublish(clientId, topic, qos, message, retained, messageID);
    }

    private void sendPublish(String clientId, String topic, AbstractMessage.QOSType qos, ByteBuffer message, boolean
            retained, int messageID) {
        /*log.debug("sendPublish invoked clientId <{}> on topic <{}> QoS {} ratained {} messageID {}", clientId, topic,
        qos, retained, messageID);*/
        PublishMessage pubMessage = new PublishMessage();
        pubMessage.setRetainFlag(retained);
        pubMessage.setTopicName(topic);
        pubMessage.setQos(qos);
        pubMessage.setPayload(message);

        if (log.isDebugEnabled()) {
            log.debug("send publish message to " + clientId + " on topic " + topic);
        }
        if (log.isTraceEnabled()) {
            log.trace("content " + DebugUtils.payload2Str(message));
        }
        if (pubMessage.getQos() != AbstractMessage.QOSType.MOST_ONE) {
            pubMessage.setMessageID(messageID);
        }

        if (m_clientIDs == null) {
            throw new RuntimeException("Internal bad error, found m_clientIDs to null while it should be initialized, " +
                    "somewhere it's overwritten!!");
        }
        if (log.isDebugEnabled()) {
            log.debug("clientIDs are " + m_clientIDs);
        }
        if (m_clientIDs.get(clientId) == null) {
            throw new RuntimeException(String.format("Can't find a ConnectionDescriptor for client <%s> in cache <%s>",
                    clientId, m_clientIDs));
        }
        if (log.isDebugEnabled()) {
            log.debug("Session for clientId" + clientId + "is " + m_clientIDs.get(clientId).getSession());
        }
//            m_clientIDs.get(clientId).getSession().write(pubMessage);
        disruptorPublish(new OutputMessagingEvent(m_clientIDs.get(clientId).getSession(), pubMessage));
    }

    private void sendPubRec(String clientID, int messageID) {
        if (log.isDebugEnabled()) {
            log.debug("SendPubRec invoked for clientID " + clientID + " with messageID " + messageID);
        }
        PubRecMessage pubRecMessage = new PubRecMessage();
        pubRecMessage.setMessageID(messageID);

//        m_clientIDs.get(clientID).getSession().write(pubRecMessage);
        disruptorPublish(new OutputMessagingEvent(m_clientIDs.get(clientID).getSession(), pubRecMessage));
    }

    private void sendPubAck(String clientId, int messageID) {
        if (log.isTraceEnabled()) {
            log.trace("sendPubAck invoked");
        }

        PubAckMessage pubAckMessage = new PubAckMessage();
        pubAckMessage.setMessageID(messageID);

        try {
            if (m_clientIDs == null) {
                throw new RuntimeException("Internal bad error, found m_clientIDs to null while it should be initialized," +
                        " somewhere it's overwritten!!");
            }
            if (log.isDebugEnabled()) {
                log.debug("clientIDs are " + m_clientIDs);
            }
            if (m_clientIDs.get(clientId) == null) {
                throw new RuntimeException(String.format("Can't find a ConnectionDEwcriptor for client %s " +
                        "in cache %s", clientId, m_clientIDs));
            }
//            log.debug("Session for clientId " + clientId + " is " + m_clientIDs.get(clientId).getSession());
//            m_clientIDs.get(clientId).getSession().write(pubAckMessage);
            disruptorPublish(new OutputMessagingEvent(m_clientIDs.get(clientId).getSession(), pubAckMessage));
        } catch (Throwable t) {
            log.error(null, t);
        }
    }

    /**
     * Second phase of a publish QoS2 protocol, sent by publisher to the broker. Search the stored message and publish
     * to all interested subscribers.
     */
    void processPubRel(String clientID, int messageID) {
        if (log.isDebugEnabled()) {
            log.debug("ProcessPubRel invoked for clientID " + clientID + "ad messageID " + messageID);
        }
        String publishKey = String.format("%s%d", clientID, messageID);
        PublishEvent evt = m_storageService.retrieveQoS2Message(publishKey);

        final String topic = evt.getTopic();
        final AbstractMessage.QOSType qos = evt.getQos();

        m_storageService.removeQoS2Message(publishKey);

        if (evt.isRetain()) {
            //TODO call the cluster specifc store here
            //m_storageService.storeRetained(topic, evt.getMessage(), qos);
        }
    }

    private void sendPubComp(String clientID, int messageID) {
        if (log.isDebugEnabled()) {
            log.debug("SendPubComp invoked for clientID " + clientID + " ad messageID " + messageID);
        }
        PubCompMessage pubCompMessage = new PubCompMessage();
        pubCompMessage.setMessageID(messageID);

//        m_clientIDs.get(clientID).getSession().write(pubCompMessage);
        disruptorPublish(new OutputMessagingEvent(m_clientIDs.get(clientID).getSession(), pubCompMessage));
    }

    void processPubRec(String clientID, int messageID) {
        //once received a PUBREC reply with a PUBREL(messageID)
        if (log.isDebugEnabled()) {
            log.debug("ProcessPubRec invoked for " + clientID + " ad messageID " + messageID);
        }
        PubRelMessage pubRelMessage = new PubRelMessage();
        pubRelMessage.setMessageID(messageID);
        pubRelMessage.setQos(AbstractMessage.QOSType.LEAST_ONE);

//        m_clientIDs.get(clientID).getSession().write(pubRelMessage);
        disruptorPublish(new OutputMessagingEvent(m_clientIDs.get(clientID).getSession(), pubRelMessage));
    }

    void processPubComp(String clientID, int messageID) {
        //For now let's stick with only the debug log to indicate the arrival of the message
        //However need to add the mechanism to recover if the subscriber fails to send the PUBCOMP
        if (log.isDebugEnabled()) {
            log.debug("ProcessPubComp invoked for clientID " + clientID + " ad messageID " + messageID);
        }
        //once received the PUBCOMP then remove the message from the temp memory
        //String publishKey = String.format("%s%d", clientID, messageID);
        //Commented since its usage is not applicable here
        //TODO need to define actions when the subscriber fails to send the PUBCOMP back to the server
      //  m_storageService.cleanInFlight(publishKey);

        // Send an acknowledgement to Andes stating that the message has been received from the client
        try {
            AndesMQTTBridge.getBridgeInstance().onAckReceived(clientID, messageID);
        } catch (MQTTException e) {
            log.error("Error while processing ack from the client " + clientID + " for message" + messageID, e);
        }
    }

    void processDisconnect(ServerChannel session, String clientID, boolean cleanSession) throws InterruptedException {
        if (cleanSession) {
            //cleanup topic subscriptions
            processRemoveAllSubscriptions(clientID);
        }
//        m_notifier.disconnect(evt.getSession());
        m_clientIDs.remove(clientID);
        session.close(true);

        //de-activate the subscriptions for this ClientID
        subscriptions.deactivate(clientID);

        try {
            AndesMQTTBridge.getBridgeInstance().onSubscriberDisconnection(clientID,
                    AndesMQTTBridge.SubscriptionEvent.DISCONNECT);
            log.info("Disconnected client " + clientID + " with clean session " + cleanSession);
        } catch (MQTTException e) {
            log.error("Error occurred when attempting to disconnect subscriber", e);
        }
    }

    void proccessConnectionLost(String clientID) {
        //If already removed a disconnect message was already processed for this clientID
        if (m_clientIDs.remove(clientID) != null) {

            //de-activate the subscriptions for this ClientID
            subscriptions.deactivate(clientID);
            log.info("Lost connection with client " + clientID);
            //Andes change
            //Need to handle disconnection
            try {
                AndesMQTTBridge.getBridgeInstance().onSubscriberDisconnection(clientID,
                        AndesMQTTBridge.SubscriptionEvent.DISCONNECT);
            } catch (MQTTException e) {
                final String message = "Error occured when attempting to diconnect subscriber ";
                log.error(message + e.getMessage(), e);
            }
            // bridge.onSubscriberDisconnection(clientID);
        }
    }

    /**
     * Remove the clientID from topic subscription, if not previously subscribed,
     * doesn't reply any error
     */
    void processUnsubscribe(ServerChannel session, String clientID, List<String> topics, int messageID) {
        if (log.isDebugEnabled()) {
            log.debug("processUnsubscribe invoked, removing subscription on topics " + topics + ", for clientID " + clientID);
        }

        for (String topic : topics) {
            subscriptions.removeSubscription(topic, clientID);
            //also will unsubscribe from the kernal
            try {
                AndesMQTTBridge.getBridgeInstance().onSubscriberDisconnection(clientID,
                        AndesMQTTBridge.SubscriptionEvent.UNSUBSCRIBE);
            } catch (Exception e) {
                final String message = "Error occured when disconneting the subscriber ";
                log.error(message + e.getMessage());
            }
            // bridge.onSubscriberDisconnection(clientID);
        }
        //ack the client
        UnsubAckMessage ackMessage = new UnsubAckMessage();
        ackMessage.setMessageID(messageID);

        log.info("replying with UnsubAck to MSG ID " + messageID);
        session.write(ackMessage);
    }


    void processSubscribe(ServerChannel session, SubscribeMessage msg, String clientID, boolean cleanSession) {
        if (log.isDebugEnabled()) {
            log.debug("processSubscribe invoked from client " + clientID + " with msgID " + msg.getMessageID());
        }

        for (SubscribeMessage.Couple req : msg.subscriptions()) {
            AbstractMessage.QOSType qos = AbstractMessage.QOSType.values()[req.getQos()];
            Subscription newSubscription = new Subscription(clientID, req.getTopic(), qos, cleanSession);
            subscribeSingleTopic(newSubscription, req.getTopic());
            //Will connect with the bridge to notify on the topic
            //Andes Specific
            try {
                AndesMQTTBridge.getBridgeInstance().onTopicSubscription(req.getTopic(), clientID, qos, cleanSession);
            } catch (Exception e) {
                final String message = "Error when registering the subscriber ";
                log.error(message + e.getMessage(), e);
                throw new RuntimeException(message, e);
            }
            // bridge.onTopicSubscription(req.getTopic(), clientID);
        }

        //ack the client
        SubAckMessage ackMessage = new SubAckMessage();
        ackMessage.setMessageID(msg.getMessageID());

        //reply with requested qos
        for (SubscribeMessage.Couple req : msg.subscriptions()) {
            AbstractMessage.QOSType qos = AbstractMessage.QOSType.values()[req.getQos()];
            ackMessage.addType(qos);
        }
        if (log.isDebugEnabled()) {
            log.debug("replying with SubAck to MSG ID " + msg.getMessageID());
        }
        session.write(ackMessage);
    }

    private void subscribeSingleTopic(Subscription newSubscription, final String topic) {
/*        log.info("<{}> subscribed to topic <{}> with QoS {}",
                newSubscription.getClientId(), topic, 
                AbstractMessage.QOSType.formatQoS(newSubscription.getRequestedQos()));*/
        subscriptions.add(newSubscription);

        //scans retained messages to be published to the new subscription
        Collection<HawtDBStorageService.StoredMessage> messages = m_storageService.searchMatching(new IMatchingCondition() {
            public boolean match(String key) {
                return SubscriptionsStore.matchTopics(key, topic);
            }
        });

        for (HawtDBStorageService.StoredMessage storedMsg : messages) {
            //fire the as retained the message
            if (log.isDebugEnabled()) {
                log.debug("send publish message for topic " + topic);
            }
            sendPublish(newSubscription.getClientId(), storedMsg.getTopic(), storedMsg.getQos(), storedMsg.getPayload(), true);
        }
    }

    private void disruptorPublish(OutputMessagingEvent msgEvent) {
        if (log.isDebugEnabled()) {
            log.debug("disruptorPublish publishing event on output " + msgEvent);
        }
        long sequence = m_ringBuffer.next();
        ValueEvent event = m_ringBuffer.get(sequence);

        event.setEvent(msgEvent);

        m_ringBuffer.publish(sequence);
    }

    public void onEvent(ValueEvent t, long l, boolean bln) throws Exception {
        MessagingEvent evt = t.getEvent();
        //It's always of type OutputMessagingEvent
        OutputMessagingEvent outEvent = (OutputMessagingEvent) evt;
        outEvent.getChannel().write(outEvent.getMessage());
    }

    @Override
    public void ack(AndesMessageMetadata metadata) {
        int qos = (Integer)metadata.getProperty(MQTTUtils.QOSLEVEL);
        String clientID = (String)metadata.getProperty(MQTTUtils.CLIENT_ID);
        int messageID = (Integer) metadata.getProperty(MQTTUtils.MESSAGE_ID);

        if(qos == AbstractMessage.QOSType.EXACTLY_ONCE.ordinal()) {
            sendPubComp(clientID, messageID);
        } else if (qos == AbstractMessage.QOSType.LEAST_ONE.ordinal()) {
            sendPubAck(clientID, messageID);
        }
    }

    @Override
    public void nack(AndesMessageMetadata metadata) {

    }
}
