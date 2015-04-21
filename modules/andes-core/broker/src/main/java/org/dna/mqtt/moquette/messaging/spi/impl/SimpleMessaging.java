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
import org.dna.mqtt.moquette.messaging.spi.IMessaging;
import org.dna.mqtt.moquette.messaging.spi.IStorageService;
import org.dna.mqtt.moquette.messaging.spi.impl.events.*;
import org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.SubscriptionsStore;
import org.dna.mqtt.moquette.proto.messages.*;
import org.dna.mqtt.moquette.server.Constants;
import org.dna.mqtt.moquette.server.IAuthenticator;
import org.dna.mqtt.moquette.server.ServerChannel;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class SimpleMessaging implements IMessaging, EventHandler<ValueEvent> {

    private static Log log = LogFactory.getLog(SimpleMessaging.class);

    private SubscriptionsStore subscriptions;

    private RingBuffer<ValueEvent> m_ringBuffer;

    private IStorageService m_storageService;

    /**
     * Disruptor for inbound ValueEvent handling 
     */
    private Disruptor<ValueEvent> disruptor;
    
    private static SimpleMessaging INSTANCE;

    private ProtocolProcessor m_processor = new ProtocolProcessor();

    CountDownLatch m_stopLatch;

    private SimpleMessaging() {
    }

    public static SimpleMessaging getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new SimpleMessaging();
        }
        return INSTANCE;
    }

    public void init(Properties configProps) {
        subscriptions = new SubscriptionsStore();
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("Disruptor MQTT Simple Messaging Thread %d").build();
        ExecutorService executor = Executors.newCachedThreadPool(namedThreadFactory);
        Integer ringBufferSize = AndesConfigurationManager.readValue(
                AndesConfiguration.TRANSPORTS_MQTT_INBOUND_BUFFER_SIZE);

        disruptor = new Disruptor<ValueEvent>( ValueEvent.EVENT_FACTORY, ringBufferSize, executor);
        
        disruptor.handleExceptionsWith(new IgnoreExceptionHandler());
        SequenceBarrier barrier = disruptor.getRingBuffer().newBarrier();
        BatchEventProcessor<ValueEvent> eventProcessor = new BatchEventProcessor<ValueEvent>(
                disruptor.getRingBuffer(), barrier, this);
        
        disruptor.handleEventsWith(eventProcessor);
        m_ringBuffer = disruptor.start();
        
        disruptorPublish(new InitEvent(configProps));
    }


    private void disruptorPublish(MessagingEvent msgEvent) {
        if (log.isDebugEnabled()) {
            log.debug("disruptorPublish publishing event " + msgEvent);
        }
        long sequence = m_ringBuffer.next();
        ValueEvent event = m_ringBuffer.get(sequence);

        event.setEvent(msgEvent);

        m_ringBuffer.publish(sequence);
    }


    public void disconnect(ServerChannel session) {
        disruptorPublish(new DisconnectEvent(session));
    }

    public void lostConnection(String clientID) {
        disruptorPublish(new LostConnectionEvent(clientID));
    }

    public void handleProtocolMessage(ServerChannel session, AbstractMessage msg) {
        disruptorPublish(new ProtocolEvent(session, msg));
    }

    public void stop() {
        m_stopLatch = new CountDownLatch(1);
        disruptorPublish(new StopEvent());
        try {
            //wait the callback notification from the protocol processor thread
            boolean elapsed = !m_stopLatch.await(10, TimeUnit.SECONDS);
            if (elapsed) {
                log.warn("Can't stop the server in 10 seconds");
            }
        } catch (InterruptedException ex) {
            log.error(null, ex);
        }
    }

    public void onEvent(ValueEvent t, long l, boolean bln) throws Exception {
        MessagingEvent evt = t.getEvent();
        if (log.isDebugEnabled()) {
            log.debug("onEvent processing messaging event from input ringbuffer " + evt);
        }
        if (evt instanceof PublishEvent) {
            m_processor.processPublish((PublishEvent) evt);
        } else if (evt instanceof StopEvent) {
            processStop();
        } else if (evt instanceof DisconnectEvent) {
            DisconnectEvent disEvt = (DisconnectEvent) evt;
            String clientID = (String) disEvt.getSession().getAttribute(Constants.ATTR_CLIENTID);
            m_processor.processDisconnect(disEvt.getSession(), clientID, false);
        } else if (evt instanceof ProtocolEvent) {
            ServerChannel session = ((ProtocolEvent) evt).getSession();
            AbstractMessage message = ((ProtocolEvent) evt).getMessage();
            if (message instanceof ConnectMessage) {
                m_processor.processConnect(session, (ConnectMessage) message);
            } else if (message instanceof PublishMessage) {
                PublishEvent pubEvt;
                String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
                pubEvt = new PublishEvent((PublishMessage) message, clientID, session);
//                if (message.getQos() == QOSType.MOST_ONE) {
//                    pubEvt = new PublishEvent(pubMsg.getTopicName(), pubMsg.getQos(), pubMsg.getPayload(), pubMsg.isRetainFlag(), clientID, session);
//
//                } else {
//                    pubEvt = new PublishEvent(pubMsg.getTopicName(), pubMsg.getQos(), pubMsg.getPayload(), pubMsg.isRetainFlag(), clientID, pubMsg.getMessageID(), session);
//                }
                m_processor.processPublish(pubEvt);
            } else if (message instanceof DisconnectMessage) {
                String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
                boolean cleanSession = (Boolean) session.getAttribute(Constants.CLEAN_SESSION);

                //close the TCP connection
                //session.close(true);
                m_processor.processDisconnect(session, clientID, cleanSession);
            } else if (message instanceof UnsubscribeMessage) {
                UnsubscribeMessage unsubMsg = (UnsubscribeMessage) message;
                String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
                m_processor.processUnsubscribe(session, clientID, unsubMsg.topics(), unsubMsg.getMessageID());
            } else if (message instanceof SubscribeMessage) {
                String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
                boolean cleanSession = (Boolean) session.getAttribute(Constants.CLEAN_SESSION);
                m_processor.processSubscribe(session, (SubscribeMessage) message, clientID, cleanSession);
            } else if (message instanceof PubRelMessage) {
                String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
                int messageID = ((PubRelMessage) message).getMessageID();
                m_processor.processPubRel(clientID, messageID);
            } else if (message instanceof PubRecMessage) {
                String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
                int messageID = ((PubRecMessage) message).getMessageID();
                m_processor.processPubRec(clientID, messageID);
            } else if (message instanceof PubCompMessage) {
                String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
                int messageID = ((PubCompMessage) message).getMessageID();
                m_processor.processPubComp(clientID, messageID);
            } else if (message instanceof PubAckMessage) {
                String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
                int messageID = ((PubAckMessage) message).getMessageID();
                m_processor.processPubAck(clientID, messageID);
            } else {
                throw new RuntimeException("Illegal message received " + message);
            }

        } else if (evt instanceof InitEvent) {
            processInit(((InitEvent) evt).getConfig());
        } else if (evt instanceof LostConnectionEvent) {
            LostConnectionEvent lostEvt = (LostConnectionEvent) evt;
            m_processor.proccessConnectionLost(lostEvt.getClientID());
        }
    }

    private void processInit(Properties props) {
        m_storageService = new HawtDBStorageService();
        m_storageService.initStore();
      /*  m_storageService = new MemoryStorageService();
        m_storageService.initStore();*/

        subscriptions.init(m_storageService);

        String authenticatorClassName = AndesConfigurationManager.readValue(AndesConfiguration.TRANSPORTS_MQTT_USER_AUTHENTICATOR_CLASS);
        
        try {
            Class<? extends IAuthenticator> authenticatorClass = Class.forName(authenticatorClassName).asSubclass(IAuthenticator.class);
            IAuthenticator authenticator = authenticatorClass.newInstance();
            m_processor.init(subscriptions, m_storageService, authenticator);
                   
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("unable to find the class authenticator: " +  authenticatorClassName, e);
        } catch (InstantiationException e) {
            throw new RuntimeException("unable to create an instance of :" + authenticatorClassName,e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("unable to create an instance of :", e);
        }
        

    }


    private void processStop() {
        if (log.isDebugEnabled()) {
            log.debug("processStop invoked");
        }
        m_storageService.close();

//        m_eventProcessor.halt();
        disruptor.shutdown();

        subscriptions = null;
        m_stopLatch.countDown();
    }
}
