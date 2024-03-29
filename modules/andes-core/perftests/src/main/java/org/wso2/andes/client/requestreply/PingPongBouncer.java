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
package org.wso2.andes.client.requestreply;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.client.AMQConnection;
import org.wso2.andes.client.AMQQueue;
import org.wso2.andes.client.AMQTopic;
import org.wso2.andes.client.topic.Config;
import org.wso2.andes.exchange.ExchangeDefaults;
import org.wso2.andes.jms.ConnectionListener;
import org.wso2.andes.jms.Session;

import javax.jms.*;
import java.io.IOException;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * PingPongBouncer is a message listener the bounces back messages to their reply to destination. This is used to return
 * ping messages generated by {@link org.wso2.andes.client.requestreply.PingPongProducer} but could be used for other purposes
 * too.
 *
 * <p/>The correlation id from the received message is extracted, and placed into the reply as the correlation id. Messages
 * are bounced back to their reply-to destination. The original sender of the message has the option to use either a unique
 * temporary queue or the correlation id to correlate the original message to the reply.
 *
 * <p/>There is a verbose mode flag which causes information about each ping to be output to the console
 * (info level logging, so usually console). This can be helpfull to check the bounce backs are happening but should
 * be disabled for real timing tests as writing to the console will slow things down.
 *
 * <p><table id="crc"><caption>CRC Card</caption>
 * <tr><th> Responsibilities <th> Collaborations
 * <tr><td> Bounce back messages to their reply to destination.
 * <tr><td> Provide command line invocation to start the bounce back on a configurable broker url.
 * </table>
 *
 * @todo Replace the command line parsing with a neater tool.
 *
 * @todo Make verbose accept a number of messages, only prints to console every X messages.
 */
public class PingPongBouncer implements MessageListener
{
    private static final Log _logger = LogFactory.getLog(PingPongBouncer.class);

    /** The default prefetch size for the message consumer. */
    private static final int PREFETCH = 1;

    /** The default no local flag for the message consumer. */
    private static final boolean NO_LOCAL = true;

    private static final String DEFAULT_DESTINATION_NAME = "ping";

    /** The default exclusive flag for the message consumer. */
    private static final boolean EXCLUSIVE = false;

    /** A convenient formatter to use when time stamping output. */
    protected static final SimpleDateFormat timestampFormatter = new SimpleDateFormat("hh:mm:ss:SS");

    /** Used to indicate that the reply generator should log timing info to the console (logger info level). */
    private boolean _verbose = false;

    /** Determines whether this bounce back client bounces back messages persistently. */
    private boolean _persistent = false;

    private Destination _consumerDestination;

    /** Keeps track of the response destination of the previous message for the last reply to producer cache. */
    private Destination _lastResponseDest;

    /** The producer for sending replies with. */
    private MessageProducer _replyProducer;

    /** The consumer controlSession. */
    private Session _consumerSession;

    /** The producer controlSession. */
    private Session _producerSession;

    /** Holds the connection to the broker. */
    private AMQConnection _connection;

    /** Flag used to indicate if this is a point to point or pub/sub ping client. */
    private boolean _isPubSub = false;

    /**
     * This flag is used to indicate that the user should be prompted to kill a broker, in order to test
     * failover, immediately before committing a transaction.
     */
    protected boolean _failBeforeCommit = false;

    /**
     * This flag is used to indicate that the user should be prompted to a kill a broker, in order to test
     * failover, immediate after committing a transaction.
     */
    protected boolean _failAfterCommit = false;

    /**
     * Creates a PingPongBouncer on the specified producer and consumer sessions.
     *
     * @param brokerDetails The addresses of the brokers to connect to.
     * @param username        The broker username.
     * @param password        The broker password.
     * @param virtualpath     The virtual host name within the broker.
     * @param destinationName The name of the queue to receive pings on
     *                        (or root of the queue name where many queues are generated).
     * @param persistent      A flag to indicate that persistent message should be used.
     * @param transacted      A flag to indicate that pings should be sent within transactions.
     * @param selector        A message selector to filter received pings with.
     * @param verbose         A flag to indicate that message timings should be sent to the console.
     *
     * @throws Exception All underlying exceptions allowed to fall through. This is only test code...
     */
    public PingPongBouncer(String brokerDetails, String username, String password, String virtualpath,
                           String destinationName, boolean persistent, boolean transacted, String selector, boolean verbose,
                           boolean pubsub) throws Exception
    {
        // Create a client id to uniquely identify this client.
        InetAddress address = InetAddress.getLocalHost();
        String clientId = address.getHostName() + System.currentTimeMillis();
        _verbose = verbose;
        _persistent = persistent;
        setPubSub(pubsub);
        // Connect to the broker.
        setConnection(new AMQConnection(brokerDetails, username, password, clientId, virtualpath));
        _logger.info("Connected with URL:" + getConnection().toURL());

        // Set up the failover notifier.
        getConnection().setConnectionListener(new FailoverNotifier());

        // Create a controlSession to listen for messages on and one to send replies on, transactional depending on the
        // command line option.
        _consumerSession = (Session) getConnection().createSession(transacted, Session.AUTO_ACKNOWLEDGE);
        _producerSession = (Session) getConnection().createSession(transacted, Session.AUTO_ACKNOWLEDGE);

        // Create the queue to listen for message on.
        createConsumerDestination(destinationName);
        MessageConsumer consumer =
            _consumerSession.createConsumer(_consumerDestination, PREFETCH, NO_LOCAL, EXCLUSIVE, selector);

        // Create a producer for the replies, without a default destination.
        _replyProducer = _producerSession.createProducer(null);
        _replyProducer.setDisableMessageTimestamp(true);
        _replyProducer.setDeliveryMode(_persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

        // Set this up to listen for messages on the queue.
        consumer.setMessageListener(this);
    }

    /**
     * Starts a stand alone ping-pong client running in verbose mode.
     *
     * @param args
     */
    public static void main(String[] args) throws Exception
    {
        System.out.println("Starting...");

        // Display help on the command line.
        if (args.length == 0)
        {
            _logger.info("Running test with default values...");
            //usage();
            //System.exit(0);
        }

        // Extract all command line parameters.
        Config config = new Config();
        config.setOptions(args);
        String brokerDetails = config.getHost() + ":" + config.getPort();
        String virtualpath = "test";
        String destinationName = config.getDestination();
        if (destinationName == null)
        {
            destinationName = DEFAULT_DESTINATION_NAME;
        }

        String selector = config.getSelector();
        boolean transacted = config.isTransacted();
        boolean persistent = config.usePersistentMessages();
        boolean pubsub = config.isPubSub();
        boolean verbose = true;

        //String selector = null;

        // Instantiate the ping pong client with the command line options and start it running.
        PingPongBouncer pingBouncer =
            new PingPongBouncer(brokerDetails, "guest", "guest", virtualpath, destinationName, persistent, transacted,
                                selector, verbose, pubsub);
        pingBouncer.getConnection().start();

        System.out.println("Waiting...");
    }

    private static void usage()
    {
        System.err.println("Usage: PingPongBouncer \n" + "-host : broker host\n" + "-port : broker port\n"
                           + "-destinationname : queue/topic name\n" + "-transacted : (true/false). Default is false\n"
                           + "-persistent : (true/false). Default is false\n"
                           + "-pubsub     : (true/false). Default is false\n" + "-selector   : selector string\n");
    }

    /**
     * This is a callback method that is notified of all messages for which this has been registered as a message
     * listener on a message consumer. It sends a reply (pong) to all messages it receieves on the reply to
     * destination of the message.
     *
     * @param message The message that triggered this callback.
     */
    public void onMessage(Message message)
    {
        try
        {
            String messageCorrelationId = message.getJMSCorrelationID();
            if (_verbose)
            {
                _logger.info(timestampFormatter.format(new Date()) + ": Got ping with correlation id, "
                             + messageCorrelationId);
            }

            // Get the reply to destination from the message and check it is set.
            Destination responseDest = message.getJMSReplyTo();

            if (responseDest == null)
            {
                _logger.debug("Cannot send reply because reply-to destination is null.");

                return;
            }

            // Spew out some timing information if verbose mode is on.
            if (_verbose)
            {
                Long timestamp = message.getLongProperty("timestamp");

                if (timestamp != null)
                {
                    long diff = System.currentTimeMillis() - timestamp;
                    _logger.info("Time to bounce point: " + diff);
                }
            }

            // Correlate the reply to the original.
            message.setJMSCorrelationID(messageCorrelationId);

            // Send the receieved message as the pong reply.
            _replyProducer.send(responseDest, message);

            if (_verbose)
            {
                _logger.info(timestampFormatter.format(new Date()) + ": Sent reply with correlation id, "
                             + messageCorrelationId);
            }

            // Commit the transaction if running in transactional mode.
            commitTx(_producerSession);
        }
        catch (JMSException e)
        {
            _logger.debug("There was a JMSException: " + e.getMessage(), e);
        }
    }

    /**
     * Gets the underlying connection that this ping client is running on.
     *
     * @return The underlying connection that this ping client is running on.
     */
    public AMQConnection getConnection()
    {
        return _connection;
    }

    /**
     * Sets the connection that this ping client is using.
     *
     * @param connection The ping connection.
     */
    public void setConnection(AMQConnection connection)
    {
        this._connection = connection;
    }

    /**
     * Sets or clears the pub/sub flag to indiciate whether this client is pinging a queue or a topic.
     *
     * @param pubsub <tt>true</tt> if this client is pinging a topic, <tt>false</tt> if it is pinging a queue.
     */
    public void setPubSub(boolean pubsub)
    {
        _isPubSub = pubsub;
    }

    /**
     * Checks whether this client is a p2p or pub/sub ping client.
     *
     * @return <tt>true</tt> if this client is pinging a topic, <tt>false</tt> if it is pinging a queue.
     */
    public boolean isPubSub()
    {
        return _isPubSub;
    }

    /**
     * Convenience method to commit the transaction on the specified controlSession. If the controlSession to commit on is not
     * a transactional controlSession, this method does nothing.
     *
     * <p/>If the {@link #_failBeforeCommit} flag is set, this will prompt the user to kill the broker before the
     * commit is applied. If the {@link #_failAfterCommit} flag is set, this will prompt the user to kill the broker
     * after the commit is applied.
     *
     * @throws javax.jms.JMSException If the commit fails and then the rollback fails.
     */
    protected void commitTx(Session session) throws JMSException
    {
        if (session.getTransacted())
        {
            try
            {
                if (_failBeforeCommit)
                {
                    _logger.debug("Failing Before Commit");
                    doFailover();
                }

                session.commit();

                if (_failAfterCommit)
                {
                    _logger.debug("Failing After Commit");
                    doFailover();
                }

                _logger.debug("Session Commited.");
            }
            catch (JMSException e)
            {
                _logger.trace("JMSException on commit:" + e.getMessage(), e);

                try
                {
                    session.rollback();
                    _logger.debug("Message rolled back.");
                }
                catch (JMSException jmse)
                {
                    _logger.trace("JMSE on rollback:" + jmse.getMessage(), jmse);

                    // Both commit and rollback failed. Throw the rollback exception.
                    throw jmse;
                }
            }
        }
    }

    /**
     * Prompts the user to terminate the named broker, in order to test failover functionality. This method will block
     * until the user supplied some input on the terminal.
     *
     * @param broker The name of the broker to terminate.
     */
    protected void doFailover(String broker)
    {
        System.out.println("Kill Broker " + broker + " now.");
        try
        {
            System.in.read();
        }
        catch (IOException e)
        { }

        System.out.println("Continuing.");
    }

    /**
     * Prompts the user to terminate the broker, in order to test failover functionality. This method will block
     * until the user supplied some input on the terminal.
     */
    protected void doFailover()
    {
        System.out.println("Kill Broker now.");
        try
        {
            System.in.read();
        }
        catch (IOException e)
        { }

        System.out.println("Continuing.");

    }

    private void createConsumerDestination(String name)
    {
        if (isPubSub())
        {
            _consumerDestination = new AMQTopic(ExchangeDefaults.TOPIC_EXCHANGE_NAME, name);
        }
        else
        {
            _consumerDestination = new AMQQueue(ExchangeDefaults.DIRECT_EXCHANGE_NAME, name);
        }
    }

    /**
     * A connection listener that logs out any failover complete events. Could do more interesting things with this
     * at some point...
     */
    public static class FailoverNotifier implements ConnectionListener
    {
        public void bytesSent(long count)
        { }

        public void bytesReceived(long count)
        { }

        public boolean preFailover(boolean redirect)
        {
            return true;
        }

        public boolean preResubscribe()
        {
            return true;
        }

        public void failoverComplete()
        {
            _logger.info("App got failover complete callback.");
        }
    }
}
