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
package org.wso2.andes.client.ping;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.wso2.andes.client.AMQSession;
import org.wso2.andes.client.requestreply.PingPongProducer;

import org.wso2.andes.junit.extensions.TimingController;
import org.wso2.andes.junit.extensions.TimingControllerAware;
import org.wso2.andes.junit.extensions.util.ParsedProperties;

import javax.jms.JMSException;
import javax.jms.Message;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PingLatencyTestPerf is a performance test that outputs multiple timings from its test method, using the timing
 * controller interface supplied by the test runner from a seperate listener thread. It outputs round trip timings for
 * individual ping messages rather than for how long a complete batch of messages took to process. It also differs from
 * the {@link PingTestPerf} test that it extends because it can output timings as replies are received, rather than
 * waiting until all expected replies are received.
 *
 * <p/>This test does not output timings for every single ping message, as when running at high volume, writing the test
 * log for a vast number of messages would slow the testing down. Instead samples ping latency occasionally. The
 * frequency of ping sampling is set using the {@link #TEST_RESULTS_BATCH_SIZE_PROPNAME} property, to override the
 * default of every {@link #DEFAULT_TEST_RESULTS_BATCH_SIZE}.
 *
 * <p/>The size parameter logged for each individual ping is set to the size of the batch of messages that the
 * individual timed ping was taken from, rather than 1 for a single message. This is so that the total throughput
 * (messages / time) can be calculated in order to examine the relationship between throughput and latency.
 *
 * <p/><table id="crc"><caption>CRC Card</caption> <tr><td> Responsibilities <th> Collaborations <tr><td> Send many ping
 * messages and output timings for sampled individual pings. </table>
 */
public class PingLatencyTestPerf extends PingTestPerf implements TimingControllerAware
{
    private static Log _logger = LogFactory.getLog(PingLatencyTestPerf.class);

    /** Holds the name of the property to get the test results logging batch size. */
    public static final String TEST_RESULTS_BATCH_SIZE_PROPNAME = "batchSize";

    /** Holds the default test results logging batch size. */
    public static final int DEFAULT_TEST_RESULTS_BATCH_SIZE = 1000;

    /** Used to hold the timing controller passed from the test runner. */
    private TimingController _timingController;

    /** Used to generate unique correlation ids for each test run. */
    private AtomicLong corellationIdGenerator = new AtomicLong();

    /**
     * Holds test specifics by correlation id. This consists of the expected number of messages and the timing
     * controler.
     */
    private Map<String, PerCorrelationId> perCorrelationIds =
        Collections.synchronizedMap(new HashMap<String, PerCorrelationId>());

    /** Holds the batched results listener, that does logging on batch boundaries. */
    private BatchedResultsListener batchedResultsListener = null;

    /**
     * Creates a new asynchronous ping performance test with the specified name.
     *
     * @param name The test name.
     */
    public PingLatencyTestPerf(String name)
    {
        super(name);

        // Sets up the test parameters with defaults.
        ParsedProperties.setSysPropertyIfNull(TEST_RESULTS_BATCH_SIZE_PROPNAME,
            Integer.toString(DEFAULT_TEST_RESULTS_BATCH_SIZE));
    }

    /** Compile all the tests into a test suite. */
    public static Test suite()
    {
        // Build a new test suite
        TestSuite suite = new TestSuite("Ping Latency Tests");

        // Run performance tests in read committed mode.
        suite.addTest(new PingLatencyTestPerf("testPingLatency"));

        return suite;
    }

    /**
     * Accepts a timing controller from the test runner.
     *
     * @param timingController The timing controller to register mutliple timings with.
     */
    public void setTimingController(TimingController timingController)
    {
        _timingController = timingController;
    }

    /**
     * Gets the timing controller passed in by the test runner.
     *
     * @return The timing controller passed in by the test runner.
     */
    public TimingController getTimingController()
    {
        return _timingController;
    }

    /**
     * Sends the specified number of pings, asynchronously outputs timings on every batch boundary, and waits until all
     * replies have been received or a time out occurs before exiting this method.
     *
     * @param numPings The number of pings to send.
     */
    public void testPingLatency(int numPings) throws Exception
    {
        _logger.debug("public void testPingLatency(int numPings): called");

        // Ensure that at least one ping was requeusted.
        if (numPings == 0)
        {
            _logger.error("Number of pings requested was zero.");
        }

        // Get the per thread test setup to run the test through.
        PerThreadSetup perThreadSetup = threadSetup.get();
        PingClient pingClient = perThreadSetup._pingClient;

        // Advance the correlation id of messages to send, to make it unique for this run.
        String messageCorrelationId = Long.toString(corellationIdGenerator.incrementAndGet());
        _logger.debug("messageCorrelationId = " + messageCorrelationId);

        // Initialize the count and timing controller for the new correlation id.
        PerCorrelationId perCorrelationId = new PerCorrelationId();
        TimingController tc = getTimingController().getControllerForCurrentThread();
        perCorrelationId._tc = tc;
        perCorrelationId._expectedCount = numPings;
        perCorrelationIds.put(messageCorrelationId, perCorrelationId);

        // Attach the chained message listener to the ping producer to listen asynchronously for the replies to these
        // messages.
        pingClient.setChainedMessageListener(batchedResultsListener);

        // Generate a sample message of the specified size.
        Message msg =
            pingClient.getTestMessage(perThreadSetup._pingClient.getReplyDestinations().get(0),
                testParameters.getPropertyAsInteger(PingPongProducer.MESSAGE_SIZE_PROPNAME),
                testParameters.getPropertyAsBoolean(PingPongProducer.PERSISTENT_MODE_PROPNAME));

        // Send the requested number of messages, and wait until they have all been received.
        long timeout = Long.parseLong(testParameters.getProperty(PingPongProducer.TIMEOUT_PROPNAME));
        int numReplies = pingClient.pingAndWaitForReply(msg, numPings, timeout, null);

        // Check that all the replies were received and log a fail if they were not.
        if (numReplies < numPings)
        {
            tc.completeTest(false, 0);
        }

        // Remove the chained message listener from the ping producer.
        pingClient.removeChainedMessageListener();

        // Remove the expected count and timing controller for the message correlation id, to ensure they are cleaned up.
        perCorrelationIds.remove(messageCorrelationId);
    }

    /** Performs test fixture creation on a per thread basis. This will only be called once for each test thread. */
    public void threadSetUp()
    {
        _logger.debug("public void threadSetUp(): called");

        try
        {
            // Call the set up method in the super class. This creates a PingClient pinger.
            super.threadSetUp();

            // Create the chained message listener, only if it has not already been created.  This is set up with the
            // batch size property, to tell it what batch size to output results on. A synchronized block is used to
            // ensure that only one thread creates this.
            synchronized (this)
            {
                if (batchedResultsListener == null)
                {
                    int batchSize = Integer.parseInt(testParameters.getProperty(TEST_RESULTS_BATCH_SIZE_PROPNAME));
                    batchedResultsListener = new BatchedResultsListener(batchSize);
                }
            }

            // Get the set up that the super class created.
            PerThreadSetup perThreadSetup = threadSetup.get();

            // Register the chained message listener on the pinger to do its asynchronous test timings from.
            perThreadSetup._pingClient.setChainedMessageListener(batchedResultsListener);
        }
        catch (Exception e)
        {
            _logger.warn("There was an exception during per thread setup.", e);
        }
    }

    /**
     * BatchedResultsListener is a {@link org.wso2.andes.client.requestreply.PingPongProducer.ChainedMessageListener} that can
     * be attached to the pinger, in order to receive notifications about every message received and the number
     * remaining to be received. Whenever the number remaining crosses a batch size boundary this results listener
     * outputs a test timing for the actual number of messages received in the current batch.
     */
    private class BatchedResultsListener implements PingPongProducer.ChainedMessageListener
    {
        /** The test results logging batch size. */
        int _batchSize;
        private boolean _strictAMQP;

        /**
         * Creates a results listener on the specified batch size.
         *
         * @param batchSize The batch size to use.
         */
        public BatchedResultsListener(int batchSize)
        {
            _batchSize = batchSize;
            _strictAMQP =
                Boolean.parseBoolean(System.getProperties().getProperty(AMQSession.STRICT_AMQP,
                        AMQSession.STRICT_AMQP_DEFAULT));
        }

        /**
         * This callback method is called from all of the pingers that this test creates. It uses the correlation id
         * from the message to identify the timing controller for the test thread that was responsible for sending those
         * messages.
         *
         * @param message        The message.
         * @param remainingCount The count of messages remaining to be received with a particular correlation id.
         *
         * @throws javax.jms.JMSException Any underlying JMSException is allowed to fall through.
         */
        public void onMessage(Message message, int remainingCount, long latency) throws JMSException
        {
            _logger.debug("public void onMessage(Message message, int remainingCount = " + remainingCount + "): called");

            // Check if a batch boundary has been crossed.
            if ((remainingCount % _batchSize) == 0)
            {
                // Extract the correlation id from the message.
                String correlationId = message.getJMSCorrelationID();

                // Get the details for the correlation id and check that they are not null. They can become null
                // if a test times out.
                PerCorrelationId perCorrelationId = perCorrelationIds.get(correlationId);
                if (perCorrelationId != null)
                {
                    // Get the timing controller and expected count for this correlation id.
                    TimingController tc = perCorrelationId._tc;
                    int expected = perCorrelationId._expectedCount;

                    // Calculate how many messages were actually received in the last batch. This will be the batch size
                    // except where the number expected is not a multiple of the batch size and this is the first remaining
                    // count to cross a batch size boundary, in which case it will be the number expected modulo the batch
                    // size.
                    int receivedInBatch = ((expected - remainingCount) < _batchSize) ? (expected % _batchSize) : _batchSize;

                    // Register a test result for the correlation id.
                    try
                    {
                        tc.completeTest(true, receivedInBatch, latency);
                    }
                    catch (InterruptedException e)
                    {
                        // Ignore this. It means the test runner wants to stop as soon as possible.
                        _logger.warn("Got InterruptedException.", e);
                    }
                }
                // Else ignore, test timed out. Should log a fail here?
            }
        }
    }

    /**
     * Holds state specific to each correlation id, needed to output test results. This consists of the count of the
     * total expected number of messages, and the timing controller for the thread sending those message ids.
     */
    private static class PerCorrelationId
    {
        public int _expectedCount;
        public TimingController _tc;
    }
}
