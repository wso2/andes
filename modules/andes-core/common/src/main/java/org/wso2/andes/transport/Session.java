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
package org.wso2.andes.transport;


import static org.wso2.andes.transport.Option.COMPLETED;
import static org.wso2.andes.transport.Option.SYNC;
import static org.wso2.andes.transport.Option.TIMELY_REPLY;
import static org.wso2.andes.transport.Session.State.CLOSED;
import static org.wso2.andes.transport.Session.State.CLOSING;
import static org.wso2.andes.transport.Session.State.DETACHED;
import static org.wso2.andes.transport.Session.State.NEW;
import static org.wso2.andes.transport.Session.State.OPEN;
import static org.wso2.andes.transport.Session.State.RESUMING;
import org.wso2.andes.transport.network.Frame;
import static org.wso2.andes.transport.util.Functions.mod;
import org.wso2.andes.transport.util.Logger;
import org.wso2.andes.transport.util.Waiter;
import static org.wso2.andes.util.Serial.ge;
import static org.wso2.andes.util.Serial.gt;
import static org.wso2.andes.util.Serial.le;
import static org.wso2.andes.util.Serial.lt;
import static org.wso2.andes.util.Serial.max;
import static org.wso2.andes.util.Strings.toUTF8;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Session
 *
 * @author Rafael H. Schloming
 */

public class Session extends SessionInvoker
{

    private static final Logger log = Logger.get(Session.class);

    public enum State { NEW, DETACHED, RESUMING, OPEN, CLOSING, CLOSED }

    static class DefaultSessionListener implements SessionListener
    {

        public void opened(Session ssn) {}

        public void resumed(Session ssn) {}

        public void message(Session ssn, MessageTransfer xfr)
        {
            log.info("message: %s", xfr);
        }

        public void exception(Session ssn, SessionException exc)
        {
            log.error(exc, "session exception");
        }

        public void closed(Session ssn) {}
    }

    public static final int UNLIMITED_CREDIT = 0xFFFFFFFF;

    private Connection connection;
    private Binary name;
    private long expiry;
    private boolean closing;
    private int channel;
    private SessionDelegate delegate;
    private SessionListener listener = new DefaultSessionListener();
    private long timeout = 60000;
    private boolean autoSync = false;

    private boolean incomingInit;
    // incoming command count
    private int commandsIn;
    // completed incoming commands
    private final Object processedLock = new Object();
    private RangeSet processed;
    private int maxProcessed;
    private int syncPoint;

    // outgoing command count
    private int commandsOut = 0;
    private Method[] commands = new Method[Integer.getInteger("qpid.session.command_limit", 64*1024)];
    private int commandBytes = 0;
    private int byteLimit = Integer.getInteger("qpid.session.byte_limit", 1024*1024);
    private int maxComplete = commandsOut - 1;
    private boolean needSync = false;

    private State state = NEW;

    // transfer flow control
    private volatile boolean flowControl = false;
    private Semaphore credit = new Semaphore(0);

    private Thread resumer = null;
    private boolean transacted = false;
    private SessionDetachCode detachCode;
    private final Object stateLock = new Object();

    protected Session(Connection connection, Binary name, long expiry)
    {
        this(connection, new SessionDelegate(), name, expiry);
    }

    protected Session(Connection connection, SessionDelegate delegate, Binary name, long expiry)
    {
        this.connection = connection;
        this.delegate = delegate;
        this.name = name;
        this.expiry = expiry;
        this.closing = false;
        initReceiver();
    }

    public Connection getConnection()
    {
        return connection;
    }

    public Binary getName()
    {
        return name;
    }

    void setExpiry(long expiry)
    {
        this.expiry = expiry;
    }

    void setClose(boolean close)
    {
        this.closing = close;
    }

    public int getChannel()
    {
        return channel;
    }

    void setChannel(int channel)
    {
        this.channel = channel;
    }

    public void setSessionListener(SessionListener listener)
    {
        if (listener == null)
        {
            this.listener = new DefaultSessionListener();
        }
        else
        {
            this.listener = listener;
        }
    }

    public SessionListener getSessionListener()
    {
        return listener;
    }

    public void setAutoSync(boolean value)
    {
        synchronized (commands)
        {
            this.autoSync = value;
        }
    }

    protected void setState(State state)
    {
        synchronized (commands)
        {
            this.state = state;
            commands.notifyAll();
        }
    }

    void setFlowControl(boolean value)
    {
        flowControl = value;
    }

    void addCredit(int value)
    {
        credit.release(value);
    }

    void drainCredit()
    {
        credit.drainPermits();
    }

    void acquireCredit()
    {
        if (flowControl)
        {
            try
            {
                if (!credit.tryAcquire(timeout, TimeUnit.MILLISECONDS))
                {
                    throw new SessionException
                        ("timed out waiting for message credit");
                }
            }
            catch (InterruptedException e)
            {
                throw new SessionException
                    ("interrupted while waiting for credit", null, e);
            }
        }
    }

    private void initReceiver()
    {
        synchronized (processedLock)
        {
            incomingInit = false;
            processed = new RangeSet();
        }
    }

    void attach()
    {
        initReceiver();
        sessionAttach(name.getBytes());
        sessionRequestTimeout(0);//use expiry here only if/when session resume is supported
    }

    void resume()
    {
        synchronized (commands)
        {
            for (int i = maxComplete + 1; lt(i, commandsOut); i++)
            {
                Method m = commands[mod(i, commands.length)];
                if (m == null)
                {
                    m = new ExecutionSync();
                    m.setId(i);
                }
                else if (m instanceof MessageTransfer)
                {
                	MessageTransfer xfr = (MessageTransfer)m;
                	
                	if (xfr.getHeader() != null)
                	{
                		if (xfr.getHeader().get(DeliveryProperties.class) != null)
                		{
                		   xfr.getHeader().get(DeliveryProperties.class).setRedelivered(true);
                		}
                		else
                		{
                			Struct[] structs = xfr.getHeader().getStructs();
                			DeliveryProperties deliveryProps = new DeliveryProperties();
                    		deliveryProps.setRedelivered(true);
                    		
                    		List<Struct> list = Arrays.asList(structs);
                    		list.add(deliveryProps);
                    		xfr.setHeader(new Header(list));
                		}
                		
                	}
                	else
                	{
                		DeliveryProperties deliveryProps = new DeliveryProperties();
                		deliveryProps.setRedelivered(true);
                		xfr.setHeader(new Header(deliveryProps));
                	}
                }
                sessionCommandPoint(m.getId(), 0);
                send(m);
            }
           
            sessionCommandPoint(commandsOut, 0);
            sessionFlush(COMPLETED);
            resumer = Thread.currentThread();
            state = RESUMING;
            listener.resumed(this);
            resumer = null;
        }
    }

    void dump()
    {
        synchronized (commands)
        {
            for (Method m : commands)
            {
                if (m != null)
                {
                    log.debug("%s", m);
                }
            }
        }
    }

    final void commandPoint(int id)
    {
        synchronized (processedLock)
        {
            this.commandsIn = id;
            if (!incomingInit)
            {
                incomingInit = true;
                maxProcessed = commandsIn - 1;
                syncPoint = maxProcessed;
            }
        }
    }

    public int getCommandsOut()
    {
        return commandsOut;
    }

    public int getCommandsIn()
    {
        return commandsIn;
    }

    public int nextCommandId()
    {
        return commandsIn++;
    }

    final void identify(Method cmd)
    {
        if (!incomingInit)
        {
            throw new IllegalStateException();
        }

        int id = nextCommandId();
        cmd.setId(id);

        if(log.isDebugEnabled())
        {
            log.debug("ID: [%s] %s", this.channel, id);
        }

        //if ((id % 65536) == 0)
        if ((id & 0xff) == 0)
        {
            flushProcessed(TIMELY_REPLY);
        }
    }

    public void processed(Method command)
    {
        processed(command.getId());
    }

    public void processed(int command)
    {
        processed(new Range(command, command));
    }

    public void processed(int lower, int upper)
    {

        processed(new Range(lower, upper));
    }

    public void processed(Range range)
    {
        log.debug("%s processed(%s) %s %s", this, range, syncPoint, maxProcessed);

        boolean flush;
        synchronized (processedLock)
        {
            log.debug("%s", processed);

            if (ge(range.getUpper(), commandsIn))
            {
                throw new IllegalArgumentException
                    ("range exceeds max received command-id: " + range);
            }

            processed.add(range);
            Range first = processed.getFirst();
            int lower = first.getLower();
            int upper = first.getUpper();
            int old = maxProcessed;
            if (le(lower, maxProcessed + 1))
            {
                maxProcessed = max(maxProcessed, upper);
            }
            boolean synced = ge(maxProcessed, syncPoint);
            flush = lt(old, syncPoint) && synced;
            if (synced)
            {
                syncPoint = maxProcessed;
            }
        }
        if (flush)
        {
            flushProcessed();
        }
    }

    void flushExpected()
    {
        RangeSet rs = new RangeSet();
        synchronized (processedLock)
        {
            if (incomingInit)
            {
                rs.add(commandsIn);
            }
        }
        sessionExpected(rs, null);
    }

    public void flushProcessed(Option ... options)
    {
        RangeSet copy;
        synchronized (processedLock)
        {
            copy = processed.copy();
        }

        synchronized (commands)
        {
            if (state == DETACHED || state == CLOSING)
            {
                return;
            }
            if (copy.size() > 0)
            {
	            sessionCompleted(copy, options);
            }
        }
    }

    void knownComplete(RangeSet kc)
    {
        synchronized (processedLock)
        {
            RangeSet newProcessed = new RangeSet();
            for (Range pr : processed)
            {
                for (Range kr : kc)
                {
                    for (Range r : pr.subtract(kr))
                    {
                        newProcessed.add(r);
                    }
                }
            }
            this.processed = newProcessed;
        }
    }

    void syncPoint()
    {
        int id = getCommandsIn() - 1;
        log.debug("%s synced to %d", this, id);
        boolean flush;
        synchronized (processedLock)
        {
            syncPoint = id;
            flush = ge(maxProcessed, syncPoint);
        }
        if (flush)
        {
            flushProcessed();
        }
    }

    protected boolean complete(int lower, int upper)
    {
        //avoid autoboxing
        if(log.isDebugEnabled())
        {
            log.debug("%s complete(%d, %d)", this, lower, upper);
        }
        synchronized (commands)
        {
            int old = maxComplete;
            for (int id = max(maxComplete, lower); le(id, upper); id++)
            {
                int idx = mod(id, commands.length);
                Method m = commands[idx];
                if (m != null)
                {
                    commandBytes -= m.getBodySize();
                    m.complete();
                    commands[idx] = null;
                }
            }
            if (le(lower, maxComplete + 1))
            {
                maxComplete = max(maxComplete, upper);
            }
            log.debug("%s   commands remaining: %s", this, commandsOut - maxComplete);
            commands.notifyAll();
            return gt(maxComplete, old);
        }
    }

    void received(Method m)
    {
        m.delegate(this, delegate);
    }

    private void send(Method m)
    {
        m.setChannel(channel);
        connection.send(m);

        if (!m.isBatch())
        {
            connection.flush();
        }
    }

    protected boolean isFull(int id)
    {
        return isCommandsFull(id) || isBytesFull();
    }

    protected boolean isBytesFull()
    {
        return commandBytes >= byteLimit;
    }

    protected boolean isCommandsFull(int id)
    {
        return id - maxComplete >= commands.length;
    }

    public void invoke(Method m)
    {
        invoke(m,(Runnable)null);
    }

    public void invoke(Method m, Runnable postIdSettingAction)
    {
        if (m.getEncodedTrack() == Frame.L4)
        {
            
            if (state == DETACHED && transacted)
            {
                state = CLOSED;
                delegate.closed(this);
                connection.removeSession(this);
                throw new SessionException(
                        "Session failed over, possibly in the middle of a transaction. " +
                        "Closing the session. Any Transaction in progress will be rolledback.");
            }
            
            if (m.hasPayload())
            {
                acquireCredit();
            }
            
            synchronized (commands)
            {
                if (state == DETACHED && m.isUnreliable())
                {
                    Thread current = Thread.currentThread();
                    if (!current.equals(resumer))
                    {
                        return;
                    }
                }

                if (state != OPEN && state != CLOSED && state != CLOSING)
                {
                    Thread current = Thread.currentThread();
                    if (!current.equals(resumer))
                    {
                        Waiter w = new Waiter(commands, timeout);
                        while (w.hasTime() && (state != OPEN && state != CLOSED))
                        {
                            w.await();
                        }
                    }
                }

                switch (state)
                {
                case OPEN:
                    break;
                case RESUMING:
                    Thread current = Thread.currentThread();
                    if (!current.equals(resumer))
                    {
                        throw new SessionException
                            ("timed out waiting for resume to finish");
                    }
                    break;
                case CLOSING:
                case CLOSED:
                    ExecutionException exc = getException();
                    if (exc != null)
                    {
                        throw new SessionException(exc);
                    }
                    else
                    {
                        throw new SessionClosedException();
                    }
                default:
                    throw new SessionException
                        (String.format
                         ("timed out waiting for session to become open " +
                          "(state=%s)", state));
                }

                int next;
                next = commandsOut++;
                m.setId(next);
                if(postIdSettingAction != null)
                {
                    postIdSettingAction.run();
                }

                if (isFull(next))
                {
                    Waiter w = new Waiter(commands, timeout);
                    while (w.hasTime() && isFull(next) && state != CLOSED)
                    {
                        if (state == OPEN || state == RESUMING)
                        {
                            try
                            {
                                sessionFlush(COMPLETED);
                            }
                            catch (SenderException e)
                            {
                                if (!closing)
                                {
                                    // if expiry is > 0 then this will
                                    // happen again on resume
                                    log.error(e, "error sending flush (full replay buffer)");
                                }
                                else
                                {
                                    e.rethrow();
                                }
                            }
                        }
                        w.await();
                    }
                }

                if (state == CLOSED)
                {
                    ExecutionException exc = getException();
                    if (exc != null)
                    {
                        throw new SessionException(exc);
                    }
                    else
                    {
                        throw new SessionClosedException();
                    }
                }

                if (isFull(next))
                {
                    throw new SessionException("timed out waiting for completion");
                }

                if (next == 0)
                {
                    sessionCommandPoint(0, 0);
                }
                
                boolean replayTransfer = !closing && !transacted &&
                                         m instanceof MessageTransfer &&
                                         ! m.isUnreliable();
                
                if ((replayTransfer) || m.hasCompletionListener())
                {
                    commands[mod(next, commands.length)] = m;
                    commandBytes += m.getBodySize();
                }
                if (autoSync)
                {
                    m.setSync(true);
                }
                needSync = !m.isSync();

                try
                {
                    send(m);
                }
                catch (SenderException e)
                {
                    if (!closing)
                    {
                        // if we are not closing then this will happen
                        // again on resume
                        log.error(e, "error sending command");
                    }
                    else
                    {
                        e.rethrow();
                    }
                }
                if (autoSync)
                {
                    sync();
                }

                // flush every 64K commands to avoid ambiguity on
                // wraparound
                if (shouldIssueFlush(next))
                {
                    try
                    {
                        sessionFlush(COMPLETED);
                    }
                    catch (SenderException e)
                    {
                        if (!closing)
                        {
                            // if expiry is > 0 then this will happen
                            // again on resume
                            log.error(e, "error sending flush (periodic)");
                        }
                        else
                        {
                            e.rethrow();
                        }
                    }
                }
            }
        }
        else
        {
            send(m);
        }
    }

    protected boolean shouldIssueFlush(int next)
    {
        return (next % 65536) == 0;
    }

    public void sync()
    {
        sync(timeout);
    }

    public void sync(long timeout)
    {
        log.debug("%s sync()", this);
        synchronized (commands)
        {
            int point = commandsOut - 1;

            if (needSync && lt(maxComplete, point))
            {
                executionSync(SYNC);
            }

            Waiter w = new Waiter(commands, timeout);
            while (w.hasTime() && state != CLOSED && lt(maxComplete, point))
            {
                log.debug("%s   waiting for[%d]: %d, %s", this, point, maxComplete, commands);
                w.await();
            }

            if (lt(maxComplete, point))
            {
                if (state != CLOSED)
                {
                    throw new SessionException(
		                    String.format("timed out waiting for sync: complete = %s, point = %s",
		                            maxComplete, point));
                }
                else
                {
                    ExecutionException ee = getException();
                    if (ee != null)
                    {
                        throw new SessionException(ee);
                    }
                }
            }
        }
    }

    private Map<Integer,ResultFuture<?>> results = new HashMap<Integer,ResultFuture<?>>();
    private ExecutionException exception = null;

    void result(int command, Struct result)
    {
        ResultFuture<?> future;
        synchronized (results)
        {
            future = results.remove(command);
        }
        
        if (future != null)
        {
            future.set(result);
        }
        else
        {
            log.warn("Received a response to a command" +
            		" that's no longer valid on the client side." +
            		" [ command id : %s , result : %s ]",command, result);
        }
    }

    void setException(ExecutionException exc)
    {
        synchronized (results)
        {
            if (exception != null)
            {
                throw new IllegalStateException(
                        String.format("too many exceptions: %s, %s", exception, exc));
            }
            exception = exc;
        }
    }

    private ConnectionClose close = null;

    void closeCode(ConnectionClose close)
    {
        this.close = close;
    }

    ExecutionException getException()
    {
        synchronized (results)
        {
            return exception;
        }
    }

    protected <T> Future<T> invoke(Method m, Class<T> klass)
    {
        synchronized (commands)
        {
            int command = commandsOut;
            ResultFuture<T> future = new ResultFuture<T>(klass);
            synchronized (results)
            {
                results.put(command, future);
            }
            invoke(m);
            return future;
        }
    }

    private class ResultFuture<T> implements Future<T>
    {

        private final Class<T> klass;
        private T result;

        private ResultFuture(Class<T> klass)
        {
            this.klass = klass;
        }

        private void set(Struct result)
        {
            synchronized (this)
            {
                this.result = klass.cast(result);
                notifyAll();
            }
        }

        public T get(long timeout)
        {
            synchronized (this)
            {
                Waiter w = new Waiter(this, timeout);
                while (w.hasTime() && state != CLOSED && !isDone())
                {
                    log.debug("%s waiting for result: %s", Session.this, this);
                    w.await();
                }
            }

            if (isDone())
            {
                return result;
            }
            else if (state == CLOSED)
            {
                throw new SessionException(getException());
            }
            else
            {
                throw new SessionException(
                        String.format("%s timed out waiting for result: %s",
                                   Session.this, this));
            }
        }

        public T get()
        {
            return get(timeout);
        }

        public boolean isDone()
        {
            return result != null;
        }

        public String toString()
        {
            return String.format("Future(%s)", isDone() ? result : klass);
        }

    }

    public final void messageTransfer(String destination,
                                      MessageAcceptMode acceptMode,
                                      MessageAcquireMode acquireMode,
                                      Header header,
                                      byte[] body,
                                      Option ... _options) {
        messageTransfer(destination, acceptMode, acquireMode, header,
                        ByteBuffer.wrap(body), _options);
    }

    public final void messageTransfer(String destination,
                                      MessageAcceptMode acceptMode,
                                      MessageAcquireMode acquireMode,
                                      Header header,
                                      String body,
                                      Option ... _options) {
        messageTransfer(destination, acceptMode, acquireMode, header,
                        toUTF8(body), _options);
    }

    public void close()
    {
        synchronized (commands)
        {
            state = CLOSING;
            setClose(true);
            sessionRequestTimeout(0);
            sessionDetach(name.getBytes());

            awaitClose();
 

        }
    }

    protected void awaitClose() 
    {
        Waiter w = new Waiter(commands, timeout);
        while (w.hasTime() && state != CLOSED)
        {
            w.await();
        }

        if (state != CLOSED)
        {
            throw new SessionException("close() timed out");
        }
    }

    public void exception(Throwable t)
    {
        log.error(t, "caught exception");
    }

    public void closed()
    {
        synchronized (commands)
        {
            if (closing || getException() != null)
            {
                state = CLOSED;
            }
            else
            {
                state = DETACHED;
            }

            commands.notifyAll();

            synchronized (results)
            {
                for (ResultFuture<?> result : results.values())
                {
                    synchronized(result)
                    {
                        result.notifyAll();
                    }
                }
            }
            if(state == CLOSED)
            {
                delegate.closed(this);
            }
            else
            {
                delegate.detached(this);
            }
        }

        if(state == CLOSED)
        {
            connection.removeSession(this);   
            listener.closed(this);
        }
    }

    public boolean isClosing()
    {
        return state == CLOSED || state == CLOSING;
    }

    public String toString()
    {
        return String.format("ssn:%s", name);
    }

    public void setTransacted(boolean b) {
        this.transacted = b;
    }

    public boolean isTransacted(){
        return transacted;
    }

    public void setDetachCode(SessionDetachCode dtc)
    {
        this.detachCode = dtc;
    }

    public SessionDetachCode getDetachCode()
    {
        return this.detachCode;
    }

    public void awaitOpen()
    {
        switch (state)
        {
        case NEW:
            synchronized(stateLock)
            {
                Waiter w = new Waiter(stateLock, timeout);
                while (w.hasTime() && state == NEW)
                {
                    w.await();
                }
            }

            if (state != OPEN)
            {
                throw new SessionException("Timed out waiting for Session to open");
            }
        case DETACHED:
        case CLOSING:
        case CLOSED:
            throw new SessionException("Session closed");
        default :
            break;
        }
    }

    public Object getStateLock()
    {
        return stateLock;
    }
}
