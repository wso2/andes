/*
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
 */
package org.wso2.andes.transport.network.io;

import static org.wso2.andes.transport.util.Functions.mod;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.wso2.andes.common.Closeable;
import org.wso2.andes.thread.Threading;
import org.wso2.andes.transport.Sender;
import org.wso2.andes.transport.SenderException;
import org.wso2.andes.transport.TransportException;
import org.wso2.andes.transport.util.Logger;


public final class IoSender implements Runnable, Sender<ByteBuffer>
{

    private static final Logger log = Logger.get(IoSender.class);

    // by starting here, we ensure that we always test the wraparound
    // case, we should probably make this configurable somehow so that
    // we can test other cases as well
    private final static int START = Integer.MAX_VALUE - 10;

    private final long timeout;
    private final Socket socket;
    private final OutputStream out;

    private final byte[] buffer;
    private volatile int head = START;
    private volatile int tail = START;
    private volatile boolean idle = true;
    private final Object notFull = new Object();
    private final Object notEmpty = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread senderThread;
    private final List<Closeable> _listeners = new ArrayList<Closeable>();
    
    private volatile Throwable exception = null;

    public IoSender(Socket socket, int bufferSize, long timeout)
    {
        this.socket = socket;
        this.buffer = new byte[pof2(bufferSize)]; // buffer size must be a power of 2
        this.timeout = timeout;

        try
        {
            out = socket.getOutputStream();
        }
        catch (IOException e)
        {
            throw new TransportException("Error getting output stream for socket", e);
        }

        try
        {
            //Create but deliberately don't start the thread.
            senderThread = Threading.getThreadFactory().createThread(this);                      
        }
        catch(Exception e)
        {
            throw new Error("Error creating IOSender thread",e);
        }
        
        senderThread.setDaemon(true);
        senderThread.setName(String.format("IoSender - %s", socket.getRemoteSocketAddress()));
    }

    public void initiate()
    {
        senderThread.start();
    }

    private static final int pof2(int n)
    {
        int result = 1;
        while (result < n)
        {
            result *= 2;
        }
        return result;
    }

    public void send(ByteBuffer buf)
    {
        if (closed.get())
        {
            throw new SenderException("sender is closed", exception);
        }

        final int size = buffer.length;
        int remaining = buf.remaining();

        while (remaining > 0)
        {
            final int hd = head;
            final int tl = tail;

            if (hd - tl >= size)
            {
                flush();
                synchronized (notFull)
                {
                    long start = System.currentTimeMillis();
                    long elapsed = 0;
                    while (!closed.get() && head - tail >= size && elapsed < timeout)
                    {
                        try
                        {
                            notFull.wait(timeout - elapsed);
                        }
                        catch (InterruptedException e)
                        {
                            // pass
                        }
                        elapsed += System.currentTimeMillis() - start;
                    }

                    if (closed.get())
                    {
                        throw new SenderException("sender is closed", exception);
                    }

                    if (head - tail >= size)
                    {
                        throw new SenderException(String.format("write timed out: %s, %s", head, tail));
                    }
                }
                continue;
            }

            final int hd_idx = mod(hd, size);
            final int tl_idx = mod(tl, size);
            final int length;

            if (tl_idx > hd_idx)
            {
                length = Math.min(tl_idx - hd_idx, remaining);
            }
            else
            {
                length = Math.min(size - hd_idx, remaining);
            }

            buf.get(buffer, hd_idx, length);
            head += length;
            remaining -= length;
        }
    }

    public void flush()
    {
        if (idle)
        {
            synchronized (notEmpty)
            {
                notEmpty.notify();
            }
        }
    }

    public void close()
    {
        close(true);
    }

    void close(boolean reportException)
    {
        if (!closed.getAndSet(true))
        {
            synchronized (notFull)
            {
                notFull.notify();
            }

            synchronized (notEmpty)
            {
                notEmpty.notify();
            }

            try
            {
                if (Thread.currentThread() != senderThread)
                {
                    senderThread.join(timeout);
                    if (senderThread.isAlive())
                    {
                        log.error("join timed out");
                        throw new SenderException("join timed out");
                    }
                }
            }
            catch (InterruptedException e)
            {
                log.error("interrupted whilst waiting for sender thread to stop");
                throw new SenderException(e);
            }
            finally
            {
                closeListeners();
            }
            if (reportException && exception != null)
            {
                throw new SenderException(exception);
            }
        }
    }

    private void closeListeners()
    {
        Exception ex = null;
        for(Closeable listener : _listeners)
        {
            try
            {
                listener.close();
            }
            catch(Exception e)
            {
                log.error("Exception closing listener: " + e.getMessage());
                ex = e;
            }
        }

        if (ex != null)
        {
            throw new SenderException(ex.getMessage(), ex);
        }
    }

    public void run()
    {
        final int size = buffer.length;       
        while (true)
        {
            final int hd = head;
            final int tl = tail;

            if (hd == tl)
            {
                if (closed.get())
                {
                    break;
                }

                idle = true;

                synchronized (notEmpty)
                {
                    while (head == tail && !closed.get())
                    {
                        try
                        {
                            notEmpty.wait();
                        }
                        catch (InterruptedException e)
                        {
                            // pass
                        }
                    }
                }

                idle = false;

                continue;
            }

            final int hd_idx = mod(hd, size);
            final int tl_idx = mod(tl, size);

            final int length;
            if (tl_idx < hd_idx)
            {
                length = hd_idx - tl_idx;
            }
            else
            {
                length = size - tl_idx;
            }

            try
            {
                out.write(buffer, tl_idx, length);
            }
            catch (IOException e)
            {
                log.error(e, "error in write thread");
                exception = e;
                close(false);
                break;
            }
            tail += length;
            if (head - tl >= size)
            {
                synchronized (notFull)
                {
                    notFull.notify();
                }
            }
        }
    }

    public void setIdleTimeout(int i)
    {
        try
        {
            socket.setSoTimeout(i);
        }
        catch (Exception e)
        {
            throw new SenderException(e);
        }
    }

    public void registerCloseListener(Closeable listener)
    {
        _listeners.add(listener);
    }
}
