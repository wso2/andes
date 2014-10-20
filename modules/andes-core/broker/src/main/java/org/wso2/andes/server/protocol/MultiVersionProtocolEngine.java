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
package org.wso2.andes.server.protocol;


import org.apache.log4j.Logger;
import org.wso2.andes.protocol.ProtocolEngine;
import org.wso2.andes.server.registry.IApplicationRegistry;
import org.wso2.andes.server.transport.ServerConnection;
import org.wso2.andes.transport.ConnectionDelegate;
import org.wso2.andes.transport.Sender;
import org.wso2.andes.transport.network.NetworkConnection;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;

public class MultiVersionProtocolEngine implements ProtocolEngine
{
    private static final Logger _logger = Logger.getLogger(MultiVersionProtocolEngine.class);

    private Set<AmqpProtocolVersion> _supported;
    private String _fqdn;
    private IApplicationRegistry _appRegistry;
    private NetworkConnection _network;
    private Sender<ByteBuffer> _sender;
    
    private volatile ProtocolEngine _delegate = new SelfDelegateProtocolEngine();

    public MultiVersionProtocolEngine(IApplicationRegistry appRegistry,
                                      String fqdn,
                                      Set<AmqpProtocolVersion> supported,
                                      NetworkConnection network)
    {
        _appRegistry = appRegistry;
        _fqdn = fqdn;
        _supported = supported;
        _network = network;
        _sender = _network.getSender();
    }

    public SocketAddress getRemoteAddress()
    {
        return _delegate.getRemoteAddress();
    }

    public SocketAddress getLocalAddress()
    {
        return _delegate.getLocalAddress();
    }

    public long getWrittenBytes()
    {
        return _delegate.getWrittenBytes();
    }

    public long getReadBytes()
    {
        return _delegate.getReadBytes();
    }

    public void closed()
    {
        _delegate.closed();
    }

    public void writerIdle()
    {
        _delegate.writerIdle();
    }

    public void readerIdle()
    {
        _delegate.readerIdle();
    }

    public void received(ByteBuffer msg)
    {
        _delegate.received(msg);
    }

    public void exception(Throwable t)
    {
        _delegate.exception(t);
    }

    private static final int MINIMUM_REQUIRED_HEADER_BYTES = 8;

    private static final byte[] AMQP_0_8_HEADER =
            new byte[] { (byte) 'A',
                         (byte) 'M',
                         (byte) 'Q',
                         (byte) 'P',
                         (byte) 1,
                         (byte) 1,
                         (byte) 8,
                         (byte) 0
            };

    private static final byte[] AMQP_0_9_HEADER =
            new byte[] { (byte) 'A',
                         (byte) 'M',
                         (byte) 'Q',
                         (byte) 'P',
                         (byte) 1,
                         (byte) 1,
                         (byte) 0,
                         (byte) 9
            };

private static final byte[] AMQP_0_9_1_HEADER =
            new byte[] { (byte) 'A',
                         (byte) 'M',
                         (byte) 'Q',
                         (byte) 'P',
                         (byte) 0,
                         (byte) 0,
                         (byte) 9,
                         (byte) 1
            };


    private static final byte[] AMQP_0_10_HEADER =
            new byte[] { (byte) 'A',
                         (byte) 'M',
                         (byte) 'Q',
                         (byte) 'P',
                         (byte) 1,
                         (byte) 1,
                         (byte) 0,
                         (byte) 10
            };

    private static interface DelegateCreator
    {
        AmqpProtocolVersion getVersion();
        byte[] getHeaderIdentifier();
        ProtocolEngine getProtocolEngine();
    }

    private DelegateCreator creator_0_8 = new DelegateCreator()
    {

        public AmqpProtocolVersion getVersion()
        {
            return AmqpProtocolVersion.v0_8;
        }

        public byte[] getHeaderIdentifier()
        {
            return AMQP_0_8_HEADER;
        }

        public ProtocolEngine getProtocolEngine()
        {
            return new AMQProtocolEngine(_appRegistry.getVirtualHostRegistry(), _network);
        }
    };

    private DelegateCreator creator_0_9 = new DelegateCreator()
    {

        public AmqpProtocolVersion getVersion()
        {
            return AmqpProtocolVersion.v0_9;
        }


        public byte[] getHeaderIdentifier()
        {
            return AMQP_0_9_HEADER;
        }

        public ProtocolEngine getProtocolEngine()
        {
            return new AMQProtocolEngine(_appRegistry.getVirtualHostRegistry(), _network);
        }
    };

    private DelegateCreator creator_0_9_1 = new DelegateCreator()
    {

        public AmqpProtocolVersion getVersion()
        {
            return AmqpProtocolVersion.v0_9_1;
        }


        public byte[] getHeaderIdentifier()
        {
            return AMQP_0_9_1_HEADER;
        }

        public ProtocolEngine getProtocolEngine()
        {
            return new AMQProtocolEngine(_appRegistry.getVirtualHostRegistry(), _network);
        }
    };


    private DelegateCreator creator_0_10 = new DelegateCreator()
    {

        public AmqpProtocolVersion getVersion()
        {
            return AmqpProtocolVersion.v0_10;
        }


        public byte[] getHeaderIdentifier()
        {
            return AMQP_0_10_HEADER;
        }

        public ProtocolEngine getProtocolEngine()
        {
            final ConnectionDelegate connDelegate =
                    new org.wso2.andes.server.transport.ServerConnectionDelegate(_appRegistry, _fqdn);

            ServerConnection conn = new ServerConnection();
            conn.setConnectionDelegate(connDelegate);

            return new ProtocolEngine_0_10( conn, _network, _appRegistry);
        }
    };

    private final DelegateCreator[] _creators =
            new DelegateCreator[] { creator_0_8, creator_0_9, creator_0_9_1, creator_0_10 };


    private class ClosedDelegateProtocolEngine implements ProtocolEngine
    {
        public SocketAddress getRemoteAddress()
        {
            return _network.getRemoteAddress();
        }

        public SocketAddress getLocalAddress()
        {
            return _network.getLocalAddress();
        }

        public long getWrittenBytes()
        {
            return 0;
        }

        public long getReadBytes()
        {
            return 0;
        }

        public void received(ByteBuffer msg)
        {
            _logger.error("Error processing incoming data, could not negotiate a common protocol");
        }

        public void exception(Throwable t)
        {
            _logger.error("Error establishing session", t);
        }

        public void closed()
        {

        }

        public void writerIdle()
        {

        }

        public void readerIdle()
        {

        }
    }

    private class SelfDelegateProtocolEngine implements ProtocolEngine
    {
        private final ByteBuffer _header = ByteBuffer.allocate(MINIMUM_REQUIRED_HEADER_BYTES);

        public SocketAddress getRemoteAddress()
        {
            return _network.getRemoteAddress();
        }

        public SocketAddress getLocalAddress()
        {
            return _network.getLocalAddress();
        }

        public long getWrittenBytes()
        {
            return 0;
        }

        public long getReadBytes()
        {
            return 0;
        }

        public void received(ByteBuffer msg)
        {
            ByteBuffer msgheader = msg.duplicate();
            if(_header.remaining() > msgheader.limit())
            {
                msg.position(msg.limit());
            }
            else
            {
                msgheader.limit(_header.remaining());
                msg.position(_header.remaining());
            }

            _header.put(msgheader);

            if(!_header.hasRemaining())
            {
                _header.flip();
                byte[] headerBytes = new byte[MINIMUM_REQUIRED_HEADER_BYTES];
                _header.get(headerBytes);


                ProtocolEngine newDelegate = null;
                byte[] newestSupported = null;

                for(int i = 0; newDelegate == null && i < _creators.length; i++)
                {

                    if(_supported.contains(_creators[i].getVersion()))
                    {
                        newestSupported = _creators[i].getHeaderIdentifier();
                        byte[] compareBytes = _creators[i].getHeaderIdentifier();
                        boolean equal = true;
                        for(int j = 0; equal && j<compareBytes.length; j++)
                        {
                            equal = headerBytes[j] == compareBytes[j];
                        }
                        if(equal)
                        {
                            newDelegate = _creators[i].getProtocolEngine();
                        }
                    }
                }

                // If no delegate is found then send back the most recent support protocol version id
                if(newDelegate == null)
                {
                    _sender.send(ByteBuffer.wrap(newestSupported));

                    _delegate = new ClosedDelegateProtocolEngine();
                }
                else
                {
                    _delegate = newDelegate;

                    _header.flip();
                    _delegate.received(_header);
                    if(msg.hasRemaining())
                    {
                        _delegate.received(msg);
                    }
                }

            }

        }

        public void exception(Throwable t)
        {
            _logger.error("Error establishing session", t);
        }

        public void closed()
        {

        }

        public void writerIdle()
        {

        }

        public void readerIdle()
        {

        }
    }
}
