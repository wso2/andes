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
package org.wso2.andes.transport.network;

import java.nio.ByteBuffer;

import org.wso2.andes.transport.Binding;
import org.wso2.andes.transport.Connection;
import org.wso2.andes.transport.ConnectionDelegate;
import org.wso2.andes.transport.ConnectionListener;
import org.wso2.andes.transport.Receiver;
import org.wso2.andes.transport.Sender;
import org.wso2.andes.transport.network.security.sasl.SASLReceiver;
import org.wso2.andes.transport.network.security.sasl.SASLSender;

/**
 * ConnectionBinding
 *
 */

public abstract class ConnectionBinding
    implements Binding<Connection,ByteBuffer>
{

    public static Binding<Connection,ByteBuffer> get(final Connection connection)
    {
        return new ConnectionBinding()
        {
            public Connection connection()
            {
                return connection;
            }
        };
    }

    public static Binding<Connection,ByteBuffer> get(final ConnectionDelegate delegate)
    {
        return new ConnectionBinding()
        {
            public Connection connection()
            {
                Connection conn = new Connection();
                conn.setConnectionDelegate(delegate);
                return conn;
            }
        };
    }

    public static final int MAX_FRAME_SIZE = 64 * 1024 - 1;

    public abstract Connection connection();

    public Connection endpoint(Sender<ByteBuffer> sender)
    {
        Connection conn = connection();

        if (conn.getConnectionSettings() != null && 
            conn.getConnectionSettings().isUseSASLEncryption())
        {
            sender = new SASLSender(sender);
            conn.addConnectionListener((ConnectionListener)sender);
        }
        
        // XXX: hardcoded max-frame
        Disassembler dis = new Disassembler(sender, MAX_FRAME_SIZE);
        conn.setSender(dis);
        return conn;
    }

    public Receiver<ByteBuffer> receiver(Connection conn)
    {
        if (conn.getConnectionSettings() != null && 
            conn.getConnectionSettings().isUseSASLEncryption())
        {
            SASLReceiver receiver = new SASLReceiver(new InputHandler(new Assembler(conn)));
            conn.addConnectionListener((ConnectionListener)receiver);
            return receiver;
        }
        else
        {
            return new InputHandler(new Assembler(conn));
        }
    }

}
