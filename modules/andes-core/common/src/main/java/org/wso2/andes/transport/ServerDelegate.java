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

import static org.wso2.andes.transport.Connection.State.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ServerDelegate
 */
public class ServerDelegate extends ConnectionDelegate
{
    protected static final Logger _logger = LoggerFactory.getLogger(ServerDelegate.class);

    private List<Object> _locales;
    private List<Object> _mechanisms;
    private Map<String, Object> _clientProperties;


    public ServerDelegate()
    {
        this(null, Collections.emptyList(), Collections.singletonList((Object)"utf8"));
    }

    protected ServerDelegate(Map<String, Object> clientProperties, List<Object> mechanisms, List<Object> locales)
    {
        _clientProperties = clientProperties;
        _mechanisms = mechanisms;
        _locales = locales;
    }

    public void init(Connection conn, ProtocolHeader hdr)
    {
        conn.send(new ProtocolHeader(1, 0, 10));

        conn.connectionStart(_clientProperties, _mechanisms, _locales);
    }

    @Override
    public void connectionStartOk(Connection conn, ConnectionStartOk ok)
    {
        conn.setLocale(ok.getLocale());
        String mechanism = ok.getMechanism();

        String clientName = (String) ok.getClientProperties().get("clientName");
        conn.setClientId(clientName);

        if (mechanism == null || mechanism.length() == 0)
        {
            tuneAuthorizedConnection(conn);
            return;
        }

        secure(conn, ok.getResponse());
    }

    protected void secure(final Connection conn, final byte[] response)
    {

//        try {
//            AMQPAuthenticationManager.authenticate(response);
//            tuneAuthorizedConnection(conn);
//        } catch (LoginException e) {
//            connectionAuthFailed(conn, e);
//        }
    }

    protected void connectionAuthFailed(final Connection conn, Exception e)
    {
        conn.exception(e);
        conn.connectionClose(ConnectionCloseCode.CONNECTION_FORCED, e.getMessage());
    }

    protected void connectionAuthContinue(final Connection conn, byte[] challenge)
    {
        conn.connectionSecure(challenge);
    }

    protected void tuneAuthorizedConnection(final Connection conn)
    {
        conn.connectionTune
                (getChannelMax(),
                        org.wso2.andes.transport.network.ConnectionBinding.MAX_FRAME_SIZE,
                        0, getHeartbeatMax());
    }

    protected int getHeartbeatMax()
    {
        return 0xFFFF;
    }

    protected int getChannelMax()
    {
        return 0xFFFF;
    }

    @Override
    public void connectionSecureOk(Connection conn, ConnectionSecureOk ok)
    {
        secure(conn, ok.getResponse());
    }

    @Override
    public void connectionTuneOk(Connection conn, ConnectionTuneOk ok)
    {
        int okChannelMax = ok.getChannelMax();
        
        if (okChannelMax > getChannelMax())
        {
            _logger.error("Connection '" + conn.getConnectionId() + "' being severed, " +
                    "client connectionTuneOk returned a channelMax (" + okChannelMax +
                    ") above the servers offered limit (" + getChannelMax() +")");

            //Due to the error we must forcefully close the connection without negotiation
            conn.getSender().close();
            return;
        }

        //0 means no implied limit, except available server resources
        //(or that forced by protocol limitations [0xFFFF])
        conn.setChannelMax(okChannelMax == 0 ? Connection.MAX_CHANNEL_MAX : okChannelMax);
    }

    @Override
    public void connectionOpen(Connection conn, ConnectionOpen open)
    {
        conn.connectionOpenOk(Collections.emptyList());

        conn.setState(OPEN);
    }

    protected Session getSession(Connection conn, SessionDelegate delegate, SessionAttach atc)
    {
        return new Session(conn, delegate, new Binary(atc.getName()), 0);
    }


    public Session getSession(Connection conn, SessionAttach atc)
    {
        return new Session(conn, new Binary(atc.getName()), 0);
    }

    @Override
    public void sessionAttach(Connection conn, SessionAttach atc)
    {
        Session ssn = getSession(conn, atc);
        conn.map(ssn, atc.getChannel());
        ssn.sessionAttached(atc.getName());
        ssn.setState(Session.State.OPEN);
    }
}
