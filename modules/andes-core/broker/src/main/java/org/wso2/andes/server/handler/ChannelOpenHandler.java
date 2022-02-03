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
package org.wso2.andes.server.handler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.ChannelOpenBody;
import org.wso2.andes.framing.ChannelOpenOkBody;
import org.wso2.andes.framing.MethodRegistry;
import org.wso2.andes.framing.ProtocolVersion;
import org.wso2.andes.framing.amqp_0_9.MethodRegistry_0_9;
import org.wso2.andes.framing.amqp_0_91.MethodRegistry_0_91;
import org.wso2.andes.framing.amqp_8_0.MethodRegistry_8_0;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.state.AMQStateManager;
import org.wso2.andes.server.state.StateAwareMethodListener;
import org.wso2.andes.server.virtualhost.VirtualHost;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class ChannelOpenHandler implements StateAwareMethodListener<ChannelOpenBody>
{
    private static final Log _logger = LogFactory.getLog(ChannelOpenHandler.class);
    
    private static ChannelOpenHandler _instance = new ChannelOpenHandler();

    private final MBeanServer server;

    public static ChannelOpenHandler getInstance()
    {
        return _instance;
    }

    private ChannelOpenHandler()
    {
        List<MBeanServer> servers = MBeanServerFactory.findMBeanServer(null);
        this.server = servers.iterator().next();
    }

    public void methodReceived(AMQStateManager stateManager, ChannelOpenBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();
        VirtualHost virtualHost = session.getVirtualHost();
        
        // Protect the broker against out of order frame request.
        if (virtualHost == null)
        {
            throw new AMQException(AMQConstant.COMMAND_INVALID, "Virtualhost has not yet been set. ConnectionOpen has not been called.", null);
        }
        _logger.info("Connecting to: " + virtualHost.getName());

        final AMQChannel channel = new AMQChannel(session, channelId, virtualHost.getMessageStore());

        session.addChannel(channel);

        /* Registering the AMQChannel MBean upon creation */
//        this.registerAMQChannelMBean(channel, channel.getId());

        ChannelOpenOkBody response;

        ProtocolVersion pv = session.getProtocolVersion();

        if(pv.equals(ProtocolVersion.v8_0))
        {
            MethodRegistry_8_0 methodRegistry = (MethodRegistry_8_0) MethodRegistry.getMethodRegistry(ProtocolVersion.v8_0);
            response = methodRegistry.createChannelOpenOkBody();

        }
        else if(pv.equals(ProtocolVersion.v0_9))
        {
            MethodRegistry_0_9 methodRegistry = (MethodRegistry_0_9) MethodRegistry.getMethodRegistry(ProtocolVersion.v0_9);
            UUID uuid = UUID.randomUUID();
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(output);
            try
            {
                dataOut.writeLong(uuid.getMostSignificantBits());
                dataOut.writeLong(uuid.getLeastSignificantBits());
                dataOut.flush();
                dataOut.close();
            }
            catch (IOException e)
            {
                // This *really* shouldn't happen as we're not doing any I/O
                throw new RuntimeException("I/O exception when writing to byte array", e);
            }

            // should really associate this channelId to the session
            byte[] channelName = output.toByteArray();
            
            response = methodRegistry.createChannelOpenOkBody(channelName);
        }
        else if(pv.equals(ProtocolVersion.v0_91))
        {
            MethodRegistry_0_91 methodRegistry = (MethodRegistry_0_91) MethodRegistry.getMethodRegistry(ProtocolVersion.v0_91);
            UUID uuid = UUID.randomUUID();
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(output);
            try
            {
                dataOut.writeLong(uuid.getMostSignificantBits());
                dataOut.writeLong(uuid.getLeastSignificantBits());
                dataOut.flush();
                dataOut.close();
            }
            catch (IOException e)
            {
                // This *really* shouldn't happen as we're not doing any I/O
                throw new RuntimeException("I/O exception when writing to byte array", e);
            }

            // should really associate this channelId to the session
            byte[] channelName = output.toByteArray();

            response = methodRegistry.createChannelOpenOkBody(channelName);
        }
        else
        {
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Got channel open for protocol version not catered for: " + pv, null);
        }


        session.writeFrame(response.generateFrame(channelId));
    }

//    private void registerAMQChannelMBean(AMQChannel channel, UUID channelId) throws AMQException {
//        try {
//            this.getMBeanServer().registerMBean(channel,
//                    new ObjectName("amqchannel:name=amqpchannel" + channelId));
//        } catch (Exception e) {
//            String msg = "Error occurred : " + e.getMessage();
//            _logger.error(msg);
//            throw new AMQException(msg, e);
//        }
//    }

    private MBeanServer getMBeanServer() {
        return server;
    }

}
