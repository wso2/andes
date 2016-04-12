/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.andes.mqtt;

import io.netty.channel.Channel;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.FlowControlListener;

import java.util.UUID;

/**
 * Holds the channel information of the publisher
 */
public class MQTTPublisherChannel implements FlowControlListener {
    /**
     * Unique id of a publisher representing the cluster
     */
    private UUID clusterID = null;

    /**
     * Holds the channel information of the socket
     */
    private Channel socket = null;

    /**
     * The channel representation of the publisher
     * This will be used for flow controlling
     */
    private AndesChannel channel = null;

    public UUID getClusterID() {
        return clusterID;
    }

    public MQTTPublisherChannel(Channel mqttSocket) {
        this.socket = mqttSocket;
        //Will randomly generate a unique cluster id
        clusterID = UUID.randomUUID();
    }

    @Override
    public void block() {
        socket.config().setAutoRead(false);
    }

    @Override
    public void unblock() {
        socket.config().setAutoRead(true);
    }

    @Override
    public void disconnect(){
        socket.close();
    }
    
    
    public void setChannel(AndesChannel channel) {
        this.channel = channel;
    }

    public AndesChannel getChannel() {
        return channel;
    }
}
