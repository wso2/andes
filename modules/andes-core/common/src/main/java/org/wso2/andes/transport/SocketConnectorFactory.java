package org.wso2.andes.transport;

import org.apache.mina.common.IoConnector;

public interface SocketConnectorFactory
{
    IoConnector newConnector();
}