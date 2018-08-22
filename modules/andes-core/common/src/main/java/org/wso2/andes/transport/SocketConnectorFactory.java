package org.wso2.andes.transport;

import org.wso2.org.apache.mina.common.IoConnector;

public interface SocketConnectorFactory
{
    IoConnector newConnector();
}