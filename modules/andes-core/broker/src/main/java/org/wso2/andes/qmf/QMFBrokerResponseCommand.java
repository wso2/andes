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

package org.wso2.andes.qmf;

import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.transport.codec.BBEncoder;

public class QMFBrokerResponseCommand extends QMFCommand
{
    private QMFCommandHeader _header;
    private VirtualHost _virtualHost;

    public QMFBrokerResponseCommand(QMFBrokerRequestCommand qmfBrokerRequestCommand, VirtualHost virtualHost)
    {
        super( new QMFCommandHeader(qmfBrokerRequestCommand.getHeader().getVersion(),
                                    qmfBrokerRequestCommand.getHeader().getSeq(),
                                    QMFOperation.BROKER_RESPONSE));
        _virtualHost = virtualHost;
    }

    public void encode(BBEncoder encoder)
    {
        super.encode(encoder);
        encoder.writeUuid(_virtualHost.getBrokerId());
    }
}
