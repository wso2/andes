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

import org.wso2.andes.server.message.ServerMessage;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.transport.codec.BBEncoder;

public abstract class QMFCommand
{

    private final QMFCommandHeader _header;

    protected QMFCommand(QMFCommandHeader header)
    {
        _header = header;
    }


    public void process(final VirtualHost virtualHost, final ServerMessage message)
    {
        throw new UnsupportedOperationException();
    }

    public void encode(BBEncoder encoder)
    {
        _header.encode(encoder);

    }

    public QMFCommandHeader getHeader()
    {
        return _header;
    }
}
