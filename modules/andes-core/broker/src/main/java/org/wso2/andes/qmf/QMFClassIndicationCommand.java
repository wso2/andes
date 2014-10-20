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

import org.wso2.andes.transport.codec.BBEncoder;

public class QMFClassIndicationCommand extends QMFCommand
{
    private QMFClass _qmfClass;

    public QMFClassIndicationCommand(QMFClassQueryCommand qmfClassQueryCommand, QMFClass qmfClass)
    {
        super(new QMFCommandHeader(qmfClassQueryCommand.getHeader().getVersion(),
                                    qmfClassQueryCommand.getHeader().getSeq(),
                                    QMFOperation.CLASS_INDICATION));
        _qmfClass = qmfClass;
    }


    @Override
    public void encode(BBEncoder encoder)
    {
        super.encode(encoder);
        encoder.writeUint8(_qmfClass.getType().getValue());
        encoder.writeStr8(_qmfClass.getPackage().getName());
        encoder.writeStr8(_qmfClass.getName());
        encoder.writeBin128(_qmfClass.getSchemaHash());
    }
}
