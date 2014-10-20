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

public class QMFCommandHeader
{
    private final byte _version;
    private final int _seq;

    private final QMFOperation _operation;

    public QMFCommandHeader(byte version, int seq, QMFOperation operation)
    {
        _version = version;
        _seq = seq;
        _operation = operation;
    }

    public byte getVersion()
    {
        return _version;
    }

    public int getSeq()
    {
        return _seq;
    }

    public QMFOperation getOperation()
    {
        return _operation;
    }

    public void encode(BBEncoder encoder)
    {
        encoder.writeUint8((short)'A');
        encoder.writeUint8((short)'M');
        encoder.writeInt8(_version);
        encoder.writeUint8((short)_operation.getOpcode());
        encoder.writeInt32(_seq);
    }
}
