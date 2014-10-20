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


/**
 * ProtocolVersionException
 *
 */

public final class ProtocolVersionException extends ConnectionException
{

    private final byte major;
    private final byte minor;

    public ProtocolVersionException(byte major, byte minor, Throwable cause)
    {
        super(String.format("version mismatch: %s-%s", major, minor), cause);
        this.major = major;
        this.minor = minor;
    }

    public ProtocolVersionException(byte major, byte minor)
    {
        this(major, minor, null);
    }

    public byte getMajor()
    {
        return this.major;
    }

    public byte getMinor()
    {
        return this.minor;
    }

    @Override public void rethrow()
    {
        throw new ProtocolVersionException(major, minor, this);
    }

}
