/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.client;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

class XAResource_0_9_1 implements XAResource {
    private final XASession_9_1 session;

    XAResource_0_9_1(XASession_9_1 xaSession_9_1) {
        session = xaSession_9_1;
    }

    @Override
    public void commit(Xid xid, boolean b) throws XAException {
        throw new RuntimeException("Feature NotImplemented");
    }

    @Override
    public void end(Xid xid, int i) throws XAException {
        throw new RuntimeException("Feature NotImplemented");
    }

    @Override
    public void forget(Xid xid) throws XAException {
        throw new RuntimeException("Feature NotImplemented");
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        throw new RuntimeException("Feature NotImplemented");
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        throw new RuntimeException("Feature NotImplemented");
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        throw new RuntimeException("Feature NotImplemented");
    }

    @Override
    public Xid[] recover(int i) throws XAException {
        throw new RuntimeException("Feature NotImplemented");
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        throw new RuntimeException("Feature NotImplemented");
    }

    @Override
    public boolean setTransactionTimeout(int i) throws XAException {
        throw new RuntimeException("Feature NotImplemented");
    }

    @Override
    public void start(Xid xid, int i) throws XAException {
        throw new RuntimeException("Feature NotImplemented");
    }
}
