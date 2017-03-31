/*
 * Copyright (c) 2017, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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
package org.wso2.andes.jms.failover;

import org.wso2.andes.jms.ConnectionURL;

/**
 * Extend the Round Robin Failover Model to gain retry functionality but once connected do not attempt to failover.
 */
public class FailoverOneTime extends FailoverRoundRobinServers {
    private boolean _connected = false;

    public FailoverOneTime(ConnectionURL connectionDetails) {
        super(connectionDetails);
    }

    @Override
    public void attainedConnection() {
        _connected = true;
        _currentCycleRetries = _cycleRetries;
        _currentServerRetry = _serverRetries;
    }

    @Override
    public String methodName() {
        return "OneTimeFailover";
    }

    @Override
    public String toString() {
        return super.toString() + (_connected ? "Connection attained." : "Never connected.") + "\n";
    }

}
