/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing,
*  software distributed under the License is distributed on an
*  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*  KIND, either express or implied.  See the License for the
*  specific language governing permissions and limitations
*  under the License.
*/

package org.dna.mqtt.wso2;

import com.lmax.disruptor.ExceptionHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MQTTLogExceptionHandler implements ExceptionHandler {
    private static Log log = LogFactory.getLog(MQTTLogExceptionHandler.class);

    @Override
    public void handleEventException(Throwable throwable, long l, Object object) {
        log.error("ValueEvent exception occurred on disruptor.", throwable);
    }

    @Override
    public void handleOnStartException(Throwable throwable) {
        log.error("Error while starting MQTT Disruptor ", throwable);
    }

    @Override
    public void handleOnShutdownException(Throwable throwable) {
        log.error("Error while shutting down MQTT Disruptor ", throwable);
    }
}
