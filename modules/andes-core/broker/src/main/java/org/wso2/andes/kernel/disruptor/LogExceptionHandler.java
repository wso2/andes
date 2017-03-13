/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.disruptor;

import com.lmax.disruptor.ExceptionHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer;

/**
 * Disruptor Inbound event related logging exception handler
 * This exception handler will log exceptions that were not handled by event handlers and keep disruptor
 * working.
 */
public class LogExceptionHandler implements ExceptionHandler<InboundEventContainer> {

    private static Log log = LogFactory.getLog(LogExceptionHandler.class);

    @Override
    public void handleEventException(Throwable throwable, long sequence, InboundEventContainer event) {

        // NOTE: Event type will be set to IGNORE event type if the exception is coming from StateEventHandler
        String eventType;
        if (event.getEventType() == InboundEventContainer.Type.IGNORE_EVENT) {
            eventType = "";
        } else {
            eventType = "Event type: " + event.getEventType().toString();
            event.setError(throwable);
        }

        log.error("[ Sequence: " + sequence + " ] Exception occurred while processing inbound events." +
                eventType, throwable);
    }

    @Override
    public void handleOnStartException(Throwable throwable) {
        log.error("Error while starting Disruptor ", throwable);
    }

    @Override
    public void handleOnShutdownException(Throwable throwable) {
        log.error("Error while shutting down Disruptor ", throwable);
    }
}
