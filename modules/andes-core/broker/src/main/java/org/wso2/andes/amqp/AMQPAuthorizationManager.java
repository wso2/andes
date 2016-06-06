/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.andes.amqp;

import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.security.AuthorizeAction;
import org.wso2.andes.server.queue.AMQQueue;

import javax.security.auth.Subject;

/**
 * Manages authorization of QPID objects against Andes.
 */
public class AMQPAuthorizationManager {

    /**
     * Check if a specific action is authorized against a given queue.
     *
     * @param action The action which needs permission for
     * @param amqQueue The Queue
     * @param authorizedSubject The subject which is going to access the queue
     * @return True if authorized
     */
    public static boolean isAuthorized(AuthorizeAction action, AMQQueue amqQueue, Subject authorizedSubject) {
        return AndesContext.getInstance().getAndesAuthorizationManager().
                            isAuthorized(authorizedSubject, amqQueue.getResourceName(), action);
    }


}
