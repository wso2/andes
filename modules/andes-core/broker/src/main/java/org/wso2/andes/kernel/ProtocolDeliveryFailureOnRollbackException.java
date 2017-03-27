/*
 *  Copyright (c) 2017, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.wso2.andes.kernel;

/**
 * When sending messages during a JMS rollback event, we should not queue up any new messages to the subscriber
 * channel until we have completely re-queued all messages rejected by the subscriber side.
 * This exception will be thrown if any new messages are about to be re-queued in the middle of a rollback.
 */
public class ProtocolDeliveryFailureOnRollbackException extends AndesException {

    /***
     * Constructor
     * @param message descriptive message
     * @param errorCode one of the above defined constants that classifies the error.
     * @param cause reference to the exception for reference.
     */
    public ProtocolDeliveryFailureOnRollbackException(String message, String errorCode, Throwable cause){
        super(message, errorCode, cause);
    }

    /***
     * Constructor
     * @param message descriptive message
     * @param cause reference to the exception for reference.
     */
    public ProtocolDeliveryFailureOnRollbackException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor
     * @param message descriptive message
     */
    public ProtocolDeliveryFailureOnRollbackException(String message) {
        super(message);
    }
}
