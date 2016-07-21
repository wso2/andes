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

package org.wso2.andes.kernel.subscription;

import org.wso2.andes.kernel.AndesException;

/**
 * Exception thrown when dealing with subscriber related information
 */
public class SubscriptionException extends AndesException {

    public SubscriptionException() {
    }

    /**
     * Generate SubscriptionException
     *
     * @param message exception message
     */
    public SubscriptionException(String message) {
        super(message);
    }

    /**
     * Generate SubscriptionException
     *
     * @param message exception message
     * @param cause   underlying cause
     */
    public SubscriptionException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Generate SubscriptionException
     *
     * @param cause underlying cause
     */
    public SubscriptionException(Throwable cause) {
        super(cause);
    }
}
