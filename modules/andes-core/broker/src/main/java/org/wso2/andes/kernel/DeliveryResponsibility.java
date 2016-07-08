/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
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

package org.wso2.andes.kernel;


import org.wso2.andes.subscription.LocalSubscription;

/**
 * This interface have methods to handle the delivery request
 */
public interface DeliveryResponsibility {

    /**
     * set the next delivery responsibility
     * @param deliveryResponsibility
     */
    public void setNextDeliveryFilter(DeliveryResponsibility deliveryResponsibility);

    /**
     * handle the messageDelivery request based on the assigned responsibility
     * @param subscription
     * @param message
     */
    public void handleDeliveryMessage(LocalSubscription subscription, DeliverableAndesMetadata message) throws AndesException;


}
