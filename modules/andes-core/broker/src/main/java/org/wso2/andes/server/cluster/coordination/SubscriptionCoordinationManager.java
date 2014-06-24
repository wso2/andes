/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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
package org.wso2.andes.server.cluster.coordination;

/**
 * Is the Listner Manager which manage the Subscription changes in Cluster node
 * It mainly take care of Managing Subscription Listeners and notify them in case of subscription changes in nodes
 * Subscription change : Subscription addition and removal
 */
public interface SubscriptionCoordinationManager {


    /**
     * Initialize the Subscription Coordination Manager
     * @throws CoordinationException in case of an Error in the initializing
     */
    public void init() throws CoordinationException;


    /**
     *Notify Subscription Listeners on subscription changes
     */
    public void notifySubscriptionChange();

    /**
     * Handle the subscription change event. Main Task of this method is to notify Subscription
     * listeners registered in all nodes about the subscription change in this node
     * @throws CoordinationException
     */
    public void handleSubscriptionChange() throws CoordinationException;


    /**
     * Register a Subscription Listener for Subscription changes. This is not a one time subscription
     * Subscriber will get notified about changes it will remove it from the  SubscriptionCoordinationManager
     * @param listener Subscription Listener implementation which will handle the subscription changes
     */
    public void registerSubscriptionListener(SubscriptionListener listener);

    /**
     * Remove Subscription Listener from SubscriptionCoordinationManager so that it will no longer notified with
     * Subscription changes
     * @param listener Subscription Listener implementation which handle the subscription changes
     */
    public void removeSubscriptionListener(SubscriptionListener listener);
}
