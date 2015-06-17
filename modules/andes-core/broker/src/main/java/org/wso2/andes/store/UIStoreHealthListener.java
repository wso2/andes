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
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.andes.store;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * The following class contains a listener for MB store to identify whether its operational or not.
 */
public class UIStoreHealthListener implements StoreHealthListener {

    private static UIStoreHealthListener currentUIHealthListener;
    private final Logger log = Logger.getLogger(UIStoreHealthListener.class);

    /**
     * Variable to store the exception content.
     */
    private String exceptionStringValue = StringUtils.EMPTY;

    /**
     * Private constructor to support singleton pattern
     */
    private UIStoreHealthListener(){
    }

    /**
     * Gets the instance of the {@link org.wso2.andes.store.UIStoreHealthListener}.
     *
     * @return The instance.
     */
    public static UIStoreHealthListener getInstance() {
        if (null == currentUIHealthListener) {
            currentUIHealthListener = new UIStoreHealthListener();
            FailureObservingStoreManager.registerStoreHealthListener(currentUIHealthListener);
        }

        return currentUIHealthListener;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNonOperational(HealthAwareStore store, Exception exception) {
        exceptionStringValue = exception.toString();
        if(log.isDebugEnabled()){
            log.debug("Exception received on store health to show in UI : " + exception.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeOperational(HealthAwareStore store) {
        exceptionStringValue = StringUtils.EMPTY;
        if(log.isDebugEnabled()){
            log.debug("Notice received that store is operational to show in UI.");
        }
    }

    public String getExceptionStringValue() {
        return exceptionStringValue;
    }
}
