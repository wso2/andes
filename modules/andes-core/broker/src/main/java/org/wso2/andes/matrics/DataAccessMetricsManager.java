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

package org.wso2.andes.matrics;

import org.wso2.andes.kernel.MessageStore;
import org.wso2.carbon.metrics.manager.Gauge;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.MetricManager;
import org.wso2.carbon.metrics.manager.Timer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Will be responsible in managing between the carbon metrics manager
 * Includes adding of timers and guages
 */
public class DataAccessMetricsManager {


    /*
     *Holds the timers which will periodically trigger the metrics info
     *Key - Timer definition, Value - timer object which will record info against the defined operation
     *Map will be concurrently accessed during multiple db operations during run time.
     * */
    private static Map<String, Timer> timers = new ConcurrentHashMap<String, Timer>();

    /*Sets the level in which the metrics should be handled the levels could be INFO, WARN, DEBUG, TRACE*/
    private static final Level level = Level.INFO;


    /**
     * Will create and register a timer if already not being created
     *
     * @param definition the definition of the operation the metrics to be created
     * @param source     the source class which the metrics data should be tabulated
     * @return the timer object which will be used for metrics calculation
     */
    public static Timer addAndGetTimer(String definition, MessageStore source) {

        Timer metricTimer = timers.get(definition);
        //If metrics has not being defined before
        if (null == metricTimer) {
            metricTimer = MetricManager.timer(level, MetricManager.name(source.getClass(), definition));
            //Will create the timer
            timers.put(definition, metricTimer);
        }

        return metricTimer;
    }

    /**
     * Will prepare a guage for tabulating value events
     *
     * @param definition the name guage should be registered
     * @param source     the source class which the metrics data should be tabulated
     * @param guage      the guage defined in the class for metrics calculation
     */
    public static void addGuage(String definition, Class<?> source, Gauge<?> guage) {
        MetricManager.gauge(Level.INFO, MetricManager.name(source,
                definition), guage);
    }


}
