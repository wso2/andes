package org.wso2.andes.store.cassandra;

/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Slot mapping recovery has to deal with cassandra tombstone when server startup. This class has utility method used to
 * recovery process.
 *
 */
public class ServerStartupRecoveryUtils {

    private static Logger log = LoggerFactory.getLogger(ServerStartupRecoveryUtils.class);

    /**
     * Return slot recovery completion message id to stop seeking through cassandra database
     *
     * @return id of the message used to stop slot recovery task
     */
    public static long getMessageIdToCompleteRecovery() {
        long REFERENCE_START = 41L * 365L * 24L * 60L * 60L * 1000L;
        long ts = System.currentTimeMillis();
        return (ts - REFERENCE_START) * 256 * 1024;
    }

    /**
     * INFO log print to inform user while reading tombstone
     *
     * @param timer TimerTask to schedule printing logs
     */
    public static void readingCassandraInfoLog(Timer timer) {
        long printDelay = 30000L;
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                log.info("Reading data from cassandra.");
            }
        }, 0, printDelay);

    }
}
