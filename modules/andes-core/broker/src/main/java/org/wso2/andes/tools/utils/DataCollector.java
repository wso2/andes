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
package org.wso2.andes.tools.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * <code>DataCollector</code> is a class that can be used inside broker to
 * Collect statistics.
 */
@SuppressWarnings("unused")
public class DataCollector {

    public static final String DATE_FORMAT_NOW = "yyyy-MM-dd-HH:mm:ss";


    public static final boolean enable = false;

    public static final String PUBLISHER_WRITE_LATENCY = "PbWL";
    public static final String TRANSFER_READ_LATENCY = "TrRL";
    public static final String TRANSFER_MOVE_LATENCY = "TrML";
    public static final String DELIVERY_SEND_LATENCY = "DlSL";
    public static final String DELIVERY_READ_LATENCY = "DlRL";
    public static final String DELIVERY_ACK_LATENCY = "DAckL";

    public static final String ADD_MESSAGE_CONTENT = "AddCNT";
    public static final String READ_MESSAGE_CONTENT = "ReadCNT";


    public static final String TRANSFER_QUEUE_WORKER_UTILISATION = "TrQWU";
    public static final String DELIVERY_QUEUE_WORKER_UTILISATION = "DlQWU";

    static {
        try {
            if (enable) {

                long currentTimeMills = System.currentTimeMillis();
                SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
                Calendar cal = Calendar.getInstance();
                OUT = new BufferedWriter(new FileWriter("perfdata_broker" + System.currentTimeMillis() + "_" +
                        sdf.format(cal.getTime()) + ".log"));
            }

        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    private static Writer OUT;


    /**
     * Write the give values as a new line to the file
     *
     * @param value value to write
     */
    public static void write(String value) {

        try {
            if (!enable) {
                return;
            }
            if (OUT != null) {
                OUT.write(value);
                OUT.write("\n");
            }
        } catch (IOException e) {

            e.printStackTrace();
        }
    }

    /**
     * Log the time of an event with a interested value
     *
     * @param key   key to identify event
     * @param time  time of the event
     * @param value values associated with the event
     */
    public static void write(Object key, double time, double value) {
        if (!enable) {
            return;
        }
        DataCollector.write(new StringBuffer().append("(").append(key.toString()).append(",")
                .append(time).append(",").append(value).append(")").toString());
    }


    /**
     * Long an event with the time difference between current time and the given time
     *
     * @param key  event key
     * @param time given time
     */
    public static void write(Object key, double time) {
        if (!enable) {
            return;
        }
        DataCollector.write(new StringBuffer().append("(").append(key.toString()).append(",")
                .append(time).append(",").append(")").toString());
    }

    /**
     * Flush data to the file
     */
    public static void flush() {
        if (!enable) {
            return;
        }
        try {
            if(OUT != null) {
                OUT.flush();
            }
        } catch (IOException e) {

            e.printStackTrace();
        }
    }


}
