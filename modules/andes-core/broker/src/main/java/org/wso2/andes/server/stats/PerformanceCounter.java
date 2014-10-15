/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.server.stats;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

public class PerformanceCounter {
    private static final Logger log = Logger.getLogger(PerformanceCounter.class);
    private static final int MSG_BUFFER_SIZE = 1000; 
    
    private  static AtomicLong totMessagesReceived = new AtomicLong();
    private  static AtomicLong totmessagesDelivered = new AtomicLong();
    
    private static AtomicLong queueDeliveryCount =  new AtomicLong();
    private static AtomicLong queueReceiveCount =  new AtomicLong();
    private static AtomicLong queueGlobalQueueMoveCount =  new AtomicLong();
    private static AtomicLong pendingMessagesToBeWrittenToCassandra = new AtomicLong();
    
    private static long startTime = -1; 
    private static AtomicLong ackLatency = new AtomicLong();
    
    private static long lastEmitTs = System.currentTimeMillis();

    
    static{
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    try {
                        Thread.sleep(30000);
                        
                    } catch (InterruptedException e) {
                    }
                    
                    long timeTookSinceEmit =  System.currentTimeMillis() - lastEmitTs; 
                    float deliverythroughput = queueDeliveryCount.get()*1000/timeTookSinceEmit; 
                    queueDeliveryCount.set(0);
                    
                    float receivethroughput = queueReceiveCount.get()*1000/timeTookSinceEmit; 
                    queueReceiveCount.set(0);
                    
                    float globalQueueMovethroughput = queueGlobalQueueMoveCount.get()*1000/timeTookSinceEmit; 
                    queueGlobalQueueMoveCount.set(0);


                    if(queueReceiveCount.get() > 0){
                        log.info("PerfCount: summary ["+new Date()+"] deliveryThoughput = "+deliverythroughput +", receiveThoughput="+ receivethroughput 
                                +", qquaueMoveT="+globalQueueMovethroughput+" delivered=" + totmessagesDelivered + ", received " + totMessagesReceived );                    
                    }
                    lastEmitTs = System.currentTimeMillis(); 
                }
            }
        }).start();
    }


    public static class QueuePerfData{
        AtomicInteger messagesReceived = new AtomicInteger();
        AtomicInteger messagesDelivered = new AtomicInteger(); 
        
        AtomicInteger[] msgSizes = new AtomicInteger[10];
        
        public QueuePerfData(){
            for(int i = 0;i< msgSizes.length;i++){
                msgSizes[i] = new AtomicInteger();
            }
        }
    }
    
    
    private static ConcurrentHashMap< String, QueuePerfData> perfMap = new ConcurrentHashMap<String, PerformanceCounter.QueuePerfData>(); 
    
    public static void recordMessageReceived(String qName, long sizeInChuncks){
        totMessagesReceived.incrementAndGet();
        long pendingMessageCount = pendingMessagesToBeWrittenToCassandra.incrementAndGet();
        if((pendingMessageCount > 10000) && (pendingMessageCount % 1000 == 0)){
           log.warn("Pending " + pendingMessageCount + " messages to be written to cassandra");
        }
        QueuePerfData perfData = perfMap.get(qName);
        
        synchronized (perfMap) {
            if(perfData == null){
                perfData = new QueuePerfData(); 
                perfMap.put(qName, perfData); 
            }
        }
        int count = perfData.messagesReceived.incrementAndGet();
        
        if(log.isDebugEnabled()){
            if(count%MSG_BUFFER_SIZE == 0){
                log.debug("PerfCount:" + qName + ":" + perfData.messagesDelivered + "/" + perfData.messagesReceived + " "+ Arrays.toString(perfData.msgSizes) + ", tot=" + totmessagesDelivered + "/" + totMessagesReceived );
            }
        }
        int index = 0;
        if(sizeInChuncks != 0) {
            index = (int)Math.min(Math.ceil(Math.log10(sizeInChuncks)),10);
        }
        perfData.msgSizes[index].incrementAndGet(); 
        queueReceiveCount.incrementAndGet();
        messageReceivedCounter.notifyEvent(false); 

    }        
        
//    public static void recordMessageReceived(){
//    }
    
//    public static void recordIncomingMessageSubmittedToCassandra(){
//        messageSubmittedToCassandraCounter.notifyEvent(false);
//    }
    
    public static void recordIncomingMessageWrittenToCassandraLatency(int latency){
        messageWrittenToCassandraLatencyCounter.notifyEvent(latency, false); 
    }
    
    public static void recordIncomingMessageWrittenToStore(){
        pendingMessagesToBeWrittenToCassandra.decrementAndGet();
        boolean log = messageWrittenToCassandraCounter.notifyEvent(false);
        if(log){
            printLine("MsgRecived", messageReceivedCounter, null);
            printLine("MsgWritten2Store", messageWrittenToCassandraCounter, messageWrittenToCassandraLatencyCounter);
        }
    }

    
    public static void recordAckReceived(String qName, int time2Ack){
        totmessagesDelivered.incrementAndGet();
        QueuePerfData perfData = perfMap.get(qName); 
        
        synchronized (perfMap) {
            if(perfData == null){
                perfData = new QueuePerfData(); 
                perfMap.put(qName, perfData); 
            }
        }
        int count = perfData.messagesDelivered.incrementAndGet();

        if(log.isDebugEnabled()){
            if(count%MSG_BUFFER_SIZE == 0){
                log.debug("PerfCount" + qName + ":" + perfData.messagesDelivered + "/" + perfData.messagesReceived + " "+ Arrays.toString(perfData.msgSizes) + ", tot=" + totmessagesDelivered + "/" + totMessagesReceived );
            }
        }
        
        long ackCount = queueDeliveryCount.incrementAndGet();
        ackLatency.addAndGet(time2Ack);
        if(ackCount%1000 == 0){
            if(startTime == -1){
                startTime = System.currentTimeMillis();
            }else{
                log.info("[mbperf] Acks "+ackCount+" Received, Submit Throughput =" + (int)(queueDeliveryCount.get()*1000d/(System.currentTimeMillis() - startTime)) + ", avg latency ="+ ackLatency.get()/queueDeliveryCount.get());
            }
        }
        ackReceivedCounter.notifyEvent(false);
        ackReceivedLatencyCounter.notifyEvent(time2Ack, false);
    }
    
    public static void recordMessageRemovedAfterAckLatency(int latency){
        messageRemovedAfterAckLatencyCounter.notifyEvent(latency, false);
    }

    
    public static void recordMessageRemovedAfterAck(){
        if(messageRemovedAfterAckCounter.notifyEvent(false)){
            printLine("Sent2Consumer", messageSentToConsumerCounter, null);
            printLine("AckRecived", ackReceivedCounter, ackReceivedLatencyCounter);
            printLine("AckHandled", messageRemovedAfterAckCounter, messageRemovedAfterAckLatencyCounter);
        }
        
    }
    
    
    public static void printLine(String label, ThroughputCounter tc, LatencyCounter lc ){
        long count = tc.count.get(); 
        if(lc == null){
            log.info("[mbperf1]["+count+"]"+label+"=(T=" + tc.getThroughput() + "("+tc.getLast1000Throughput()+")");
        }else{
             log.info("[mbperf1]["+count+"]"+label+"=(T=" + tc.getThroughput() + "("+tc.getLast1000Throughput()+"), L="+ lc.getLatency() + Arrays.toString(lc.getHistorgram()));
        }
    }
    

    
    public static void recordGlobalQueueMsgMove(int messagesMoved){
        queueGlobalQueueMoveCount.addAndGet(messagesMoved);
    }

    
    public static void recordMessageSentToConsumer(){
        messageSentToConsumerCounter.notifyEvent(false);
    }
    
    
    public static void recordErrorHappend(String error){
    }


    
    public static void recordCassandraRead(long timeTookInMillis){
    }
    
    
    private static ThroughputCounter messageReceivedCounter = new ThroughputCounter(MSG_BUFFER_SIZE, "messageReceived");
    //private static ThroughputCounter messageSubmittedToCassandraCounter = new ThroughputCounter(MSG_BUFFER_SIZE, "messageSubmittedToCassandra");

    private static ThroughputCounter messageWrittenToCassandraCounter = new ThroughputCounter(MSG_BUFFER_SIZE, "messageWrittenToCassandra");
    private static LatencyCounter messageWrittenToCassandraLatencyCounter = new LatencyCounter(MSG_BUFFER_SIZE, "messageWrittenToCassandraLatency");

    private static ThroughputCounter messageSentToConsumerCounter = new ThroughputCounter(MSG_BUFFER_SIZE, "messageSentToConsumer");
    private static ThroughputCounter ackReceivedCounter = new ThroughputCounter(MSG_BUFFER_SIZE, "ackReceived");
    private static LatencyCounter ackReceivedLatencyCounter = new LatencyCounter(MSG_BUFFER_SIZE, "ackReceivedLatency");
    private static ThroughputCounter messageRemovedAfterAckCounter = new ThroughputCounter(MSG_BUFFER_SIZE, "messageRemovedAfterAck");
    private static LatencyCounter messageRemovedAfterAckLatencyCounter = new LatencyCounter(MSG_BUFFER_SIZE, "messageRemovedAfterAckLatency");

    
    
    public static class ThroughputCounter{
        AtomicLong count = new AtomicLong(); 
        long lastTime = 0;
        long startTime = -1; 
        private final int messagesBetweenTwoEmits;
        private final String label; 
        private int last1000Throughput = 0; 
        private int lastThroughput = 0; 
        
        public ThroughputCounter(int messagesBetweenTwoEmits, String label){
            this.messagesBetweenTwoEmits = messagesBetweenTwoEmits; 
            this.label = label;
        }
        
        public boolean notifyEvent(boolean print){
            long currentCount = this.count.incrementAndGet();
            if(currentCount%messagesBetweenTwoEmits ==  0){
                if(startTime == -  1){
                    startTime = System.currentTimeMillis();
                    return false;
                }
                if(print){
                    log.info("[mbperf1]"+label+"> throughput"+ getThroughput() + " messageCount =" + count.get());
                }
                last1000Throughput = (int)(messagesBetweenTwoEmits*1000d/(System.currentTimeMillis() - lastTime));
                lastTime = System.currentTimeMillis();
                lastThroughput = (int)(count.get()*1000d/(System.currentTimeMillis() - startTime));
                return true;
            }
            
            
            
            
            if(currentCount == Long.MAX_VALUE){
                count.set(0);
                startTime = System.currentTimeMillis();
            }
            return false;
        }
        
        public int getLast1000Throughput(){
            return last1000Throughput;
        }
        
        public int getThroughput(){
            return lastThroughput;
        }

    }
    
    public static class LatencyCounter{
        private AtomicLong count = new AtomicLong(); 
        private AtomicLong total = new AtomicLong(); 
        private long startTime = -1; 
        private final int messagesBetweenTwoEmits;
        private final String label; 
        private float lastLatency = 0; 
        
        private long[] latencyHistogram = new long[10];
        
        public LatencyCounter(int messagesBetweenTwoEmits, String label){
            this.messagesBetweenTwoEmits = messagesBetweenTwoEmits; 
            this.label = label;
        }
        
        public boolean notifyEvent(int latency, boolean print){
            total.addAndGet(latency);
            long currentCount = this.count.incrementAndGet();
            if(currentCount%messagesBetweenTwoEmits ==  0){
                if(startTime == -  1){
                    startTime = System.currentTimeMillis();
                }
                if(print){
                    log.info("[mbperf1]"+label+"> avg Latency"+ getLatency() + " messageCount =" + count.get());
                }
                lastLatency = (int)total.get()/count.get();
                return true;
            }
            
            if(total.get() == Long.MAX_VALUE){
                count.set(0);
                startTime = System.currentTimeMillis();
                latencyHistogram = new long[6];
            }
            
            int index = 0; 
            if(latency < 1){
                index = 0;
            }else if(latency < 10){
                index = 1; 
            }else{
                index = (int)Math.ceil(Math.log10(latency));
            }
            latencyHistogram[index]++;
            return false; 
        }
        
        public long[] getHistorgram(){
            return latencyHistogram;
        }
        
        public float getLatency(){
            return lastLatency;
        }
    }

    
}
