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

package org.wso2.andes.kernel.distrupter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;

import com.lmax.disruptor.EventHandler;
import org.wso2.andes.server.slot.SlotMessageCounter;

/**
 * We do this to make Listener take turns while running. So we can run many copies of these and control number
 * of IO threads through that.
 */
public class AlternatingCassandraWriter implements EventHandler<CassandraDataEvent> {

    private static Log log = LogFactory.getLog(AlternatingCassandraWriter.class);
    int count = 0;
    int totalPendingEventLength = 0;
    private int writerCount;
    private int turn;
    private MessageStoreManager messageStoreManager;
    private List<AndesMessageMetadata> metaList = new ArrayList<AndesMessageMetadata>();

    private List<AndesMessagePart> partList = new ArrayList<AndesMessagePart>();
    private static ExecutorService metadataExecutor = Executors.newFixedThreadPool(1);


    private long startTime = 0L;
    private AtomicInteger counter = new AtomicInteger(0);
    /**
     * Maximum data length for a single write to data base
     */
    private static final int MAX_DATA_LENGTH = 128000;

    public AlternatingCassandraWriter(int writerCount, int turn, MessageStoreManager messageStoreManager) {
        this.writerCount = writerCount;
        this.turn = turn;
        this.messageStoreManager = messageStoreManager;
    }

    public void onEvent(final CassandraDataEvent event, final long sequence, final boolean endOfBatch) throws Exception {
        if (event.isPart) {
            //if part, we write randomly
           int calculatedTurn = (int) Math.abs(event.part.getMessageID() % writerCount);

            if (calculatedTurn == turn) {
                //Message parts we write on the fly. It is tradeoff of memory vs. batching
                //May be we need better handling .. batch that data as well
                partList.add(event.part);
                totalPendingEventLength += event.part.getDataLength();

                count = counter.incrementAndGet();
                if(count == 1){
                    startTime = System.currentTimeMillis();
                }
           }
        } else {


            //if messageID, we write in sequence per queue
            int calculatedTurn = Math.abs(event.metadata.getDestination().hashCode() %
                   writerCount);

           if (calculatedTurn == turn) {
                metaList.add(event.metadata);
                totalPendingEventLength += event.metadata.getMetadata().length;
               count = counter.incrementAndGet();
               if(count == 1){
                   startTime = System.currentTimeMillis();
               }
           }
        }

        if (totalPendingEventLength > MAX_DATA_LENGTH || (endOfBatch)) {
            // Write message part list to database
//            if (partList.size() > 0) {
//                if(log.isDebugEnabled()){
//                    log.debug("Number of message content sent to message store: " + partList.size
//                            ());
//                }
//                long startTime = System.currentTimeMillis();
//                messageStoreManager.storeMessagePart(partList);
//                log.info("@@@@@@@@@@@@@@@@@@ latency message parts: " + (System.currentTimeMillis
//                        () - startTime));
//                partList.clear();
//            }

            // Write message meta list to cassandra
            if (metaList.size() > 0) {
                if(log.isDebugEnabled()){
                    log.debug("Number of message metadata sent to message store: " + partList.size
                            () );
                }
//                if ((count>1000)) {
//                    double throughput = (count * 1000)/ (System.currentTimeMillis() - startTime);
//                    log.info("@@@@@@@@@@@@@@@@@@@@@@@ throughput = " +
//                            throughput);
//                }
                long startTime = System.currentTimeMillis();

                messageStoreManager.storeMetaData(metaList);
                log.info("@@@@@@@@@@@@@@@@@@ latency message metadata: " + (System
                        .currentTimeMillis
                        () - startTime));

//                final List<AndesMessageMetadata> mdatalist = metaList;
//                metadataExecutor.submit(new Runnable() {
//                    @Override
//                    public void run() {
//                        try {
//                            messageStoreManager.storeMetaData(mdatalist);
//                            if (AndesContext.getInstance().isClusteringEnabled()) {
//                                SlotMessageCounter.getInstance().recordMetaDataCountInSlot(metaList);
//                            }
//                        } catch (AndesException e) {
//                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                        }
//                    }
//
//                });

                //record message data count
                if (AndesContext.getInstance().isClusteringEnabled()) {
                    SlotMessageCounter.getInstance().recordMetaDataCountInSlot(metaList);
                }
                metaList = new ArrayList<AndesMessageMetadata>();
            }

            totalPendingEventLength = 0;
        }
    }
}
