/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel.slot;

import java.util.HashMap;
import java.util.Map;

public class SlotRangesEncorderDecoder {

    public static String encode(Slot slot){
        String rangesString = "";
        for(Map.Entry<Long, SlotRange> entry: slot.getNodeIdToRangeMap().entrySet()){
            rangesString = entry.getKey() + ":" + entry.getValue().getStartMessasgeId() + "-" + entry.getValue()
                    .getEndMessageId() + ",";
        }
        rangesString  = rangesString.substring(0, rangesString.length()-1);
        //node_Id:startID-endId,nodeId:start-endID
        return  rangesString;
    }


    public static Slot decode(String rangesString){
        Map<Long, SlotRange>  nodeIdToSlotRangeMap = new HashMap<>();
        String[] seperateQueues =  rangesString.split(",");
        for(String entry: seperateQueues){
             String[] splitWithColonString = entry.split(":");
             String nodeId = splitWithColonString[0];
            String startMessageId = splitWithColonString[1].split("-")[0];
            String endMessageId = splitWithColonString[1].split("-")[1];
            SlotRange slotRange = new SlotRange(Long.valueOf(startMessageId), Long.valueOf(endMessageId));
            nodeIdToSlotRangeMap.put(Long.valueOf(nodeId), slotRange);
        }
        Slot slot = new Slot(nodeIdToSlotRangeMap);
        return slot;
    }

}
