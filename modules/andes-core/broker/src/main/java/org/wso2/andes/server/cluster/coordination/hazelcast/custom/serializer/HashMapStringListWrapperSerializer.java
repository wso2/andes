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

package org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer;

import com.google.gson.*;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.HashmapStringListWrapper;
import org.wso2.andes.server.slot.Slot;


import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

public class HashMapStringListWrapperSerializer implements
        StreamSerializer<HashmapStringListWrapper> {


    @Override
    public void write(ObjectDataOutput objectDataOutput, HashmapStringListWrapper hashmapStringListWrapper) throws IOException {
        //Convert the hashmapStringListWrapper object to a json string and save it in hazelcast map
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{}");
        HashMap<String, List<Slot>> hashmap = hashmapStringListWrapper.getStringListHashMap();
        if (hashmap != null) {
            stringBuilder.insert(1, "\"stringListHashMap\":{}");
            Set<Map.Entry<String, List<Slot>>> entrySet = hashmap.entrySet();
            for (Map.Entry entry : entrySet) {
                stringBuilder.insert(stringBuilder.length() - 2, "\"" + entry.getKey() + "\":[],");
                List<Slot> slots = (List<Slot>) entry.getValue();
                if (slots != null) {
                    for (Slot slot : slots) {
                        String isActiveString;
                        if (slot.isSlotActive()) {
                            isActiveString = "true";
                        } else {
                            isActiveString = "false";
                        }
                        stringBuilder.insert(stringBuilder.length() - 4,
                                "{\"messageCount\":" + slot.getMessageCount() + "," +
                                        "\"startMessageId\":" + slot.getStartMessageId() + "," +
                                        "\"endMessageId\":" + slot.getEndMessageId()
                                        + ",\"storageQueueName\":\"" + slot.getStorageQueueName()
                                        + "\",\"isSlotActive\":" + isActiveString + "},");
                    }
                    if (slots.size() != 0) {
                        stringBuilder.deleteCharAt(stringBuilder.length() - 5);
                    }
                }
            }
            if (hashmap.keySet().size() != 0) {
                stringBuilder.deleteCharAt(stringBuilder.length() - 3);
            }

        }

        objectDataOutput.writeUTF(stringBuilder.toString());
    }

    @Override
    public HashmapStringListWrapper read(ObjectDataInput objectDataInput) throws IOException {
        //Build HashmapStringListWrapper object using json string.
        String jsonString = objectDataInput.readUTF();
        HashmapStringListWrapper wrapper = new HashmapStringListWrapper();
        HashMap<String, List<Slot>> hashMap = new HashMap();
        JsonObject jsonObject = new JsonParser().parse(jsonString).getAsJsonObject()
                .getAsJsonObject("stringListHashMap");
        Set<Map.Entry<String, JsonElement>> set = jsonObject.entrySet();
        Iterator<Map.Entry<String, JsonElement>> iterator = set.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonElement> entry = iterator.next();
            String key = entry.getKey();
            JsonElement value = entry.getValue();
            List<Slot> arrayList = new ArrayList<Slot>();
            JsonArray jsonArray = value.getAsJsonArray();
            for (JsonElement elem : jsonArray) {
                Slot slot = new Slot();
                JsonObject jsonObjectForSlot = (JsonObject) elem;
                slot.setMessageCount(jsonObjectForSlot.get("messageCount").getAsInt());
                slot.setStartMessageId(jsonObjectForSlot.get("startMessageId").getAsLong());
                slot.setEndMessageId(jsonObjectForSlot.get("endMessageId").getAsLong());
                slot.setStorageQueueName(jsonObjectForSlot.get("storageQueueName").getAsString());
                if (!jsonObjectForSlot.get("isSlotActive").getAsBoolean()) {
                    slot.setSlotInActive();
                }
                arrayList.add(slot);
            }
            hashMap.put(key, arrayList);
        }
        wrapper.setStringListHashMap(hashMap);
        return wrapper;
    }

    @Override
    public int getTypeId() {
        return 3;
    }

    @Override
    public void destroy() {

    }
}
