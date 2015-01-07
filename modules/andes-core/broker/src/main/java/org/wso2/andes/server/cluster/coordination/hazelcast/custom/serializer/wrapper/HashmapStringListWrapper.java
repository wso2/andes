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

package org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper;


import org.wso2.andes.server.slot.Slot;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * This class is a wrapper class to a HashMap with String and Slot List.It encapsulates a
 * hashmap in order to
 * customize
 * the serialization of a HashMap<String,List<Slot>> in hazelcast. This class is used because
 * general HashMap<T,V>
 * class should not affect with the custom serialization.
 */
public class HashmapStringListWrapper implements Serializable {

    private HashMap<String, List<Slot>>  stringListHashMap;


    public HashMap<String, List<Slot>> getStringListHashMap() {
        return stringListHashMap;
    }

    public void setStringListHashMap(HashMap<String, List<Slot>> stringListHashMap) {
        this.stringListHashMap = stringListHashMap;
    }
}
