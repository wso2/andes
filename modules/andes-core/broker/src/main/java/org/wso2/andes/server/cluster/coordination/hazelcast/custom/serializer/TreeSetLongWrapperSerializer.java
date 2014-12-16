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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.TreeSetLongWrapper;

import java.io.IOException;

/**
 * This class implements the custom serialization methods for TreeSetLongWrapper objects.
 */
public class TreeSetLongWrapperSerializer implements StreamSerializer<TreeSetLongWrapper> {


    @Override
    public void write(ObjectDataOutput objectDataOutput, TreeSetLongWrapper treeSetLongWrapper)
            throws IOException {
        //Convert the TreeSetLongWrapper object to a json string and save it in hazelcast map
        com.google.gson.Gson gson = new GsonBuilder().create();
        String jsonString = gson.toJson(treeSetLongWrapper);
        objectDataOutput.writeUTF(jsonString);
    }

    @Override
    public TreeSetLongWrapper read(ObjectDataInput objectDataInput) throws IOException {
        //Build TreeSetLongWrapper object using json string.
        String jsonString = objectDataInput.readUTF();
        Gson gson = new GsonBuilder().create();
        TreeSetLongWrapper wrapper = gson.fromJson(jsonString, TreeSetLongWrapper.class);
        return wrapper;
    }

    @Override
    public int getTypeId() {
        return 1;
    }

    @Override
    public void destroy() {

    }
}
