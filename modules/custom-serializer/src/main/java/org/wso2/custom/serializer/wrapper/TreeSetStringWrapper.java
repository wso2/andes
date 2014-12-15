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

package org.wso2.custom.serializer.wrapper;

import java.io.Serializable;
import java.util.TreeSet;

/**
 * This class is a wrapper class to a String TreeSet.It encapsulates a Treeset in order to
 * customize the serialization of a long treeset in hazelcast. This class is used because general
 * TreeSet class should not affect with the custom serialization.
 */
public class TreeSetStringWrapper implements Serializable {

    private TreeSet<String> stringTreeSet = new TreeSet<String>();

    public TreeSet<String> getStringTreeSet() {
        return stringTreeSet;
    }

    public void setStringTreeSet(TreeSet<String> stringTreeSet) {
        this.stringTreeSet = stringTreeSet;
    }
}
