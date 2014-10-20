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
package org.wso2.andes.qmf;

import java.util.List;

public abstract class QMFObjectClass<T extends QMFObject, S extends QMFObject.Delegate> extends QMFClass
{
    public QMFObjectClass(String name,
                          byte[] schemaHash,
                          List<QMFProperty> properties,
                          List<QMFStatistic> statistics, List<QMFMethod> methods)
    {
        super(QMFClass.Type.OBJECT, name, schemaHash, properties, statistics, methods);
    }

    public QMFObjectClass(String name, byte[] schemaHash)
    {
        super(QMFClass.Type.OBJECT, name, schemaHash);
    }


    public abstract T newInstance(S delegate);


}
