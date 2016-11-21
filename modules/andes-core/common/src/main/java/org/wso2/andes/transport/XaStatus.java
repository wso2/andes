/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.transport;

public enum XaStatus {
    XA_OK(0),
    XA_RBROLLBACK(1),
    XA_RBTIMEOUT(2),
    XA_HEURHAZ(3),
    XA_HEURCOM(4),
    XA_HEURRB(5),
    XA_HEURMIX(6),
    XA_RDONLY(7);

    private final int value;

    XaStatus(int value) {
        this.value = value;
    }

    public static XaStatus valueOf(int value) {
        switch (value) {
        case 0:
            return XA_OK;
        case 1:
            return XA_RBROLLBACK;
        case 2:
            return XA_RBTIMEOUT;
        case 3:
            return XA_HEURHAZ;
        case 4:
            return XA_HEURCOM;
        case 5:
            return XA_HEURRB;
        case 6:
            return XA_HEURMIX;
        case 7:
            return XA_RDONLY;
        default:
            throw new IllegalArgumentException("no such value: " + value);
        }
    }

    public int getValue() {
        return value;
    }
}
