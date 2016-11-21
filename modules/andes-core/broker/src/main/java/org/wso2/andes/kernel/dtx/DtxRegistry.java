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

package org.wso2.andes.kernel.dtx;

import java.util.HashMap;
import java.util.Map;
import javax.transaction.xa.Xid;

public class DtxRegistry {
    private final Map<ComparableXid, DtxBranch> branches = new HashMap<ComparableXid, DtxBranch>();

    public synchronized DtxBranch getBranch(Xid xid) {
        return branches.get(new ComparableXid(xid));
    }

    public synchronized boolean registerBranch(DtxBranch branch)
    {
        ComparableXid xid = new ComparableXid(branch.getXid());
        if(!branches.containsKey(xid))
        {
            branches.put(xid, branch);
            return true;
        }
        return false;
    }

    private static final class ComparableXid {
        private final Xid xid;

        private ComparableXid(Xid xid) {
            this.xid = xid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ComparableXid that = (ComparableXid) o;

            return compareBytes(xid.getBranchQualifier(), that.xid.getBranchQualifier()) && compareBytes(
                    xid.getGlobalTransactionId(), that.xid.getGlobalTransactionId());
        }

        private static boolean compareBytes(byte[] a, byte[] b) {
            if (a.length != b.length) {
                return false;
            }
            for (int i = 0; i < a.length; i++) {
                if (a[i] != b[i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = 0;
            for (int i = 0; i < xid.getGlobalTransactionId().length; i++) {
                result = 31 * result + (int) xid.getGlobalTransactionId()[i];
            }
            for (int i = 0; i < xid.getBranchQualifier().length; i++) {
                result = 31 * result + (int) xid.getBranchQualifier()[i];
            }

            return result;
        }
    }
}
