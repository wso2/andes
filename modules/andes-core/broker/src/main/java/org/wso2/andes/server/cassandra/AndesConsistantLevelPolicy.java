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
package org.wso2.andes.server.cassandra;

import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;

public class AndesConsistantLevelPolicy implements ConsistencyLevelPolicy {
  @Override
	public HConsistencyLevel get(OperationType op) {

		switch (op){
	      case READ:return HConsistencyLevel.QUORUM;
	      case WRITE: return HConsistencyLevel.QUORUM;
	      default: return HConsistencyLevel.QUORUM; //Just in Case
	   }
	}

	@Override
	public HConsistencyLevel get(OperationType arg0, String arg1) {

		return HConsistencyLevel.QUORUM;
	}
}
