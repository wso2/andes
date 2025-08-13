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
package org.wso2.andes.server.state;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQMethodBody;
import org.wso2.andes.protocol.AMQMethodEvent;

/**
 * A frame listener that is informed of the protocol state when invoked and has
 * the opportunity to update state.
 *
 */
public interface StateAwareMethodListener<B extends AMQMethodBody>
{
    void methodReceived(AMQStateManager stateManager,  B evt, int channelId) throws AMQException;
}
