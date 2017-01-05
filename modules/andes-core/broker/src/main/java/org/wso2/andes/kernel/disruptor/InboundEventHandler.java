/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel.disruptor;

import com.lmax.disruptor.EventHandler;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer;

/**
 * Abstract class handling inbound events. This is to make each inbound event handler stem from the same event
 * handler class without using generics. Avoid generic array type error when creating an array consisting of different
 * types of inbound event handlers in {@link org.wso2.andes.kernel.disruptor.inbound.InboundEventManager}
 */
public abstract class InboundEventHandler implements EventHandler<InboundEventContainer> {

}
