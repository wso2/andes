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

package org.wso2.andes.kernel.registry;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.router.AndesMessageRouter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the message router registry. All generic exchanges
 * are registered here.
 */
public class MessageRouterRegistry {

    private Map<String, AndesMessageRouter> messageRouterMap;

    /**
     * Create a in-memory registry for keeping message routers in broker
     */
    public MessageRouterRegistry() {
        messageRouterMap = new HashMap<>();
    }

    /**
     * Register a AndesMessageRouter. Registration will not happen if registry already
     * has a registered router in same name
     *
     * @param routerName name of the router
     * @param router     message router impl
     * @throws AndesException
     */
    public void registerMessageRouter(String routerName, AndesMessageRouter router) throws AndesException {
        AndesMessageRouter messageRouter = getMessageRouter(routerName);
        if (null == messageRouter) {
            messageRouterMap.put(routerName, router);
        }
    }

    /**
     * Remove a message router from registry
     *
     * @param routerName name of the message router
     * @return removed AndesMessageRouter instance. Null if no router is removed.
     */
    public AndesMessageRouter removeMessageRouter(String routerName) {
        return messageRouterMap.remove(routerName);
    }

    /**
     * Get message router by name
     *
     * @param routerName AndesMessageRouter registered to the name
     * @return Registered router instance
     */
    public AndesMessageRouter getMessageRouter(String routerName) {
        return messageRouterMap.get(routerName);
    }

    /**
     * Get all message routers registered
     *
     * @return a list of AndesMessageRouters registered in broker
     */
    public List<AndesMessageRouter> getAllMessageRouters() {
        return new ArrayList<>(messageRouterMap.values());
    }
}
