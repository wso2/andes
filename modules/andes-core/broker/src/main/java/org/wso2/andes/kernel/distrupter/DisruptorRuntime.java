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

package org.wso2.andes.kernel.distrupter;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DisruptorRuntime<T> {
    private ExecutorService executorPool = Executors.newCachedThreadPool();
    private Disruptor<T> disruptor;
    private final RingBuffer<T> ringBuffer;

    // TODO : Multiple disruptor runtime for seq and mul
    public DisruptorRuntime(EventFactory<T> eventFactory, EventHandler<T>[] handlers) {
        disruptor = new Disruptor<T>(eventFactory, executorPool,
                new MultiThreadedLowContentionClaimStrategy(65536), // this is for multiple publishers
                new BlockingWaitStrategy());
// Using Blocking wait strategy over Sleeping wait strategy to over come CPU load issue happening when starting up the sever. With this it will
// to resolve the issue https://wso2.org/jira/browse/MB-372
//                new SleepingWaitStrategy());

        //cannot solve the warrning http://stackoverflow.com/questions/1445233/is-it-possible-to-solve-the-a-generic-array-of-t-is-created-for-a-varargs-param
        disruptor.handleEventsWith(handlers);
        ringBuffer = disruptor.start();
    }

    public RingBuffer<T> getRingBuffer() {
        return ringBuffer;
    }
}
