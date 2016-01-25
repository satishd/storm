/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.trident.windowing.strategy;

import org.apache.storm.trident.windowing.config.WindowConfig;

/**
 *
 */
//todo-sato add unit test for this factory
public final class WindowStrategyFactory {

    public WindowStrategy create(WindowConfig windowConfig) {
        WindowStrategy windowStrategy = null;
        WindowConfig.Type windowType = windowConfig.getWindowType();
        switch(windowType) {
            case SLIDING_COUNT:
                windowStrategy = new SlidingCountWindowStrategy(windowConfig);
                break;
            case TUMBLING_COUNT:
                windowStrategy = new TumblingCountWindowStrategy(windowConfig);
                break;
            case SLIDING_DURATION:
                windowStrategy = new SlidingDurationWindowStrategy(windowConfig);
                break;
            case TUMBLING_DURATION:
                windowStrategy = new TumblingDurationWindowStrategy(windowConfig);
                break;
            default:
                throw new IllegalArgumentException("Given WindowConfig of type "+windowType+" is not supported");
        }

        return windowStrategy;
    }
}
