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
package org.apache.storm.trident.windowing.config;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.windowing.strategy.TumblingCountWindowStrategy;
import org.apache.storm.trident.windowing.strategy.WindowStrategy;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.windowing.WindowInfo;

import java.util.List;

/**
 * Represents tumbling count window configuration. Window tumbles at each given {@code windowLength} count of events.
 */
public final class TumblingCountWindow extends SlidingWindowConfig {

    private TumblingCountWindow(int windowLength) {
        super(windowLength, windowLength);
    }

    @Override
    public <T> WindowStrategy<T> getWindowStrategy() {
        return new TumblingCountWindowStrategy<>(this);
    }

    @Override
    public List<WindowInfo> assignWindows(ITuple tuple) {
        return null;
    }

    public static TumblingCountWindow of(int windowLength) {
        return new TumblingCountWindow(windowLength);

    }

    public static WindowConfig of(BaseWindowedBolt.Count windowLengthCount) {
        return of(windowLengthCount.value);
    }
}
