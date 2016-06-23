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
import org.apache.storm.trident.windowing.strategy.SlidingCountWindowStrategy;
import org.apache.storm.trident.windowing.strategy.WindowStrategy;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.windowing.CountTriggerPolicy;
import org.apache.storm.windowing.TriggerPolicy;
import org.apache.storm.windowing.WindowInfo;

import java.util.List;

/**
 * Represents configuration of sliding window based on count of events. Window of length {@code windowLength} slides
 * at every count of given {@code slideLength}
 *
 */
public final class SlidingCountWindow extends SlidingWindowConfig {

    private SlidingCountWindow(int windowLength, int slideLength) {
        super(windowLength, slideLength);
    }

    @Override
    public <T> WindowStrategy<T> getWindowStrategy() {
        return new SlidingCountWindowStrategy<>(this);
    }

    @Override
    public TriggerPolicy getDefaultTriggerPolicy() {
        return new CountTriggerPolicy();
    }

    @Override
    public List<WindowInfo> assignWindows(ITuple tuple) {
        return null;
    }

    public static SlidingCountWindow of(int windowCount, int slidingCount) {
        return new SlidingCountWindow(windowCount, slidingCount);
    }

    public static WindowConfig of(BaseWindowedBolt.Count windowLengthCount, BaseWindowedBolt.Count slidingIntervalCount) {
        return of(windowLengthCount.value, slidingIntervalCount.value);
    }
}
