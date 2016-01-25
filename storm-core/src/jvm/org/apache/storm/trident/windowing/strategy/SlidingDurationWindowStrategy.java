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

import org.apache.storm.trident.windowing.TridentBatchTuple;
import org.apache.storm.trident.windowing.config.SlidingDurationWindow;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.windowing.EvictionPolicy;
import org.apache.storm.windowing.TimeEvictionPolicy;
import org.apache.storm.windowing.TimeTriggerPolicy;
import org.apache.storm.windowing.TriggerHandler;
import org.apache.storm.windowing.TriggerPolicy;

/**
 *
 */
public final class SlidingDurationWindowStrategy extends BaseWindowStrategy {

    public SlidingDurationWindowStrategy(WindowConfig slidingDurationWindow) {
        super(slidingDurationWindow);
    }

    @Override
    public TriggerPolicy<TridentBatchTuple> getTriggerPolicy(TriggerHandler triggerHandler, EvictionPolicy<TridentBatchTuple> evictionPolicy) {
        return new TimeTriggerPolicy<>(windowConfig.getSlideLength(), triggerHandler, evictionPolicy);
    }

    @Override
    public EvictionPolicy<TridentBatchTuple> getEvictionPolicy() {
        return new TimeEvictionPolicy<>(windowConfig.getWindowLength());
    }
}
