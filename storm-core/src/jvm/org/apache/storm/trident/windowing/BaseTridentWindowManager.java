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
package org.apache.storm.trident.windowing;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.trident.windowing.strategy.WindowStrategy;
import org.apache.storm.trident.windowing.strategy.WindowStrategyFactory;
import org.apache.storm.windowing.EvictionPolicy;
import org.apache.storm.windowing.TriggerPolicy;
import org.apache.storm.windowing.WindowLifecycleListener;
import org.apache.storm.windowing.WindowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public abstract class BaseTridentWindowManager<T> implements ITridentWindowManager<T> {
    private static final Logger log = LoggerFactory.getLogger(BaseTridentWindowManager.class);

    public static final String TRIGGER_PREFIX = "tr" + WindowsStore.KEY_SEPARATOR;
    public static final String TRIGGER_COUNT_PREFIX = "tc" + WindowsStore.KEY_SEPARATOR;

    protected final WindowManager<T> windowManager;
    protected final Aggregator aggregator;
    protected final BatchOutputCollector delegateCollector;
    protected final String windowTriggerTaskId;
    protected final WindowsStore windowStore;

    protected final Set<String> activeBatches = new HashSet<>();
    protected final Queue<TriggerResult> pendingTriggers = new ConcurrentLinkedQueue<>();
    protected final AtomicInteger triggerId = new AtomicInteger();
    private final String windowTriggerCountId;


    public BaseTridentWindowManager(WindowConfig windowConfig, String windowTaskId, WindowsStore windowStore, Aggregator aggregator,
                                    BatchOutputCollector delegateCollector) {
        this.windowStore = windowStore;
        this.aggregator = aggregator;
        this.delegateCollector = delegateCollector;

        windowTriggerTaskId = TRIGGER_PREFIX + windowTaskId;
        windowTriggerCountId = TRIGGER_COUNT_PREFIX + windowTaskId;
        windowManager = new WindowManager<>(new TridentWindowLifeCycleListener());

        WindowStrategyFactory<T> windowStrategyFactory = new WindowStrategyFactory<>();
        WindowStrategy<T> windowStrategy = windowStrategyFactory.create(windowConfig);
        EvictionPolicy<T> evictionPolicy = windowStrategy.getEvictionPolicy();
        windowManager.setEvictionPolicy(evictionPolicy);
        TriggerPolicy<T> triggerPolicy = windowStrategy.getTriggerPolicy(windowManager, evictionPolicy);
        windowManager.setTriggerPolicy(triggerPolicy);
    }

    @Override
    public void prepare() {
        // get trigger count value from store
        Object result = windowStore.get(new WindowsStore.Key(windowTriggerCountId, ""));
        triggerId.set((Integer) result);
    }

    class TridentWindowLifeCycleListener implements WindowLifecycleListener<T> {

        @Override
        public void onExpiry(List<T> expiredEvents) {
            log.debug("onExpiry is invoked");
            onTuplesExpired(expiredEvents);
        }

        @Override
        public void onActivation(List<T> events, List<T> newEvents, List<T> expired) {
            log.debug("onActivation is invoked with events size: {}", events.size());
            // trigger occurred, create an aggregation and keep them in store
            int currentTriggerId = triggerId.incrementAndGet();
            execAggregatorAndStoreResult(currentTriggerId, events);
        }
    }

    protected abstract void onTuplesExpired(List<T> expiredEvents);

    protected void execAggregatorAndStoreResult(int currentTriggerId, List<T> tupleEvents) {
        List<TridentTuple> resultTuples = getTridentTuples(tupleEvents);

        // run aggregator to compute the result
        AccumulatedTuplesCollector collector = new AccumulatedTuplesCollector(delegateCollector);
        Object state = aggregator.init(currentTriggerId, collector);
        for (TridentTuple resultTuple : resultTuples) {
            aggregator.aggregate(state, resultTuple, collector);
        }
        aggregator.complete(state, collector);

        List<List<Object>> resultantAggregatedValue = collector.values;
        windowStore.put(triggerKey(currentTriggerId), resultantAggregatedValue);
        pendingTriggers.add(new TriggerResult(currentTriggerId, resultantAggregatedValue));
    }

    protected abstract List<TridentTuple> getTridentTuples(List<T> tupleEvents);

    public WindowsStore.Key triggerKey(Integer currentTriggerId) {
        return new WindowsStore.Key(windowTriggerTaskId, currentTriggerId.toString());
    }

    static class AccumulatedTuplesCollector implements TridentCollector {

        final List<List<Object>> values = new ArrayList<>();
        private final BatchOutputCollector delegateCollector;

        public AccumulatedTuplesCollector(BatchOutputCollector delegateCollector) {
            this.delegateCollector = delegateCollector;
        }

        @Override
        public void emit(List<Object> values) {
            this.values.add(values);
        }

        @Override
        public void reportError(Throwable t) {
            delegateCollector.reportError(t);
        }

    }

    static class TriggerResult {
        final int id;
        final List<List<Object>> result;

        public TriggerResult(int id, List<List<Object>> result) {
            this.id = id;
            this.result = result;
        }
    }

    public Queue<TriggerResult> getPendingTriggers() {
        return pendingTriggers;
    }

    public void shutdown() {
        windowManager.shutdown();
        windowStore.shutdown();
    }

}
