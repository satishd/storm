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
import org.apache.storm.trident.spout.IBatchID;
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
public class InMemoryTridentWindowManager {
    private static final Logger log = LoggerFactory.getLogger(InMemoryTridentWindowManager.class);

    public static final String TRIGGER_PREFIX = "tr" + WindowTridentProcessor.KEY_SEPARATOR;

    private final WindowManager<TridentTuple> windowManager;
    private final WindowsStore windowStore;
    private final Aggregator aggregator;
    private final BatchOutputCollector delegateCollector;
    private final Queue<TriggerResult> pendingTriggers = new ConcurrentLinkedQueue<>();
    private final AtomicInteger triggerId = new AtomicInteger();
    private final String windowTriggerTaskId;
    private Set<String> activeBatches = new HashSet<>();

    public InMemoryTridentWindowManager(WindowConfig windowConfig, String windowTaskId, WindowsStore windowStore, Aggregator aggregator, BatchOutputCollector delegateCollector) {
        this.windowStore = windowStore;
        this.aggregator = aggregator;
        this.delegateCollector = delegateCollector;

        windowTriggerTaskId = TRIGGER_PREFIX + windowTaskId;
        windowManager = new WindowManager<>(new TridentWindowLifeCycleListener());

        WindowStrategyFactory<TridentTuple> windowStrategyFactory = new WindowStrategyFactory<>();
        WindowStrategy<TridentTuple> windowStrategy = windowStrategyFactory.create(windowConfig);
        EvictionPolicy<TridentTuple> evictionPolicy = windowStrategy.getEvictionPolicy();
        windowManager.setEvictionPolicy(evictionPolicy);
        TriggerPolicy<TridentTuple> triggerPolicy = windowStrategy.getTriggerPolicy(windowManager, evictionPolicy);
        windowManager.setTriggerPolicy(triggerPolicy);
    }

    private String batchIdFromPrimaryKey(String primaryKey) {
        int lastSepIndex = primaryKey.lastIndexOf(WindowTridentProcessor.KEY_SEPARATOR);
        if(lastSepIndex< 0) {
            throw new IllegalArgumentException("primaryKey does not have key separator '"+WindowTridentProcessor.KEY_SEPARATOR+"'");
        }
        return primaryKey.substring(lastSepIndex);
    }

    public void addTuplesBatch(Object batchId, List<TridentTuple> tuples) {
        // check if they are already added then ignore these tuples. This batch is replayed.
        if(activeBatches.contains(getBatchTxnId(batchId))) {
            log.info("Ignoring already added tuples with batch: %s", batchId);
            return;
        }

        log.debug("Adding tuples to window-manager for batch: ", batchId);
        for (TridentTuple tuple : tuples) {
            windowManager.add(tuple);
        }
    }

    public String getBatchTxnId(Object batchId) {
        if (!(batchId instanceof IBatchID)) {
            throw new IllegalArgumentException("argument should be an IBatchId instance");
        }
        return ((IBatchID) batchId).getId().toString();
    }

    public void shutdown() {
        windowManager.shutdown();
        windowStore.shutdown();
    }

    class TridentWindowLifeCycleListener implements WindowLifecycleListener<TridentTuple> {

        @Override
        public void onExpiry(List<TridentTuple> expiredEvents) {
            log.debug("onExpiry is invoked");
//            removeExpiredTuplesFromStore(expiredEvents);
        }

        @Override
        public void onActivation(List<TridentTuple> events, List<TridentTuple> newEvents, List<TridentTuple> expired) {
            log.debug("onActivation is invoked with events size: {}", events.size());
            // trigger occurred, create an aggregation and keep them in store
            int currentTriggerId = triggerId.incrementAndGet();
            execAggregatorAndStoreResult(currentTriggerId, events);
        }
    }

    private void execAggregatorAndStoreResult(int currentTriggerId, List<TridentTuple> tridentBatchTuples) {
        List<TridentTuple> resultTuples = tridentBatchTuples;

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

    protected WindowsStore.Key triggerKey(Integer currentTriggerId) {
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
}
