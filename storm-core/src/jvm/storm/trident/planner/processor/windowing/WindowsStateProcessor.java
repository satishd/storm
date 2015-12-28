/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package storm.trident.planner.processor.windowing;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseWindowedBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.windowing.CountEvictionPolicy;
import backtype.storm.windowing.CountTriggerPolicy;
import backtype.storm.windowing.WindowLifecycleListener;
import backtype.storm.windowing.WindowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.Aggregator;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TridentProcessor;
import storm.trident.planner.processor.FreshCollector;
import storm.trident.planner.processor.TridentContext;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class WindowsStateProcessor implements TridentProcessor {

    private static final Logger log = LoggerFactory.getLogger(WindowsStateProcessor.class);

    private final BaseWindowedBolt.Count tumblingCount;
    private final MapStore<byte[], Collection<TridentTuple>> _mapStore;
    private final Fields _inputFields;
    private final Aggregator aggregator;
    private TridentContext _tridentContext;
    private FreshCollector _collector;
    private TridentTupleView.ProjectionFactory _projection;
    private TopologyContext context;
    private int ct;
    private CombinedWindowState combinedWindowState;

    public WindowsStateProcessor(BaseWindowedBolt.Count tumblingCount, MapStore mapStore, Fields inputFields, Aggregator aggregator) {
        this.tumblingCount = tumblingCount;
        _mapStore = mapStore;
        _inputFields = inputFields;
        this.aggregator = aggregator;
    }

    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        this.context = context;
        List<TridentTuple.Factory> parents = tridentContext.getParentTupleFactories();
        if (parents.size() != 1) {
            throw new RuntimeException("Aggregate operation can only have one parent");
        }
        _tridentContext = tridentContext;
        _collector = new FreshCollector(tridentContext);
        _projection = new TridentTupleView.ProjectionFactory(parents.get(0), _inputFields);
//        _agg.prepare(conf, new TridentOperationContext(context, _projection));
        // windowing operations
        combinedWindowState = new CombinedWindowState(tumblingCount, _collector, _mapStore, context, aggregator);

        // create hbase client and connect to it. You can push required tuples in to state store.
    }

    @Override
    public void cleanup() {
//        _agg.cleanup();
//        combinedWindowState.cleanup();
    }

    static class WindowState {
        private Object batchId;
        private final Set<TridentTuple> tuples = new HashSet<>();

        public WindowState(Object batchId) {
            this.batchId = batchId;
        }
    }

    static class TridentBatchTuple {
        private final Object batchId;
        private final TridentTuple tuple;

        public TridentBatchTuple(Object batchId, TridentTuple tuple) {
            this.batchId = batchId;
            this.tuple = tuple;
        }
    }

    static class CombinedWindowState {
        private final WindowManager<TridentBatchTuple> windowManager;
        private Map<Object, WindowState> batchIdVsWindowState = new HashMap<>();
        private final FreshCollector _collector;
        private final MapStore<byte[], Collection<TridentTuple>> _mapStore;
        private final TopologyContext context;
        private final Aggregator aggregator;
        private List<Object> finishedBatches = new ArrayList<>();
        private AtomicInteger triggerId = new AtomicInteger();
        private Map<Integer, Collection<TridentTuple>> triggers = new ConcurrentHashMap<>();

        public CombinedWindowState(BaseWindowedBolt.Count tumblingCount, FreshCollector _collector, MapStore<byte[], Collection<TridentTuple>> _mapStore, TopologyContext context, Aggregator aggregator) {
            this._collector = _collector;
            this._mapStore = _mapStore;
            this.context = context;
            this.aggregator = aggregator;
            WindowLifecycleListener<TridentBatchTuple> lifecycleListener = newLifeCycleListener();
            windowManager = new WindowManager<>(lifecycleListener);

            int count = tumblingCount != null ? tumblingCount.value : 100;

            CountEvictionPolicy<TridentBatchTuple> timeEvictionPolicy = new CountEvictionPolicy<>(count);
            windowManager.setEvictionPolicy(timeEvictionPolicy);
            windowManager.setTriggerPolicy(new CountTriggerPolicy<>(count, windowManager, timeEvictionPolicy));
        }

        private WindowLifecycleListener<TridentBatchTuple> newLifeCycleListener() {
            return new WindowLifecycleListener<TridentBatchTuple>() {
                @Override
                public void onExpiry(List<TridentBatchTuple> events) {
                    log.debug("List of expired tuples: {}", events);
                    // removing expired tuples form their respective batches
                    updateStateWithExpiredTuples(events, true);
                }

                @Override
                public void onActivation(List<TridentBatchTuple> events, List<TridentBatchTuple> newEvents, List<TridentBatchTuple> expired) {
                    log.debug("Window is triggered, events size: {} ", events.size());
                    // trigger occurred, update state and inmemory
                    int curTriggerId = triggerId.incrementAndGet();
                    storeTriggeredTuples(curTriggerId, events);

                    // todo can we use aggregator and aggregate the result and store?

                    // remove expired events from state
                    Set<Object> removedBatches = updateStateWithExpiredTuples(expired, false);

                    // update only newly added as expired events are already removed.
                    Set<Object> updatedBatches = updateStateWithNewEvents(newEvents);
                    updatedBatches.addAll(removedBatches);

                    // store update batches
                    updateBatchesInStore(updatedBatches);

                }

                private void storeTriggeredTuples(int curTriggerId, List<TridentBatchTuple> events) {
                    List<TridentTuple> tuples = new ArrayList<>();
                    for (TridentBatchTuple event : events) {
                        tuples.add(event.tuple);
                    }
                    triggers.put(curTriggerId, tuples);
                    _mapStore.put(("trigger:"+curTriggerId).getBytes(), tuples);
                }

                private Set<Object> updateStateWithNewEvents(List<TridentBatchTuple> newEvents) {
                    Set<Object> batches = new HashSet<>();

                    for (TridentBatchTuple newEvent : newEvents) {
                        batches.add(newEvent.batchId);
                        batchIdVsWindowState.get(newEvent.batchId).tuples.remove(newEvent.tuple);
                    }

                    return batches;
                }

                private Set<Object> updateStateWithExpiredTuples(List<TridentBatchTuple> expiredEvents, boolean updateStore) {
                    Set<Object> batches = new HashSet<>();
                    for (TridentBatchTuple event : expiredEvents) {
                        batches.add(event.batchId);
                        batchIdVsWindowState.get(event.batchId).tuples.remove(event.tuple);
                    }
                    // store modified batches
                    if (updateStore) {
                        updateBatchesInStore(batches);
                    }

                    return batches;
                }

                private void updateBatchesInStore(Set<Object> batches) {
                    for (Object batchId : batches) {
                        WindowState windowState = batchIdVsWindowState.get(batchId);
                        _mapStore.put(keyOf(context, batchId), windowState.tuples);
                    }
                }

            };
        }

        public WindowState initState(Object batchId) {
            if (batchIdVsWindowState.containsKey(batchId)) {
                throw new IllegalStateException("WindowState is already initialized");
            }

            WindowState windowState = new WindowState(batchId);
            batchIdVsWindowState.put(batchId, windowState);
            return windowState;
        }

        public boolean exists(Object batchId) {
            WindowState windowState = batchIdVsWindowState.get(batchId);
            return !(windowState == null || windowState.tuples.isEmpty());
        }

        public void addTupleToBatch(Object batchId, TridentTuple tuple) {
            batchIdVsWindowState.get(batchId).tuples.add(tuple);
            // check whether it can be triggered or not.
            // remove window state tuples
        }

        public void addFinishedBatch(Object currentBatch) {
            finishedBatches.add(currentBatch);
            for (TridentTuple tuple : batchIdVsWindowState.get(currentBatch).tuples) {
                windowManager.add(new TridentBatchTuple(currentBatch, tuple));
            }
        }

        public void cleanup() {
            windowManager.shutdown();
        }

        public Set<TridentTuple> getTuples(Object batchId) {
            return batchIdVsWindowState.get(batchId).tuples;
        }
    }


    @Override
    public void startBatch(ProcessorContext processorContext) {
        _collector.setContext(processorContext);
//        processorContext.state[_tridentContext.getStateIndex()] = _agg.init(processorContext.batchId, _collector);
        processorContext.state[_tridentContext.getStateIndex()] = new WindowState(processorContext.batchId);
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        combinedWindowState.addTupleToBatch(processorContext.batchId, tuple);
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
//        _collector.setContext(processorContext);
//        _agg.complete(processorContext.state[_tridentContext.getStateIndex()], _collector);
        Object batchId = processorContext.batchId;
        System.out.println("##########WindowsProcessor.finishBatch: "+batchId);

        // this would also add those tuples to the window.
        combinedWindowState.addFinishedBatch(batchId);
        Set<TridentTuple> tuples = combinedWindowState.getTuples(batchId);

        // before it returns from here, current state should be saved.

        // check whether the state exists in the combined window or not.
        if (tuples != null && !tuples.isEmpty()) {
            // save the state
            // todo batchId is unique for each stream, Right? Then we should add streamId as one of the keys in the map.
            _mapStore.put(keyOf(context, batchId), tuples);
        }
    }

    private static byte[] keyOf(TopologyContext context, Object batchId) {
        String key = context.getThisComponentId()+":"+context.getThisTaskId()+":"+batchId;
        return key.getBytes();
    }

    @Override
    public TridentTuple.Factory getOutputFactory() {
        return _collector.getOutputFactory();
    }

    public static class InMemoryMapStore<K, V> implements MapStore<K, V> {

        private Map<K, V> map = new HashMap<>();

        @Override
        public V get(K k) {
            return map.get(k);
        }

        @Override
        public void put(K k, V v) {
            map.put(k, v);
        }

        @Override
        public boolean remove(K k) {
            map.remove(k);
            return true;
        }
    }

    public static interface MapStore<K, V> {
        public V get(K k);
        public void put(K k, V v);
        public boolean remove(K k);
    }
}
