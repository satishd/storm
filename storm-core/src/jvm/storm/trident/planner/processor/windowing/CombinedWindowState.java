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
import backtype.storm.windowing.CountEvictionPolicy;
import backtype.storm.windowing.CountTriggerPolicy;
import backtype.storm.windowing.WindowLifecycleListener;
import backtype.storm.windowing.WindowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class CombinedWindowState {
    private static final String KEY_SEPARATOR = "|";

    private final WindowManager<WindowsStateProcessor.TridentBatchTuple> windowManager;
    private final Map<Object, WindowsStateProcessor.WindowState> batchIdVsWindowState = new ConcurrentHashMap<>();
    private final WindowsStateProcessor.MapStore<byte[], Collection<TridentTuple>> _mapStore;
    private final TopologyContext context;
    private final List<Object> finishedBatches = new ArrayList<>();
    private final AtomicInteger triggerId = new AtomicInteger();

    final Map<Integer, Collection<TridentTuple>> triggers = new ConcurrentHashMap<>();

    private static final Logger log = LoggerFactory.getLogger(CombinedWindowState.class);

    public CombinedWindowState(Integer tumblingCount, WindowsStateProcessor.MapStore<byte[], Collection<TridentTuple>> _mapStore, TopologyContext context) {
        this._mapStore = _mapStore;
        this.context = context;
        WindowLifecycleListener<WindowsStateProcessor.TridentBatchTuple> lifecycleListener = newLifeCycleListener();
        windowManager = new WindowManager<>(lifecycleListener);

        int count = tumblingCount != null ? tumblingCount : WindowsStateProcessor.DEFAULT_TUMBLING_COUNT;

        CountEvictionPolicy<WindowsStateProcessor.TridentBatchTuple> timeEvictionPolicy = new CountEvictionPolicy<>(count);
        windowManager.setEvictionPolicy(timeEvictionPolicy);
        windowManager.setTriggerPolicy(new CountTriggerPolicy<>(count, windowManager, timeEvictionPolicy));

        // todo get the state from mapStore if this task is restarted
        // retrieve all tuples starting with
        // byte[] scanKey = keyOf(context, "");
        // from mapStore
        Collection<Collection<TridentTuple>> tridentTuplesCollection = _mapStore.getWithPrefixKey(keyOf(context, ""));
        for (Collection<TridentTuple> tridentTuples : tridentTuplesCollection) {
            for (TridentTuple tridentTuple : tridentTuples) {
                // todo get row key also to retrieve batchid
                Object batchId = null;
                windowManager.add(new WindowsStateProcessor.TridentBatchTuple(batchId, tridentTuple));
            }
        }
    }

    private WindowLifecycleListener<WindowsStateProcessor.TridentBatchTuple> newLifeCycleListener() {
        return new WindowLifecycleListener<WindowsStateProcessor.TridentBatchTuple>() {
            @Override
            public void onExpiry(List<WindowsStateProcessor.TridentBatchTuple> events) {
                log.debug("List of expired tuples: {}", events);
                // removing expired tuples form their respective batches
                updateStateWithExpiredTuples(events, true);
            }

            @Override
            public void onActivation(List<WindowsStateProcessor.TridentBatchTuple> events, List<WindowsStateProcessor.TridentBatchTuple> newEvents, List<WindowsStateProcessor.TridentBatchTuple> expired) {
                System.out.println("Window is triggered, events size:  " + events.size());
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

                System.out.println("####### batchIdVsWindowState size = " + batchIdVsWindowState.size());
            }

            private void storeTriggeredTuples(int curTriggerId, List<WindowsStateProcessor.TridentBatchTuple> events) {
                List<TridentTuple> tuples = new LinkedList<>();
                for (WindowsStateProcessor.TridentBatchTuple event : events) {
                    tuples.add(event.tuple);
                }
                triggers.put(curTriggerId, tuples);
                //todo should we store triggers also in store? These are fired only when next finish batch is invoked.
                storeTuples("trigger|" + curTriggerId, tuples);
            }

            private Set<Object> updateStateWithNewEvents(List<WindowsStateProcessor.TridentBatchTuple> newEvents) {
                Set<Object> batches = new HashSet<>();

                for (WindowsStateProcessor.TridentBatchTuple newEvent : newEvents) {
                    batches.add(newEvent.batchId);
                    batchIdVsWindowState.get(newEvent.batchId).tuples.remove(newEvent.tuple);
                }

                return batches;
            }

            private Set<Object> updateStateWithExpiredTuples(List<WindowsStateProcessor.TridentBatchTuple> expiredEvents, boolean updateStore) {
                Set<Object> batches = new HashSet<>();
                for (WindowsStateProcessor.TridentBatchTuple event : expiredEvents) {
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
                    WindowsStateProcessor.WindowState windowState = batchIdVsWindowState.get(batchId);
                    if(windowState != null) {
                        storeTuples(batchId, windowState.tuples);
                    }
                }
            }

        };
    }

    public WindowsStateProcessor.WindowState initState(Object batchId) {
        if (batchIdVsWindowState.containsKey(batchId)) {
            throw new IllegalStateException("WindowState is already initialized");
        }

        WindowsStateProcessor.WindowState windowState = new WindowsStateProcessor.WindowState(batchId);
        batchIdVsWindowState.put(batchId, windowState);
        return windowState;
    }

    public void addTupleToBatch(Object batchId, TridentTuple tuple) {
        batchIdVsWindowState.get(batchId).tuples.add(tuple);
        // check whether it can be triggered or not.
        // remove window state tuples
    }

    public void addFinishedBatch(Object currentBatch) {
        finishedBatches.add(currentBatch);
        WindowsStateProcessor.WindowState windowState = batchIdVsWindowState.get(currentBatch);
        for (TridentTuple tuple : windowState.tuples) {
            windowManager.add(new WindowsStateProcessor.TridentBatchTuple(currentBatch, tuple));
        }
    }

    public void cleanup() {
        windowManager.shutdown();
    }

    public Set<TridentTuple> getTuples(Object batchId) {
        return batchIdVsWindowState.get(batchId).tuples;
    }

    public void storeTuples(Object batchId, Collection<TridentTuple> tuples) {
        _mapStore.put(keyOf(context, batchId), tuples);
    }

    static byte[] keyOf(TopologyContext context, Object suffixKey) {
        String key = context.getThisComponentId()+KEY_SEPARATOR+context.getThisTaskId()+ KEY_SEPARATOR +suffixKey;
        return key.getBytes();
    }

    public void cleanExpiredBatches() {
        for (Map.Entry<Object, WindowsStateProcessor.WindowState> batchWindowStateEntry : batchIdVsWindowState.entrySet()) {
            Set<TridentTuple> tuples = batchWindowStateEntry.getValue().tuples;
            if(tuples.isEmpty()) {
                batchIdVsWindowState.remove(batchWindowStateEntry.getKey());
            }
        }
    }
}
