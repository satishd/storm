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
import storm.trident.state.map.MapState;
import storm.trident.topology.TransactionAttempt;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class WindowsProcessor implements TridentProcessor {

    private static final Logger log = LoggerFactory.getLogger(WindowsProcessor.class);

    private final Duration _duration;
    private final MapState _mapState;
    private final Fields _inputFields;
    private TridentContext _tridentContext;
    private FreshCollector _collector;
    private TridentTupleView.ProjectionFactory _projection;
    private CombinedWindowState combinedWindowState;
    private TopologyContext context;
    private int ct;

    public WindowsProcessor(Duration duration, MapState mapState, Fields inputFields, Aggregator agg) {
        _duration = duration;
        _mapState = mapState;
//        _agg = agg;
        _inputFields = inputFields;
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
        combinedWindowState = new CombinedWindowState(_duration, _collector);
    }

    @Override
    public void cleanup() {
//        _agg.cleanup();
        combinedWindowState.cleanup();
    }

    static class WindowState {
        final List<TridentTuple> tuples = new LinkedList<>();
        private Object batchId;

        public WindowState(Object batchId) {
            this.batchId = batchId;
        }
    }

    static class CombinedWindowState {
        private final WindowManager<TridentTuple> windowManager;
        private Map<Object, WindowState> batchIdVsWindowState = new HashMap<>();
        private final FreshCollector _collector;
        private List<Object> finishedBatches = new ArrayList<>();
        private final TreeMap<Long, ActiveBatch> activeBatches = new TreeMap<>();

        public CombinedWindowState(Duration tumblingWindowDuration, FreshCollector _collector) {
            this._collector = _collector;
            WindowLifecycleListener<TridentTuple> lifecycleListener = newLifeCycleListener();
            windowManager = new WindowManager<>(lifecycleListener);
            CountEvictionPolicy<TridentTuple> timeEvictionPolicy = new CountEvictionPolicy<>(100);
            windowManager.setEvictionPolicy(timeEvictionPolicy);
            windowManager.setTriggerPolicy(new CountTriggerPolicy<>(100, windowManager, timeEvictionPolicy));
        }

        private WindowLifecycleListener<TridentTuple> newLifeCycleListener() {
            return new WindowLifecycleListener<TridentTuple>() {
                @Override
                public void onExpiry(List<TridentTuple> events) {
                    log.debug("List of expired tuples: {}", events);
                }

                @Override
                public void onActivation(List<TridentTuple> events, List<TridentTuple> newEvents, List<TridentTuple> expired) {
                    log.debug("Window is triggered, events size: {} ", events.size());
                    // when trigger occurs, save state.

                    // send the tuples with the minimum active batch
                    ActiveBatch activeBatch = null;
                    while (activeBatch == null) {
                        Map.Entry<Long, ActiveBatch> activeBatchEntry = activeBatches.firstEntry();
                        if (activeBatchEntry != null) {
                            activeBatch = activeBatchEntry.getValue();
                        }
                        /// can spin with yield..
                    }

                    System.out.println("############ Triggered");
                    try {
                        activeBatch.lock.lock();
                        _collector.setContext(activeBatch.processorContext);
                        for (TridentTuple event : events) {
                            //todo make sure it always contains atleast one single active batch.
                            _collector.emit(event.getValues());
                            System.out.println("####### event = " + event.getValues());
                        }
                    } finally {
                        _collector.setContext(null);
                        activeBatch.lock.unlock();
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
                windowManager.add(tuple);
            }

        }

        public void cleanup() {
            windowManager.shutdown();
        }
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        _collector.setContext(processorContext);
//        processorContext.state[_tridentContext.getStateIndex()] = _agg.init(processorContext.batchId, _collector);
        processorContext.state[_tridentContext.getStateIndex()] = combinedWindowState.initState(processorContext.batchId);
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        _collector.setContext(processorContext);
//        _agg.aggregate(processorContext.state[_tridentContext.getStateIndex()], _projection.create(tuple), _collector);
        Object batchId = processorContext.batchId;
        if (batchId instanceof TransactionAttempt) {
            Long transactionId = ((TransactionAttempt) batchId).getTransactionId();
            addActiveBatch(processorContext, transactionId);
        }
        System.out.println("##########WindowsProcessor.execute: "+ ct++);
        // do not add to the window now, they will be added to the window in finishbatch
        addToWindow(batchId, _projection.create(tuple));
    }

    private void addActiveBatch(ProcessorContext processorContext, Long transactionId) {
        if (!combinedWindowState.activeBatches.containsKey(transactionId)) {
            combinedWindowState.activeBatches.put(transactionId, new ActiveBatch(processorContext));
        }
    }

    static class ActiveBatch {
        private final ProcessorContext processorContext;
        private final ReentrantLock lock = new ReentrantLock();

        public ActiveBatch(ProcessorContext processorContext) {
            this.processorContext = processorContext;
        }
    }

    private void addToWindow(Object batchId, TridentTuple tridentTuple) {
        combinedWindowState.addTupleToBatch(batchId, tridentTuple);
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
//        _collector.setContext(processorContext);
//        _agg.complete(processorContext.state[_tridentContext.getStateIndex()], _collector);
        Object batchId = processorContext.batchId;
        System.out.println("##########WindowsProcessor.finishBatch: "+batchId);
        combinedWindowState.addFinishedBatch(batchId);
        if (batchId instanceof TransactionAttempt) {
            removeActiveBatch(((TransactionAttempt) batchId).getTransactionId());
        }

        // check whether the state exists in the combined window or not.
        if (combinedWindowState.exists(batchId)) {
            // save the state
            // todo batchId is unique for each stream, Right? Then we should add streamId as one of the keys in the map.
//            _mapState.multiPut(Collections.singletonList(Collections.singletonList(batchId)),
//                    Collections.singletonList(processorContext.state[_tridentContext.getStateIndex()]));
//            _mapState.multiPut(Collections.singletonList(Collections.singletonList("__window.active.batches$" + context.getThisComponentId() + "$" + context.getThisTaskId())),
//                    combinedWindowState.finishedBatches);
        }
    }

    private void removeActiveBatch(Long transactionId) {
        ActiveBatch activeBatch = combinedWindowState.activeBatches.get(transactionId);
        if (activeBatch != null) {
            activeBatch.lock.lock();
            try {
                combinedWindowState.activeBatches.remove(transactionId);
            } finally {
                activeBatch.lock.unlock();
            }
        }
    }

    @Override
    public TridentTuple.Factory getOutputFactory() {
        return _collector.getOutputFactory();
    }
}
