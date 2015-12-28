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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.Aggregator;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TridentProcessor;
import storm.trident.planner.processor.AggregateProcessor;
import storm.trident.planner.processor.FreshCollector;
import storm.trident.planner.processor.TridentContext;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class WindowsStateProcessor implements TridentProcessor {

    private static final Logger log = LoggerFactory.getLogger(WindowsStateProcessor.class);
    public static final int DEFAULT_TUMBLING_COUNT = 100;

    private final int tumblingCount;
    private final MapStoreFactory<byte[], Collection<TridentTuple>> _mapStoreFactory;
    private final Fields _inputFields;
    private final Aggregator aggregator;
    private TridentContext _tridentContext;
    private FreshCollector _collector;
    private TridentTupleView.ProjectionFactory _projection;
    private Map conf;
    private TopologyContext context;
    private int ct;
    private CombinedWindowState combinedWindowState;

    public WindowsStateProcessor(int tumblingCount, MapStoreFactory<byte[], Collection<TridentTuple>> mapStoreFactory, Fields inputFields, Aggregator aggregator) {
        this.tumblingCount = tumblingCount;
        _mapStoreFactory = mapStoreFactory;
        _inputFields = inputFields;
        this.aggregator = aggregator;
    }

    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        this.conf = conf;
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
        combinedWindowState = new CombinedWindowState(tumblingCount, _mapStoreFactory.create(), context);
    }

    @Override
    public void cleanup() {
//        _agg.cleanup();
//        combinedWindowState.cleanup();
    }

    static class WindowState {
        final Object batchId;
        final Set<TridentTuple> tuples = Collections.newSetFromMap(new ConcurrentHashMap<TridentTuple, Boolean>());

        public WindowState(Object batchId) {
            this.batchId = batchId;
        }
    }

    public static class TridentBatchTuple {
        final Object batchId;
        final TridentTuple tuple;

        public TridentBatchTuple(Object batchId, TridentTuple tuple) {
            this.batchId = batchId;
            this.tuple = tuple;
        }
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        _collector.setContext(processorContext);
//        processorContext.state[_tridentContext.getStateIndex()] = _agg.init(processorContext.batchId, _collector);
        processorContext.state[_tridentContext.getStateIndex()] = combinedWindowState.initState(processorContext.batchId);
        _collector.setContext(null);
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
            // kryo is not working fine with SetFromMap instance. building new list
            LinkedList<TridentTuple> tridentTuples = new LinkedList<>();
            tridentTuples.addAll(tuples);
            combinedWindowState.storeTuples(batchId, tridentTuples);
        }

        // fire triggers if any
        _collector.setContext(processorContext);
        Map<Integer, Collection<TridentTuple>> triggers = combinedWindowState.triggers;

        for (Map.Entry<Integer, Collection<TridentTuple>> entry : triggers.entrySet()) {
            Collection<TridentTuple> tridentTuples = entry.getValue();
            executeAggregator(processorContext, tridentTuples);
        }
        _collector.setContext(null);

        // remove state if there are no elements in it.
//        combinedWindowState.cleanExpiredBatches();

        // clear once all triggers are run
        triggers.clear();
        // todo update in store also.
    }

    private void executeAggregator(ProcessorContext processorContext, Collection<TridentTuple> tridentTuples) {
        AggregateProcessor aggregateProcessor = new AggregateProcessor(_inputFields, aggregator);
        // todo is it safe to use processorContext and tridentContext of WindowsStateProcessor?
        aggregateProcessor.prepare(conf, context, _tridentContext);
        aggregateProcessor.startBatch(processorContext);

        for (TridentTuple tridentTuple : tridentTuples) {
            String streamId = null; // todo store and get this value
            aggregateProcessor.execute(processorContext, streamId, tridentTuple);
        }
        aggregateProcessor.finishBatch(processorContext);
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
        public Collection<V> getWithPrefixKey(K k) {
            throw new UnsupportedOperationException("partial-key get is not supported");
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
        public Collection<V> getWithPrefixKey(K k);
        public void put(K k, V v);
        public boolean remove(K k);
    }
}
