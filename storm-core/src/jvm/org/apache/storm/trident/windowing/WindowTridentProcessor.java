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
package org.apache.storm.trident.windowing;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.planner.ProcessorContext;
import org.apache.storm.trident.planner.TridentProcessor;
import org.apache.storm.trident.planner.processor.FreshCollector;
import org.apache.storm.trident.planner.processor.TridentContext;
import org.apache.storm.trident.tuple.ConsList;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 *
 */
public class WindowTridentProcessor implements TridentProcessor {
    private static final Logger log = LoggerFactory.getLogger(WindowTridentProcessor.class);

    public static final String TRIGGER_FIELD_NAME = "_task_info";
    static final String KEY_SEPARATOR = "|";

    private final String windowId;
    private final Fields inputFields;
    private final Aggregator aggregator;
    private WindowConfig windowConfig;

    private WindowsStoreFactory windowStoreFactory;

    private Map conf;
    private TopologyContext topologyContext;
    private FreshCollector collector;
    private TridentTupleView.ProjectionFactory projection;
    private TridentContext tridentContext;
    private TridentWindowManager tridentWindowManager;

    public WindowTridentProcessor(WindowConfig windowConfig, String uniqueWindowId,
                                  WindowsStoreFactory windowStoreFactory, Fields inputFields, Aggregator aggregator) {

        this.windowConfig = windowConfig;
        windowId = uniqueWindowId;
        this.windowStoreFactory = windowStoreFactory;
        this.inputFields = inputFields;
        this.aggregator = aggregator;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        this.conf = conf;
        this.topologyContext = context;
        List<TridentTuple.Factory> parents = tridentContext.getParentTupleFactories();
        if (parents.size() != 1) {
            throw new RuntimeException("Aggregate operation can only have one parent");
        }
        this.tridentContext = tridentContext;
        collector = new FreshCollector(tridentContext);
        projection = new TridentTupleView.ProjectionFactory(parents.get(0), inputFields);
        String windowTaskId = windowId + KEY_SEPARATOR + topologyContext.getThisTaskId() + KEY_SEPARATOR;
        tridentWindowManager = new TridentWindowManager(windowConfig, windowTaskId, windowStoreFactory.create(windowTaskId), aggregator, tridentContext.getDelegateCollector());
    }

    @Override
    public void cleanup() {
        tridentWindowManager.shutdown();
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        // initialize state for batch
        processorContext.state[tridentContext.getStateIndex()] = new ArrayList<TridentTuple>();
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        // add tuple to the batch state
        Object state = processorContext.state[tridentContext.getStateIndex()];
        ((List<TridentTuple>) state).add(projection.create(tuple));
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
        log.debug("Received finishBatch of : {} ", processorContext.batchId);
        // get all the tuples in a batch and add it to trident-window-manager
        List<TridentTuple> tuples = (List<TridentTuple>) processorContext.state[tridentContext.getStateIndex()];
        tridentWindowManager.addTuplesBatch(processorContext.batchId, tuples);

        collector.setContext(processorContext);
        Queue<TridentWindowManager.TriggerResult> pendingTriggers = tridentWindowManager.getPendingTriggers();
        log.debug("pending triggers at batch: {} and triggers.size: {} ", processorContext.batchId, pendingTriggers.size());

        Iterator<TridentWindowManager.TriggerResult> pendingTriggersIter = pendingTriggers.iterator();
        TridentWindowManager.TriggerResult triggerResult = null;
        while(pendingTriggersIter.hasNext()) {
            triggerResult = pendingTriggersIter.next();
            for (List<Object> aggregatedResult : triggerResult.result) {
                collector.emit(new ConsList(tridentWindowManager.triggerKey(triggerResult.id), aggregatedResult));
            }
            pendingTriggersIter.remove();
        }
        collector.setContext(null);
    }

    @Override
    public TridentTuple.Factory getOutputFactory() {
        return collector.getOutputFactory();
    }

}
