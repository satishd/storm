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
import org.apache.storm.trident.spout.IBatchID;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class TridentWindowManager extends BaseTridentWindowManager<TridentBatchTuple> {
    private static final Logger log = LoggerFactory.getLogger(TridentWindowManager.class);

    private static final String TUPLE_PREFIX = "tu" + WindowTridentProcessor.KEY_SEPARATOR;

    private final String windowTupleTaskId;

    private Integer maxCachedTuplesSize;
    private AtomicInteger currentCachedTuplesSize = new AtomicInteger();

    public TridentWindowManager(WindowConfig windowConfig, String windowTaskId, WindowsStore windowStore, Aggregator aggregator,
                                BatchOutputCollector delegateCollector, Integer maxTuplesCacheSize) {
        super(windowConfig, windowTaskId, windowStore, aggregator, delegateCollector);

        this.maxCachedTuplesSize = maxTuplesCacheSize;

        windowTupleTaskId = TUPLE_PREFIX + windowTaskId;
    }

    public void prepare() {
        // get existing tuples and pending triggers for this operator-component/task and add them to WindowManager
        Iterable<Map.Entry<String, Map<String, Object>>> allEntriesIterable = windowStore.getAllEntries();

        for (Map.Entry<String, Map<String, Object>> primaryKeyEntries : allEntriesIterable) {
            String primaryKey = primaryKeyEntries.getKey();
            if (primaryKey.startsWith(TUPLE_PREFIX)) {
                String batchId = batchIdFromPrimaryKey(primaryKey);
                for (Map.Entry<String, Object> indexedTuples : primaryKeyEntries.getValue().entrySet()) {
                    addToWindowManager(Integer.valueOf(indexedTuples.getKey()), batchId, (TridentTuple) indexedTuples.getValue());
                }
            } else if (primaryKey.startsWith(TRIGGER_PREFIX)) {
                for (Map.Entry<String, Object> triggerEntry : primaryKeyEntries.getValue().entrySet()) {
                    int triggerId = Integer.valueOf(triggerEntry.getKey());
                    List<List<Object>> triggerValue = (List<List<Object>>) triggerEntry.getValue();
                    pendingTriggers.add(new TriggerResult(triggerId, triggerValue));
                }
            } else {
                log.warn("Ignoring unknown primary key entry from windows store [{}]", primaryKey);
            }
        }

    }

    private String batchIdFromPrimaryKey(String primaryKey) {
        int lastSepIndex = primaryKey.lastIndexOf(WindowTridentProcessor.KEY_SEPARATOR);
        if (lastSepIndex < 0) {
            throw new IllegalArgumentException("primaryKey does not have key separator '" + WindowTridentProcessor.KEY_SEPARATOR + "'");
        }
        return primaryKey.substring(lastSepIndex);
    }

    public void addTuplesBatch(Object batchId, List<TridentTuple> tuples) {
        // check if they are already added then ignore these tuples. This batch is replayed.
        if (activeBatches.contains(getBatchTxnId(batchId))) {
            log.info("Ignoring already added tuples with batch: %s", batchId);
            return;
        }

        log.debug("Adding tuples to window-manager for batch: ", batchId);
        for (int i = 0; i < tuples.size(); i++) {
            String primaryKey = keyOf(batchId);
            TridentTuple tridentTuple = tuples.get(i);
            windowStore.put(new WindowsStore.Key(primaryKey, String.valueOf(i)), tridentTuple);
            addToWindowManager(i, primaryKey, tridentTuple);
        }
    }

    private void addToWindowManager(int tupleIndex, String batchTxnId, TridentTuple tridentTuple) {
        TridentTuple actualTuple = null;
        if (maxCachedTuplesSize == null || currentCachedTuplesSize.get() < maxCachedTuplesSize) {
            actualTuple = tridentTuple;
        }
        windowManager.add(new TridentBatchTuple(batchTxnId, System.currentTimeMillis(), tupleIndex, actualTuple));
    }

    public String getBatchTxnId(Object batchId) {
        if (!(batchId instanceof IBatchID)) {
            throw new IllegalArgumentException("argument should be an IBatchId instance");
        }
        return ((IBatchID) batchId).getId().toString();
    }

    public String keyOf(Object batchId) {
        return windowTupleTaskId + WindowTridentProcessor.KEY_SEPARATOR + getBatchTxnId(batchId);
    }

    public List<TridentTuple> getTridentTuples(List<TridentBatchTuple> tridentBatchTuples) {
        List<TridentTuple> resultTuples = new ArrayList<>();
        List<WindowsStore.Key> keys = new ArrayList<>();
        for (TridentBatchTuple tridentBatchTuple : tridentBatchTuples) {
            TridentTuple tuple = collectTridentTupleOrKey(tridentBatchTuple, keys);
            if(tuple != null) {
                resultTuples.add(tuple);
            }
        }

        if(keys.size() > 0) {
            Iterable<?> storedTuples = windowStore.get(keys);
            for (Object storedTuple : storedTuples) {
                resultTuples.add((TridentTuple) storedTuple);
            }
        }

        return resultTuples;
    }

    public TridentTuple collectTridentTupleOrKey(TridentBatchTuple tridentBatchTuple, List<WindowsStore.Key> keys) {
        if (tridentBatchTuple.tridentTuple != null) {
            return tridentBatchTuple.tridentTuple;
        }
        keys.add(tupleKey(tridentBatchTuple));
//        return (TridentTuple) windowStore.get(tupleKey(tridentBatchTuple));
        return null;
    }

    public void onTuplesExpired(List<TridentBatchTuple> expiredTuples) {
        if (maxCachedTuplesSize != null) {
            currentCachedTuplesSize.addAndGet(-expiredTuples.size());
        }

        List<WindowsStore.Key> keys = new ArrayList<>();
        for (TridentBatchTuple expiredTuple : expiredTuples) {
            keys.add(tupleKey(expiredTuple));
        }

        windowStore.removeAll(keys);
    }

    private WindowsStore.Key tupleKey(TridentBatchTuple tridentBatchTuple) {
        return new WindowsStore.Key(tridentBatchTuple.batchId, String.valueOf(tridentBatchTuple.tupleIndex));
    }

}
