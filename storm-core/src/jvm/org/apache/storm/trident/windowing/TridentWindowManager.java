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
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class TridentWindowManager extends BaseTridentWindowManager<TridentBatchTuple> {
    private static final Logger log = LoggerFactory.getLogger(TridentWindowManager.class);

    private static final String TUPLE_PREFIX = "tu" + WindowsStore.KEY_SEPARATOR;

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
        super.prepare();
        // get existing tuples and pending triggers for this operator-component/task and add them to WindowManager
        Iterable<WindowsStore.Entry> allEntriesIterable = windowStore.getAllKeys();
        //todo-sato how to maintain uniqueness in generating trigger keys so that there will be no overlaps between task restarts.
        // oneway to do that may be store get existing server restart count and increment it when a task is prepared.
        List<String> triggerKeys = new ArrayList<>();
        for (WindowsStore.Entry entry : allEntriesIterable) {
            String key = entry.key;
            if (key.startsWith(windowTupleTaskId)) {
                int tupleIndexValue = lastPart(key);
                String batchId = secondLastPart(key);
                windowManager.add(new TridentBatchTuple(batchId, System.currentTimeMillis(), tupleIndexValue, null));
            } else if (key.startsWith(windowTriggerTaskId)) {
                triggerKeys.add(entry.key);
            } else {
                log.warn("Ignoring unknown primary key entry from windows store [{}]", key);
            }
        }

        // get trigger values
        Iterable<Object> triggerObjects = windowStore.get(triggerKeys);
        int i=0;
        for (Object triggerObject : triggerObjects) {
            pendingTriggers.add(new TriggerResult(lastPart(triggerKeys.get(i++)), (List<List<Object>>) triggerObject));
        }

    }

    private int lastPart(String key) {
        int lastSepIndex = key.lastIndexOf(WindowsStore.KEY_SEPARATOR);
        if (lastSepIndex < 0) {
            throw new IllegalArgumentException("primaryKey does not have key separator '" + WindowsStore.KEY_SEPARATOR + "'");
        }
        return Integer.parseInt(key.substring(lastSepIndex));
    }

    private String secondLastPart(String key) {
        int lastSepIndex = key.lastIndexOf(WindowsStore.KEY_SEPARATOR);
        if (lastSepIndex < 0) {
            throw new IllegalArgumentException("key "+key+" does not have key separator '" + WindowsStore.KEY_SEPARATOR + "'");
        }
        String trimKey = key.substring(0, lastSepIndex);
        int secondLastSepIndex = trimKey.lastIndexOf(WindowsStore.KEY_SEPARATOR);
        if (lastSepIndex < 0) {
            throw new IllegalArgumentException("key "+key+" does not have second key separator '" + WindowsStore.KEY_SEPARATOR + "'");
        }

        return key.substring(secondLastSepIndex+1, lastSepIndex);
    }

    public void addTuplesBatch(Object batchId, List<TridentTuple> tuples) {
        // check if they are already added then ignore these tuples. This batch is replayed.
        if (activeBatches.contains(getBatchTxnId(batchId))) {
            log.info("Ignoring already added tuples with batch: %s", batchId);
            return;
        }

        log.debug("Adding tuples to window-manager for batch: ", batchId);
        for (int i = 0; i < tuples.size(); i++) {
            String key = keyOf(batchId);
            TridentTuple tridentTuple = tuples.get(i);
            windowStore.put(key+i, tridentTuple);
            addToWindowManager(i, key, tridentTuple);
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
        return windowTupleTaskId + getBatchTxnId(batchId) + WindowsStore.KEY_SEPARATOR;
    }

    public List<TridentTuple> getTridentTuples(List<TridentBatchTuple> tridentBatchTuples) {
        List<TridentTuple> resultTuples = new ArrayList<>();
        List<String> keys = new ArrayList<>();
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

    public TridentTuple collectTridentTupleOrKey(TridentBatchTuple tridentBatchTuple, List<String> keys) {
        if (tridentBatchTuple.tridentTuple != null) {
            return tridentBatchTuple.tridentTuple;
        }
        keys.add(tupleKey(tridentBatchTuple));
        return null;
    }

    public void onTuplesExpired(List<TridentBatchTuple> expiredTuples) {
        if (maxCachedTuplesSize != null) {
            currentCachedTuplesSize.addAndGet(-expiredTuples.size());
        }

        List<String> keys = new ArrayList<>();
        for (TridentBatchTuple expiredTuple : expiredTuples) {
            keys.add(tupleKey(expiredTuple));
        }

        windowStore.removeAll(keys);
    }

    private String tupleKey(TridentBatchTuple tridentBatchTuple) {
        return tridentBatchTuple.batchId + String.valueOf(tridentBatchTuple.tupleIndex);
    }

}
