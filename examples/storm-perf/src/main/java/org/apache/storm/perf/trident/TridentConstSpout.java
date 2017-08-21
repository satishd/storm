/*
 * Copyright 2016 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.perf.trident;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class TridentConstSpout implements IBatchSpout {
    private static final Logger LOG = LoggerFactory.getLogger(TridentConstSpout.class);

    public static final int DEFAULT_BATCH_SIZE = 100;
    private String fieldName;
    private final String value;
    private Integer batchSize;

    public TridentConstSpout(String value) {
        this(null, value, null);
    }

    public TridentConstSpout(String fieldName, String value, Integer batchSize) {
        this.fieldName = fieldName != null ? fieldName : "field-str";
        this.value = value;
        this.batchSize = batchSize;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context) {
        if (batchSize == null) {
            batchSize = (Integer) conf.getOrDefault("trident.batch.size", DEFAULT_BATCH_SIZE);
        }
        LOG.info("##### batchSize: [{}] with field value as [{}]", batchSize, value);
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
//        LOG.info("In emit, batch: {}", batchId);
        for (int i = 0; i < batchSize; i++) {
            collector.emit(Collections.singletonList(value));
        }
    }

    @Override
    public void ack(long batchId) {

    }

    @Override
    public void close() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(fieldName);
    }
}
