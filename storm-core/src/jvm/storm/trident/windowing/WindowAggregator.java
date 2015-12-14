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
package storm.trident.windowing;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class WindowAggregator implements Aggregator<WindowState> {

    private final WindowPolicy windowPolicy;

    private List<WindowState> batchIdVsState = new ArrayList<>();

    public WindowAggregator(WindowPolicy windowPolicy) {
        this.windowPolicy = windowPolicy;
    }

    @Override
    public WindowState init(Object batchId, TridentCollector collector) {
        return new WindowState(batchId, collector);
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void aggregate(WindowState windowState, TridentTuple tuple, TridentCollector collector) {
        windowState.addTuple(tuple);
    }

    @Override
    public void complete(WindowState val, TridentCollector collector) {
        // minimum trigger window/length would be of a batch size
        // so, effectively triggers are run for each batch or multiple batches after merging the current batch with the existing batches
        // while a batch is completed, see whether it is required to run trigger
        // if it is not so then add with the next batch
        // after trigger is run and it determines the action to be skipped/continue then save the current window snapshot
        // in a state
        // check whether we can evaluate triggers after merging the tuples of the current batches


        // we can have a persistable-state taken from the user, which can store the window state at complete or trigger
        // apis

        batchIdVsState.add(val);

        // evaluate states to fire a pane if it is required

    }

    @Override
    public void cleanup() {

    }

}
