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
package org.apache.storm.starter.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 *
 */
public class TridentWindowingInmemoryStoreTopology {

    public static StormTopology buildTopology(WindowsStoreFactory mapState) throws Exception {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
//        TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(16).each(new Fields("sentence"),
//                new Split(), new Fields("word")).groupBy(new Fields("word")).persistentAggregate(new MemoryMapState.Factory(),
//                new Count(), new Fields("count")).parallelismHint(16);

        Stream stream = topology.newStream("spout1", spout).parallelismHint(16).each(new Fields("sentence"),
                new Split(), new Fields("word")).
//                tumblingWindow(Duration.ofSeconds(10), mapState, new Fields("word"), null, new Fields("words"))
        tumblingWindow(1000, mapState, new Fields("word"), new CountAsAggregator(), new Fields("count")).parallelismHint(2)
//        tumblingWindow(100, mapState, new Fields("word"), new EchoAggregator(), new Fields("count"))
//                .aggregate(new Fields("count"), new Count(), new Fields("count-p"))
//                .aggregate(new Fields("count-p"), new Count(), new Fields("count-aggr"))
                .each(new Fields("count"), new Debug())
                .each(new Fields("count"), new Echo(), new Fields("ct"))
                .each(new Fields("ct"), new Debug());

        return topology.build();
    }

    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
//                System.out.println("############ splitting..");
                collector.emit(new Values(word));
            }
        }
    }

    static class Echo implements Function {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            System.out.println("##########Echo.execute: " + tuple);
            collector.emit(tuple.getValues());
        }

        @Override
        public void prepare(Map conf, TridentOperationContext context) {

        }

        @Override
        public void cleanup() {

        }
    }

    static class EchoAggregator extends BaseAggregator<EchoAggregator.State> {
        @Override
        public State init(Object batchId, TridentCollector collector) {
            return new State();
        }

        @Override
        public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
            System.out.println(String.format("############ %s :: %s", tuple.getFields(), tuple.getValues()));
            state.val++;
        }

        @Override
        public void complete(State state, TridentCollector collector) {
            collector.emit(new Values(state.val));
        }

        static class State {
            int val;
        }
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        WindowsStoreFactory mapState = new InMemoryWindowsStoreFactory();
        System.out.println("############ Using inmemory store..");

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(mapState));
            Utils.sleep(60 * 1000);
            cluster.shutdown();
            System.exit(1);
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(mapState));
        }
    }

}
