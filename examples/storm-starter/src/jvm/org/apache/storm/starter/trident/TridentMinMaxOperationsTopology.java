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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.NumberGeneratorSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


public class TridentMinMaxOperationsTopology {
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static StormTopology buildNumbersTopology() {
        NumberGeneratorSpout spout = new NumberGeneratorSpout(new Fields("id"), 10, 1000);

        TridentTopology topology = new TridentTopology();
        Stream wordsStream = topology.newStream("numgen-spout", spout).
                each(new Fields("id"), new Debug("##### ids"));

        wordsStream.min(new Fields("id"), new Fields("min-id")).
                each(new Fields("min-id"), new Debug("#### min-id"));

        wordsStream.max(new Fields("id"), new Fields("max-id")).
                each(new Fields("max-id"), new Debug("#### max-id"));

        return topology.build();
    }

    public static StormTopology buildWordsTopology() {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream wordsStream = topology.newStream("spout1", spout).parallelismHint(16).
                each(new Fields("sentence"), new Split(), new Fields("word")).
                each(new Fields("word"), new Debug("##### words"));

        wordsStream.min(new Fields("word"), new Fields("lowest")).
                each(new Fields("lowest"), new Debug("#### lowest word"));

        wordsStream.max(new Fields("word"), new Fields("highest")).
                each(new Fields("highest"), new Debug("#### highest word"));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        StormTopology[] topologies = {buildWordsTopology(), buildNumbersTopology()};
        if (args.length == 0) {
            for (StormTopology topology : topologies) {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("min-max-topology", conf, topology);
                Utils.sleep(60*1000);
                cluster.shutdown();
            }
            System.exit(0);
        } else {
            conf.setNumWorkers(3);
            int ct=1;
            for (StormTopology topology : topologies) {
                StormSubmitter.submitTopologyWithProgressBar(args[0]+"-"+ct++, conf, topology);
            }
        }
    }
}
