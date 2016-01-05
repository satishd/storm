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
package storm.starter.trident;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Debug;
import storm.trident.planner.processor.windowing.InmemoryMapStoreFactory;
import storm.trident.planner.processor.windowing.MapStoreFactory;
import storm.trident.planner.processor.windowing.WindowsStateProcessor;
import storm.trident.testing.CountAsAggregator;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TridentWindowing {
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

    public static StormTopology buildTopology(MapStoreFactory<byte[], Collection<TridentTuple>> mapState) throws Exception {
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
                tumblingWindow(1000, mapState, new Fields("word"), new CountAsAggregator(), new Fields("count"))
//                .aggregate(new Count(), new Fields("count"))
//                .aggregate(new Count(), new Fields("count-aggr"))
                .each(new Fields("count"), new Debug());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        boolean useHbaseStore = Boolean.getBoolean("window.store.useHBase");
        System.out.println("####### useHbaseStore = " + useHbaseStore);
        MapStoreFactory<byte[], Collection<TridentTuple>> mapState = null;
        if(!useHbaseStore) {
            System.out.println("############ Using inmemory store..");
            mapState = new InmemoryMapStoreFactory<>();
        } else {
            System.out.println("############ Using HBase map store..");
            mapState = new HBaseMapStoreFactory(new HashMap<String, Object>(), "window_state", "cf".getBytes("UTF-8"), "tuples".getBytes("UTF-8"));
        }

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(mapState));
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(mapState));
        }
    }

    static class HBaseMapStoreFactory implements MapStoreFactory<byte[], Collection<TridentTuple>> {

        private final Map<String, Object> config;
        private final String tableName;
        private final byte[] family;
        private final byte[] qualifier;

        public HBaseMapStoreFactory(Map<String, Object> config, String tableName, byte[] family, byte[] qualifier) {
            this.config = config;
            this.tableName = tableName;
            this.family = family;
            this.qualifier = qualifier;
        }

        @Override
        public WindowsStateProcessor.MapStore<byte[], Collection<TridentTuple>> create() {
            Configuration configuration = HBaseConfiguration.create();
            for (Map.Entry<String, Object> entry : config.entrySet()) {
                if(entry.getValue() != null) {
                    configuration.set(entry.getKey(), entry.getValue().toString());
                }
            }
            return new HBaseMapStore(configuration, tableName, family, qualifier);
        }
    }

    static class HBaseMapStore implements WindowsStateProcessor.MapStore<byte[], Collection<TridentTuple>> {

        private final HTable htable;
        private byte[] family;
        private byte[] qualifier;

        public HBaseMapStore(Configuration config, String tableName, byte[] family, byte[] qualifier) {
            this.family = family;
            this.qualifier = qualifier;
            try {
                htable = new HTable(config, tableName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Collection<TridentTuple> get(byte[] key) {
            Get get = new Get(key);
            Result result = null;
            try {
                result = htable.get(get);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Kryo kryo = new Kryo();
            Input input = new Input(result.getValue(family, qualifier));
            Collection tuples = kryo.readObject(input, Collection.class);
            return tuples;
        }

        @Override
        public Collection<Collection<TridentTuple>> getWithPrefixKey(byte[] prefix) {
            Scan scan = new Scan(prefix);
            scan.setFilter(new PrefixFilter(prefix));
            ResultScanner resultScanner = null;
            try {
                resultScanner = htable.getScanner(scan);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Iterator<Result> iterator = resultScanner.iterator();
            Kryo kryo = new Kryo();
            List<Collection<TridentTuple>> tuplesList = new LinkedList<>();
            for (Result result : resultScanner) {
                Input input = new Input(result.getValue(family, qualifier));
                Collection tuples = kryo.readObject(input, Collection.class);
                tuplesList.add(tuples);
            }
            return tuplesList;
        }

        @Override
        public void put(byte[] bytes, Collection<TridentTuple> tridentTuples) {
            Put put = new Put(bytes, System.currentTimeMillis());
            Kryo kryo = new Kryo();
            Output output = new Output(new ByteArrayOutputStream());
            kryo.writeObject(output, tridentTuples);
            put.add(family, ByteBuffer.wrap(qualifier), System.currentTimeMillis(), ByteBuffer.wrap(output.getBuffer(), 0, output.position()));
            try {
                htable.put(put);
            } catch (InterruptedIOException | RetriesExhaustedWithDetailsException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean remove(byte[] bytes) {
            return false;
        }

    }

}
