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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.perf.spout.StringGenSpout;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class TridentKafkaSpoutNullBoltTopology {

    public static final String KAFKA_TOPIC = "kafka.topic";

    public static StormTopology getTopology(Map<String, Object> config) {
        TridentTopology tridentTopology = new TridentTopology();

        String zkConnString = config.getOrDefault("zk.uri", "localhost:2181").toString();
        String topicName = config.getOrDefault(KAFKA_TOPIC, "trident-topic").toString();

        BrokerHosts brokerHosts = new ZkHosts(zkConnString);

        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(brokerHosts, topicName);
        TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);
        Stream stream = tridentTopology.newStream("trident-kafka", kafkaSpout);
        stream.map(new MapFunction() {
            @Override
            public Values execute(TridentTuple input) {
                return new Values(input.getValues());
            }
        });

        return tridentTopology.build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("args: runDurationSec");
            return;
        }

        Integer durationSec = Integer.parseInt(args[0]);
        String topicName = "trident-topic";

        Map<String, Object> topoConf = Utils.readDefaultConfig();
//        topoConf.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, 1000);
//        topoConf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.0005);
//        topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);
//        topoConf.put(Config.TOPOLOGY_BOLT_WAIT_STRATEGY, "org.apache.storm.policy.WaitStrategyPark");
//        topoConf.put(Config.TOPOLOGY_BOLT_WAIT_PARK_MICROSEC, 0);
        topoConf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        topoConf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        topoConf.put(KAFKA_TOPIC, topicName);

        StormTopology producerTopology = newProducerTopology("localhost:9092", topicName);
        String producerTopologyName = "producer-topology";

        StormSubmitter.submitTopology(producerTopologyName, topoConf, producerTopology);
        Helper.setupShutdownHook(producerTopologyName);

        //  Submit topology to storm cluster
        Helper.runOnClusterAndPrintMetrics(durationSec,
                                           "trident-const-kafka-spout",
                                           topoConf,
                                           getTopology(topoConf));
    }


    public static StormTopology newProducerTopology(String brokerUrl, String topicName) {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new StringGenSpout(64).withFieldName("word"), 2);

        /* The output field of the RandomSentenceSpout ("word") is provided as the boltMessageField
          so that this gets written out as the message in the kafka topic. */
        final KafkaBolt<String, String> bolt = new KafkaBolt<String, String>()
                .withProducerProperties(newProps(brokerUrl))
                .withTopicSelector(new DefaultTopicSelector(topicName))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "word"));

        builder.setBolt("forwardToKafka", bolt, 1).shuffleGrouping("spout");

        return builder.createTopology();
    }

    /**
     * @return the Storm config for the topology that publishes sentences to kafka using a kafka bolt.
     */
    private static Properties newProps(final String brokerUrl) {
        return new Properties() {{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            put(ProducerConfig.CLIENT_ID_CONFIG, "trident-topology");
        }};
    }

}
