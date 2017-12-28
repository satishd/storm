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

import java.util.Map;
import java.util.Properties;

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
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * Trident kafka spout with no-op processor and routing to target kafka topic.
 */
public class TridentKafkaSpoutNullBoltTopology {

    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String ZK_URI = "zk.uri";
    public static final String KAFKA_BROKER_URL = "kafka.broker.url";
    public static final String PRODUCER_SPOUT_PARALLELISM = "producer.spout.parallelism";
    public static final String PRODUCER_BOLT_PARALLELISM = "producer.bolt.parallelism";
    public static final String CONSUMER_SPOUT_PARALLELISM = "consumer.spout.parallelism";
    public static final String CONSUMER_BOLT_PARALLELISM = "consumer.bolt.parallelism";

    public static StormTopology getTopology(Map<String, Object> config) {
        TridentTopology tridentTopology = new TridentTopology();

        String zkConnString = config.getOrDefault(ZK_URI, "localhost:2181").toString();
        String topicName = config.getOrDefault(KAFKA_TOPIC, "trident-topic").toString();

        BrokerHosts brokerHosts = new ZkHosts(zkConnString);

        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(brokerHosts, topicName);
        int fetchSizeBytes = (int) config.getOrDefault("fetchSizeMb", 1) * 1024 * 1024;
        int bufferSizeBytes = (int) config.getOrDefault("bufferSizeMb", 1) * 1024 * 1024;
        tridentKafkaConfig.fetchSizeBytes = fetchSizeBytes;
        tridentKafkaConfig.bufferSizeBytes = bufferSizeBytes;

        TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);
        Stream stream = tridentTopology.newStream("trident-kafka", kafkaSpout)
                                       .parallelismHint((int) config.getOrDefault(CONSUMER_SPOUT_PARALLELISM, 2));
        stream.shuffle()
              .map((MapFunction) input -> new Values(input.getValues()))
              .parallelismHint((int) config.getOrDefault(CONSUMER_BOLT_PARALLELISM, 2));

        return tridentTopology.build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("args: runDurationSec config-file");
            return;
        }

        String confFile = null;
        if (args.length >= 2) {
            confFile = args[1];
        }

        Integer durationSec = Integer.parseInt(args[0]);

        Map<String, Object> topoConf =
                confFile != null ? Utils.findAndReadConfigFile(confFile) : Utils.readDefaultConfig();
        // topoConf.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, 1000);
        // topoConf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.0005);
        // topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);
        // topoConf.put(Config.TOPOLOGY_BOLT_WAIT_STRATEGY, "org.apache.storm.policy.WaitStrategyPark");
        // topoConf.put(Config.TOPOLOGY_BOLT_WAIT_PARK_MICROSEC, 0);
        // topoConf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // topoConf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topicName = (String) topoConf.getOrDefault(KAFKA_TOPIC, "trident-topic");
        String brokerUrl = (String) topoConf.getOrDefault(KAFKA_BROKER_URL, "localhost:9092");
        StormTopology producerTopology = newProducerTopology(brokerUrl,
                                                             topicName,
                                                             (int) topoConf.getOrDefault(PRODUCER_SPOUT_PARALLELISM, 2),
                                                             (int) topoConf.getOrDefault(PRODUCER_BOLT_PARALLELISM, 2)
                                                            );
        String producerTopologyName = "producer-topology-" + System.currentTimeMillis();

        StormSubmitter.submitTopology(producerTopologyName, topoConf, producerTopology);
        Helper.setupShutdownHook(producerTopologyName);

        // Submit topology to storm cluster
        Helper.runOnClusterAndPrintMetrics(durationSec,
                                           "trident-const-kafka-spout-" + System.currentTimeMillis(),
                                           topoConf,
                                           getTopology(topoConf));
    }

    public static StormTopology newProducerTopology(String brokerUrl,
                                                    String topicName,
                                                    int spoutParallelism,
                                                    int boltParallelism) {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new StringGenSpout(64).withFieldName("word"), spoutParallelism);

        /* The output field of the RandomSentenceSpout ("word") is provided as the boltMessageField
          so that this gets written out as the message in the kafka topic. */
        final KafkaBolt<String, String> bolt = new KafkaBolt<String, String>()
                .withProducerProperties(newProps(brokerUrl))
                .withTopicSelector(new DefaultTopicSelector(topicName))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "word"));

        builder.setBolt("forwardToKafka", bolt, boltParallelism).shuffleGrouping("spout");

        return builder.createTopology();
    }

    /**
     *
     * @return the Storm config for the topology that publishes sentences to kafka using a kafka bolt.
     */
    private static Properties newProps(final String brokerUrl) {
        return new Properties() {
            {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                put(ProducerConfig.CLIENT_ID_CONFIG, "trident-topology");
            }
        };
    }

}
