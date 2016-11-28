/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.cassandra;

import com.datastax.driver.core.ResultSet;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.cassandra.bolt.BaseCassandraBolt;
import org.apache.storm.cassandra.bolt.BatchCassandraWriterBolt;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.cassandra.query.builder.BoundCQLStatementMapperBuilder;
import org.apache.storm.cassandra.query.selector.FieldSelector;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class WordCountTopology {

    public static final String WEATHER_SPOUT = "weather_spout";
    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("schema.cql", "weather"));

    @org.junit.Test
    public void testTopology() throws Exception {
        CQLStatementTupleMapper mapper = createCQLStatementTupleMapper();
        int maxQueries = 100;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(WEATHER_SPOUT, new WeatherSpout("test", maxQueries), 1).setMaxTaskParallelism(1);

        BatchCassandraWriterBolt bolt = new BatchCassandraWriterBolt(mapper).withTickFrequency(1, TimeUnit.SECONDS);
        builder.setBolt("bolt", bolt, 1).setMaxTaskParallelism(1).shuffleGrouping(WEATHER_SPOUT);

        runLocalTopologyAndWait(builder);

        ResultSet rows = cassandraCQLUnit.session.execute("SELECT * FROM temperature WHERE weatherstation_id='test'");
        Assert.assertEquals(maxQueries, rows.all().size());

    }

    private CQLStatementTupleMapper createCQLStatementTupleMapper() {
        String cql = "INSERT INTO weather.temperature (weatherstation_id, event_time, temperature) VALUES (?, ?, ?)";
        BoundCQLStatementMapperBuilder mapperBuilder = new BoundCQLStatementMapperBuilder(cql);
        mapperBuilder = mapperBuilder.bind(new FieldSelector("weatherStationId").as("weatherstation_id"),
                                           new FieldSelector("eventTime").as("event_time"),
                                           new FieldSelector("temperature").as("temperature")
        );

        return mapperBuilder.build();
    }

    protected void runLocalTopologyAndWait(TopologyBuilder builder) {
        LocalCluster cluster = new LocalCluster();
        StormTopology topology = builder.createTopology();
        Config config = getConfig();
        cluster.submitTopology("my-cassandra-topology", config, topology);

        Utils.sleep(TimeUnit.SECONDS.toMillis(30));

        cluster.killTopology("my-cassandra-topology");
        cluster.shutdown();
    }

    protected Config getConfig() {
        Config config = new Config();
        config.put("cassandra.keyspace", "weather");
        config.put("cassandra.nodes", "localhost");
        config.put("cassandra.port", "9142");
        return config;
    }
}
