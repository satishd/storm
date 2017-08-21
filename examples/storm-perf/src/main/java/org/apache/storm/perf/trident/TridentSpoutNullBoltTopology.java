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

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 *
 */
public class TridentSpoutNullBoltTopology {
    public static final Logger LOG = LoggerFactory.getLogger(TridentSpoutNullBoltTopology.class);

    public static StormTopology getTopology(int batchsize) {
        TridentTopology topology = new TridentTopology();
        String fieldName = "f-1";
        topology.newStream("spout1", new TridentConstSpout(fieldName, "foo-" + new Date(), batchsize))
                .parallelismHint(4)
                .each(new Fields(fieldName), new BaseFilter() {
                    @Override
                    public boolean isKeep(TridentTuple tuple) {
//                        String field = tuple.getStringByField(fieldName);
//                        LOG.info("############## field = [{}]", field);
                        return true;
                    }
                }).parallelismHint(8);

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        int runTime = -1;
        Config topoConf = new Config();
        int batchSize = 8*1024;
        if (args.length > 1) {
            batchSize = Integer.parseInt(args[0]);
            runTime = Integer.parseInt(args[1]);
        }
        if (args.length > 2) {
            topoConf.putAll(Utils.findAndReadConfigFile(args[2]));
        }
        if (args.length > 3) {
            System.err.println("args: [batchSize] [runDurationSec]  [optionalConfFile]");
            return;
        }

        //  Submit topology to storm cluster
        Helper.runOnClusterAndPrintMetrics(runTime, "trident-const-value-spout", topoConf, getTopology(batchSize));
    }

}
