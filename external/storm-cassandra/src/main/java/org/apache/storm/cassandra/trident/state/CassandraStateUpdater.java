package org.apache.storm.cassandra.trident.state;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 *
 */
public class CassandraStateUpdater extends BaseStateUpdater<CassandraState> {

    @Override
    public void updateState(CassandraState state, List<TridentTuple> list, TridentCollector collector) {
        state.updateState(list, collector);
    }
}
