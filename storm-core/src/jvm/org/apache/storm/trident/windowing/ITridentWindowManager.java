/**
 *
 */
package org.apache.storm.trident.windowing;

import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Queue;

/**
 *
 */
public interface ITridentWindowManager<T> {

    public void prepare();

    public void shutdown();

    public void addTuplesBatch(Object batchId, List<TridentTuple> tuples);

    public Queue<TridentWindowManager.TriggerResult> getPendingTriggers();

    public WindowsStore.Key triggerKey(Integer currentTriggerId);
}
