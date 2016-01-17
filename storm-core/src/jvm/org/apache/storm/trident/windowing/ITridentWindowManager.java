/**
 *
 */
package org.apache.storm.trident.windowing;

import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Queue;

/**
 * Window manager to handle trident tuple events.
 */
public interface ITridentWindowManager {

    /**
     * This is invoked from {@code org.apache.storm.trident.planner.TridentProcessor}'s  prepare method. So any
     * initialization tasks can be done before the topology starts accepting tuples. For ex:
     * initialize window manager with any earlier stored tuples/triggers and start WindowManager
     */
    public void prepare();

    /**
     * This is invoked when from {@code org.apache.storm.trident.planner.TridentProcessor}'s  cleanup method. So, any
     * cleanup operations like clearing cache or close store connection etc can be done.
     */
    public void shutdown();

    /**
     * Add received batch of tuples to cache/store and add them to {@code WindowManager}
     *
     * @param batchId
     * @param tuples
     */
    public void addTuplesBatch(Object batchId, List<TridentTuple> tuples);

    /**
     * Returns pending triggers to be emitted.
     *
     * @return
     */
    public Queue<StoreBasedTridentWindowManager.TriggerResult> getPendingTriggers();

}
