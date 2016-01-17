/**
 *
 */
package org.apache.storm.trident.windowing.strategy;

import org.apache.storm.windowing.EvictionPolicy;
import org.apache.storm.windowing.TriggerHandler;
import org.apache.storm.windowing.TriggerPolicy;
/**
 * Strategy for windowing which will have respective trigger and eviction policies.
 */
public interface WindowStrategy<T> {

    /**
     * Returns a {@code TriggerPolicy}  by creating with {@code triggerHandler} and {@code evictionPolicy} with
     * the given configuration.
     *
     * @param triggerHandler
     * @param evictionPolicy
     * @return
     */
    public TriggerPolicy<T> getTriggerPolicy(TriggerHandler triggerHandler, EvictionPolicy<T> evictionPolicy);

    /**
     * Returns an {@code EvictionPolicy} instance for this strategy with the given configuration.
     *
     * @return
     */
    public EvictionPolicy<T> getEvictionPolicy();
}
