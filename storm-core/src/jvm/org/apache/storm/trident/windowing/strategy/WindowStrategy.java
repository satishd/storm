/**
 *
 */
package org.apache.storm.trident.windowing.strategy;

import org.apache.storm.windowing.EvictionPolicy;
import org.apache.storm.windowing.TriggerPolicy;
import org.apache.storm.windowing.TriggerHandler;
/**
 *
 */
public interface WindowStrategy<T> {

//    public TriggerPolicy<TridentBatchTuple> getTriggerPolicy(TriggerHandler triggerHandler, EvictionPolicy<TridentBatchTuple> evictionPolicy);
//
//    public EvictionPolicy<TridentBatchTuple> getEvictionPolicy();
//

    public TriggerPolicy<T> getTriggerPolicy(TriggerHandler triggerHandler, EvictionPolicy<T> evictionPolicy);

    public EvictionPolicy<T> getEvictionPolicy();
}
