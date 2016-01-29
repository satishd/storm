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
package org.apache.storm.trident.operation.builtin;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 *
 */
public class Min implements CombinerAggregator<Comparable<Object>> {

    @Override
    public Comparable<Object> init(TridentTuple tuple) {
        return (Comparable<Object>) tuple.getValue(0);
    }

    @Override
    public Comparable<Object> combine(Comparable<Object> val1, Comparable<Object> val2) {
        if(val1 == null) return val2;
        if(val2 == null) return val1;
        return val1.compareTo(val2) > 0 ? val2 : val1;
    }

    @Override
    public Comparable<Object> zero() {
        return null;
    }
}
