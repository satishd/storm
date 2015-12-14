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
package storm.trident.windowing;

import storm.trident.Stream;

/**
 * This class implements
 */
public interface Trigger {

    /**
     * {@code FIRE} represents whether to send the @{TridentWindow} for computing with the associated function.
     * {@code SKIP} represents whether to skip and continue accumulating tuples.
     * {@cide FINISH} represents whether this window is finished and it will slide or tumble to new window according to the {@WindowPolicy}
     */
    enum Action {FIRE, SKIP, FINISH}

    /**
     * Evaluates what {@link Action} to be taken after evaluating given event.
     *
     * @param tupleEvent
     * @return returns Action whether to fire/skip/finish
     */
//        public Action evaluate(Event tupleEvent);
}
