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
 * This class implements a policy to evict tuples in the given {@link TridentWindow}
 */
public interface Evictor {

    /**
     * Specifies to evict all the elements in the window.
     */
    public static final Evictor ALL = new Evictor() {
        @Override
        public int evict(TridentWindow tridentWindow) {
            return tridentWindow.get().size();
        }
    };

    /**
     * Specifies to evict none of the elements in the window.
     */
    public static final Evictor NONE = new Evictor() {
        @Override
        public int evict(TridentWindow tridentWindow) {
            return 0;
        }
    };

    /**
     *
     * @param tridentWindow window of tuples for which no of evicted elements to be computed.
     * @return number of elements to be evicted from start
     */
    public int evict(TridentWindow tridentWindow);
}
