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

import storm.trident.state.map.MapState;

/**
 * Abstract window policy with {@link Evictor}, {@link Trigger} and {@link MapState}.
 */
public abstract class WindowPolicy {

    protected Evictor evictor;
    protected Trigger trigger;
    protected MapState<WindowState> mapState;

    public Evictor getEvictor() {
        return evictor;
    }

    public void setEvictor(Evictor evictor) {
        this.evictor = evictor;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }

    public MapState getMapState() {
        return mapState;
    }

    public void setMapState(MapState<WindowState> mapState) {
        this.mapState = mapState;
    }
}
