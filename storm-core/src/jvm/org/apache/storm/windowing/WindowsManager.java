/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.windowing;

import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages multiple active windows for a bolt/operation.
 */
public class WindowsManager implements IWindowManager {

    private ConcurrentMap<WindowInfo, WindowManager<Tuple>> activeWindows = new ConcurrentHashMap<>();
    private WindowConfig windowConfig;
    private WindowLifecycleListener<Tuple> windowsLifeCycleListener;
    private Map<ITuple, Integer> tupleStatus = new ConcurrentHashMap<>();

    public WindowsManager(WindowConfig windowConfig, WindowLifecycleListener<Tuple> windowLifeCycleListener) {
        this.windowConfig = windowConfig;
        // todo create windowLifeCycleListener
        windowsLifeCycleListener = new WindowsLifecycleListener(windowLifeCycleListener);
    }

    public class WindowsLifecycleListener implements WindowLifecycleListener<Tuple> {

        private final WindowLifecycleListener<Tuple> rootListener;
        // todo need to check how it is handled in retries of tuples

        public WindowsLifecycleListener(WindowLifecycleListener<Tuple> rootListener) {
            this.rootListener = rootListener;
        }

        @Override
        public void onExpiry(List<Tuple> tuples) {
            for (Tuple tuple : tuples) {
                if(decrementTupleStatusCounter(tuple)) {
                    rootListener.onExpiry(Collections.singletonList(tuple));
                }
            }
        }

        @Override
        public void onActivation(List<Tuple> events, List<Tuple> newEvents, List<Tuple> expired) {
            rootListener.onActivation(events, newEvents, expired);
        }
    }

    public void shutdown() {
        for (WindowManager<Tuple> windowManager : activeWindows.values()) {
            windowManager.shutdown();
        }
    }

    public void add(Tuple tuple) {
        add(tuple, System.currentTimeMillis());
    }

    public void add(Tuple tuple, long timeStamp) {
        final List<WindowInfo> windowInfos = windowConfig.assignWindows(tuple);
        for (WindowInfo windowInfo : windowInfos) {
            WindowManager<Tuple> windowManager = null;
            if(!activeWindows.containsKey(windowInfo)) {
                windowManager = new WindowManager<>(windowsLifeCycleListener);
                // no need to check for existing value as this is invoked from bolt task's executor only.
                activeWindows.putIfAbsent(windowInfo, windowManager);
            } else {
                windowManager = activeWindows.get(windowInfo);
            }

            windowManager.add(tuple, timeStamp);
        }

        incrementTupleStatusCounter(tuple);
    }

    private void incrementTupleStatusCounter(Tuple tuple) {
        final Integer ct = tupleStatus.get(tuple);
        tupleStatus.put(tuple, ct != null ? ct+1 : 1);
    }

    private boolean decrementTupleStatusCounter(Tuple tuple) {
        final Integer ct = tupleStatus.get(tuple);
        if(ct == null) {
            throw new RuntimeException("Tuple should have been added before its count is decremented");
        }
        if(ct == 1) {
            tupleStatus.remove(tuple);
            return true;
        }

        tupleStatus.put(tuple, ct-1);
        return false;
    }

}
