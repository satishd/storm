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
package org.apache.storm.trident.windowing;

/**
 * InMemoryWindowsStoreFactory contains a single instance which will be used for storing tuples and triggers of the
 * window in a bolt containing windowing task.
 *
 */
public class InMemoryWindowsStoreFactory implements WindowsStoreFactory {

    private InMemoryWindowsStore inMemoryWindowsStore;

    @Override
    public WindowsStore create(String baseId) {
        // baseId is ignored here, as it is inmemory and one store instance for each task.
        if(inMemoryWindowsStore == null) {
            inMemoryWindowsStore = new InMemoryWindowsStore();
        }
        return inMemoryWindowsStore;
    }
}
