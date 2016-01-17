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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class InMemoryWindowsStore implements WindowsStore, Serializable {

    private ConcurrentHashMap<String, Map<String, Object>> primaryKeyStore = new ConcurrentHashMap<>();

    public InMemoryWindowsStore() {
    }

    @Override
    public Object get(Key key) {
        Map<String, Object> primaryKeyContainer = primaryKeyStore.get(key.primaryKey);
        if(primaryKeyContainer == null) {
            return null;
        }
        return primaryKeyContainer.get(key.secondaryKey);
    }

    @Override
    public Iterable<Map.Entry<String, Map<String, Object>>> getAllEntries() {
        final Iterator<Map.Entry<String, Map<String, Object>>> iterator = new UnmodifiableIterator<>(primaryKeyStore.entrySet().iterator());

        return new Iterable<Map.Entry<String, Map<String, Object>>>() {
            @Override
            public Iterator<Map.Entry<String, Map<String, Object>>> iterator() {
                return iterator;
            }
        };
    }

    static class UnmodifiableIterator<E> implements Iterator<E> {

        private final Iterator<E> delegate;

        public UnmodifiableIterator(Iterator<E> iterator) {
            delegate = iterator;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public E next() {
            return delegate.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove operation is not supported as it is immutable.");
        }
    }

    @Override
    public void put(Key key, Object value) {
        Map<String, Object> primaryKeyContainer = primaryKeyStore.get(key.primaryKey);
        if(primaryKeyContainer == null) {
            primaryKeyContainer = new ConcurrentHashMap<>();
            primaryKeyStore.put(key.primaryKey, primaryKeyContainer);
        }
        primaryKeyContainer.put(key.secondaryKey, value);
    }

    @Override
    public void remove(Key key) {
        Map<String, Object> primaryKeyContainer = primaryKeyStore.get(key.primaryKey);
        if(primaryKeyContainer == null) {
            throw new IllegalStateException("no value exists for given key's primary-key");
        }

        primaryKeyContainer.remove(key.secondaryKey);
        if(primaryKeyContainer.isEmpty()) {
            primaryKeyStore.remove(key.primaryKey, Collections.emptyMap());
        }
    }

    @Override
    public void removeAll(Collection<Key> keys) {
        for (Key key : keys) {
            remove(key);
        }
    }

    @Override
    public void shutdown() {
        primaryKeyStore.clear();
    }

    @Override
    public String toString() {
        return "InMemoryWindowsStore{" +
                "primaryKeyStore:size =" + primaryKeyStore.size() +
                '}';
    }
}
