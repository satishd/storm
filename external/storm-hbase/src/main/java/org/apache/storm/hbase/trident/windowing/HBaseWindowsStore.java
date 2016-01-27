/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.hbase.trident.windowing;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.storm.trident.windowing.WindowsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class HBaseWindowsStore implements WindowsStore {
    private static final Logger log = LoggerFactory.getLogger(HBaseWindowsStore.class);
    public static final String UTF_8 = "utf-8";

    private final HTable htable;
    private byte[] baseId;
    private byte[] family;
    private byte[] qualifier;

    public HBaseWindowsStore(Configuration config, String baseId, String tableName, byte[] family, byte[] qualifier) {
        this.family = family;
        this.qualifier = qualifier;

        if(baseId != null) {
            this.baseId = baseId.getBytes();
        }
        try {
            htable = new HTable(config, tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] effectiveKey(byte[] key) {

        if(baseId == null) {
            return key;
        }

        byte[] effectiveKey = new byte[baseId.length + key.length];
        System.arraycopy(baseId, 0, effectiveKey, 0, baseId.length);
        System.arraycopy(key, 0, effectiveKey, baseId.length, effectiveKey.length);
        return effectiveKey;
    }

    private byte[] effectiveKey(Key key) {
        try {
            return (key.primaryKey + "|" + key.secondaryKey).getBytes(UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private Key createKey(byte[] bytes) {
        try {
            String string = new String(bytes, UTF_8);
            int index = string.lastIndexOf("|");
            if(index == -1) {
                throw new RuntimeException("Invalid key without a separator");
            }
            return new Key(string.substring(0, index), string.substring(index+1));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Object get(Key key) {
        WindowsStore.Entry.nonNullCheckForKey(key);

        byte[] effectiveKey = effectiveKey(key);
        Get get = new Get(effectiveKey);
        Result result = null;
        try {
            result = htable.get(get);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if(result.isEmpty()) {
            return null;
        }

        Kryo kryo = new Kryo();
        Input input = new Input(result.getValue(family, qualifier));
        Object resultObject = kryo.readClassAndObject(input);
        return resultObject;

    }

    @Override
    public Iterable<Object> get(List<Key> keys) {
        List<Get> gets = new ArrayList<>();
        for (Key key : keys) {
            WindowsStore.Entry.nonNullCheckForKey(key);

            byte[] effectiveKey = effectiveKey(key);
            gets.add(new Get(effectiveKey));
        }

        Result[] results = null;
        try {
            results = htable.get(gets);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Kryo kryo = new Kryo();
        List<Object> values = new ArrayList<>();
        for (int i=0; i<results.length; i++) {
            Result result = results[i];
            if(result.isEmpty()) {
                log.error("Got empty result for key [{}]", keys.get(i));
                throw new RuntimeException("Received empty result for key: "+keys.get(i));
            }
            Input input = new Input(result.getValue(family, qualifier));
            Object resultObject = kryo.readClassAndObject(input);
            values.add(resultObject);
        }

        return values;
    }

    @Override
    public Iterable<WindowsStore.Entry> getAllKeys() {
        //todo-sato implement this functionality
        Scan scan = new Scan();

        final Iterator<Result> resultIterator;
        try {
            resultIterator = htable.getScanner(scan).iterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final Kryo kryo = new Kryo();
        final Iterator<WindowsStore.Entry> iterator = new Iterator<WindowsStore.Entry>() {
            @Override
            public boolean hasNext() {
                return resultIterator.hasNext();
            }

            @Override
            public WindowsStore.Entry next() {
                Result result = resultIterator.next();
                Input input = new Input(result.getValue(family, qualifier));
                Object value = kryo.readClassAndObject(input);
                Key key = createKey(result.getRow());
                return new WindowsStore.Entry(key, value);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove operation is not supported");
            }
        };

        return new Iterable<WindowsStore.Entry>() {
            @Override
            public Iterator<WindowsStore.Entry> iterator() {
                return iterator;
            }
        };
    }

    @Override
    public void put(Key key, Object value) {
        WindowsStore.Entry.nonNullCheckForKey(key);
        WindowsStore.Entry.nonNullCheckForValue(value);

        if(value == null) {
            throw new IllegalArgumentException("Invalid value of null with key: "+key);
        }
        Put put = new Put(effectiveKey(key), System.currentTimeMillis());
        Kryo kryo = new Kryo();
        Output output = new Output(new ByteArrayOutputStream());
        kryo.writeClassAndObject(output, value);
        put.add(family, ByteBuffer.wrap(qualifier), System.currentTimeMillis(), ByteBuffer.wrap(output.getBuffer(), 0, output.position()));
        try {
            htable.put(put);
        } catch (InterruptedIOException | RetriesExhaustedWithDetailsException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putAll(Collection<Entry> entries) {
        List<Put> list = new ArrayList<>();
        Kryo kryo = new Kryo();
        for (Entry entry : entries) {
            Put put = new Put(effectiveKey(entry.key), System.currentTimeMillis());
            Output output = new Output(new ByteArrayOutputStream());
            kryo.writeClassAndObject(output, entry.value);
            put.add(family, ByteBuffer.wrap(qualifier), System.currentTimeMillis(), ByteBuffer.wrap(output.getBuffer(), 0, output.position()));
        }

        try {
            htable.put(list);
        } catch (InterruptedIOException | RetriesExhaustedWithDetailsException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove(Key key) {
        WindowsStore.Entry.nonNullCheckForKey(key);

        Delete delete = new Delete(effectiveKey(key));
        try {
            htable.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeAll(Collection<Key> keys) {
        List<Delete> deleteBatch = new ArrayList<>();
        for (Key key : keys) {
            WindowsStore.Entry.nonNullCheckForKey(key);

            Delete delete = new Delete(effectiveKey(key));
            deleteBatch.add(delete);
        }
        try {
            htable.delete(deleteBatch);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        try {
            htable.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}