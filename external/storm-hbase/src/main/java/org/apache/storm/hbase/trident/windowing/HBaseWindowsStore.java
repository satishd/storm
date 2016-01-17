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
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.storm.trident.windowing.WindowsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class HBaseWindowsStore implements WindowsStore {
    private static final Logger log = LoggerFactory.getLogger(HBaseWindowsStore.class);

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
        if (key == null) {
            throw new IllegalArgumentException("key can not be null");
        }
        if(baseId == null) {
            return key;
        }

        byte[] effectiveKey = new byte[baseId.length + key.length];
        System.arraycopy(baseId, 0, effectiveKey, 0, baseId.length);
        System.arraycopy(key, 0, effectiveKey, baseId.length, effectiveKey.length);
        return effectiveKey;
    }

    private byte[] effectiveKey(Key key) {
        return (key.primaryKey + "|" + key.secondaryKey).getBytes();
    }

    @Override
    public Object get(Key key) {
        byte[] effectiveKey = effectiveKey(key);
        Get get = new Get(effectiveKey);
        Result result = null;
        try {
            result = htable.get(get);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Kryo kryo = new Kryo();
        Input input = new Input(result.getValue(family, qualifier));
        Object resultObject = kryo.readClassAndObject(input);
        return resultObject;

    }

    @Override
    public Iterable<Map.Entry<String, Map<String, Object>>> getAllEntries() {
        return null;
    }

    @Override
    public void put(Key key, Object value) {
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
    public void remove(Key key) {
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