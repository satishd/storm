/**
 *
 */
package org.apache.storm.trident.windowing;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public interface WindowsStore extends Serializable {

    public static final String KEY_SEPARATOR = "|";

    public Object get(String key);

    public Iterable<Object> get(List<String> keys);

    public Iterable<WindowsStore.Entry> getAllKeys();

    public void put(String key, Object value);

    public void putAll(Collection<Entry> entries);

    public void remove(String key);

    public void removeAll(Collection<String> keys);

    public void shutdown();

    public static class Entry implements Serializable {
        public final String key;
        public final Object value;

        public Entry(String key, Object value) {
            nonNullCheckForKey(key);
            nonNullCheckForValue(value);
            this.key = key;
            this.value = value;
        }

        public static void nonNullCheckForKey(Object key) {
            Preconditions.checkArgument(key != null, "key argument can not be null");
        }

        public static void nonNullCheckForValue(Object value) {
            Preconditions.checkArgument(value != null, "value argument can not be null");
        }

    }

}
