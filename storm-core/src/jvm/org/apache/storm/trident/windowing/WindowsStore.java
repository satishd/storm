/**
 *
 */
package org.apache.storm.trident.windowing;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Store for storing window related entities like windowed tuples, triggers etc.
 *
 */
public interface WindowsStore extends Serializable {

    /**
     * This can be used as a separator while generating a key from sequence of strings.
     */
    public static final String KEY_SEPARATOR = "|";

    public Object get(String key);

    public Iterable<Object> get(List<String> keys);

    public Iterable<String> getAllKeys();

    public void put(String key, Object value);

    public void putAll(Collection<Entry> entries);

    public void remove(String key);

    public void removeAll(Collection<String> keys);

    public void shutdown();

    /**
     * This class wraps key and value objects which can be passed to {@code putAll} method.
     */
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
