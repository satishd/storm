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

    public Object get(Key key);

    public Iterable<Object> get(List<Key> keys);

    public Iterable<WindowsStore.Entry> getAllKeys();

    public void put(Key key, Object value);

    public void putAll(Collection<Entry> entries);

    public void remove(Key key);

    public void removeAll(Collection<Key> keys);

    public void shutdown();

    public static class Entry implements Serializable {
        public final Key key;
        public final Object value;

        public Entry(Key key, Object value) {
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

    public static class Key implements Serializable {
        public final String primaryKey;
        public final String secondaryKey;

        public Key(String primaryKey, String secondaryKey) {
            Preconditions.checkArgument(primaryKey!= null, "primaryKey argument can not be null");
            Preconditions.checkArgument(secondaryKey!= null, "secondaryKey argument can not be null");

            this.primaryKey = primaryKey;
            this.secondaryKey = secondaryKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key)) return false;

            Key key = (Key) o;

            if (primaryKey != null ? !primaryKey.equals(key.primaryKey) : key.primaryKey != null) return false;
            return !(secondaryKey != null ? !secondaryKey.equals(key.secondaryKey) : key.secondaryKey != null);

        }

        @Override
        public int hashCode() {
            int result = primaryKey != null ? primaryKey.hashCode() : 0;
            result = 31 * result + (secondaryKey != null ? secondaryKey.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Key{" +
                    "primaryKey=" + primaryKey +
                    ", secondaryKey=" + secondaryKey +
                    '}';
        }
    }

}
