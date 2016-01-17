/**
 *
 */
package org.apache.storm.trident.windowing;

import java.io.Serializable;

/**
 *
 */
public interface WindowsStoreFactory extends Serializable {

    /**
     * Creates a window store
     *
     * @param baseId id of this store which can be used as base key id for the keys.
     * @return
     */
    public WindowsStore create(String baseId);
}
