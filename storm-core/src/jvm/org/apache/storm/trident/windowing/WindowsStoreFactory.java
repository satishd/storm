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
     * @return
     */
    public WindowsStore create();
}
