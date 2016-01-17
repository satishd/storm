/**
 *
 */
package org.apache.storm.trident.windowing;

import java.io.Serializable;

/**
 * Factory to create instances of {@code WindowsStore}.
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
