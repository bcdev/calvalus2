package com.bc.calvalus.inventory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * The interface to the Calvalus inventory.
 *
 * @author MarcoZ
 * @author Norman
 */
public interface InventoryService {

    /**
     * Gets pre-defined product sets.
     *
     * @param filter A filter expression (unused)
     *
     * @return The array product sets, which may be empty.
     *
     * @throws java.io.IOException If an I/O error occurs
     */
    ProductSet[] getProductSets(String username, String filter) throws IOException;

}
