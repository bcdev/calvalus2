package com.bc.calvalus.inventory;

/**
 * The interface to the Calvalus inventory.
 *
 * @author MarcoZ
 * @author Norman
 */
public interface InventoryService {

    /**
     * Gets all product sets.
     *
     * @param filter A filter expression (unused)
     * @return The array product sets, which may be empty.
     * @throws java.io.IOException If an I/O error occurs
     */
    ProductSet[] getProductSets(String filter) throws Exception;
}
