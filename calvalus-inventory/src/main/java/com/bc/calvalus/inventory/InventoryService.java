package com.bc.calvalus.inventory;

import java.io.IOException;

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
     * @return The array product sets, which may be empty.
     * @throws java.io.IOException If an I/O error occurs
     */
    ProductSet[] getProductSets(String filter) throws Exception;

    /**
     * @param inputGlob A relative or absolute data input path which may contain wildcards.
     * @return An array of fully qualified URIs comprising the filesystem and absolute data input path.
     */
    String[] getDataInputPaths(String inputGlob) throws IOException;

    /**
     * @param outputPath A relative or absolute data output path.
     * @return A fully qualified URI comprising the filesystem and absolute data output path.
     */
    String getDataOutputPath(String outputPath);
}
