package com.bc.calvalus.inventory;

import java.io.IOException;
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
     * @return The array product sets, which may be empty.
     * @throws java.io.IOException If an I/O error occurs
     */
    ProductSet[] getProductSets(String filter) throws Exception;

    /**
     *
     * @param pathPatterns A list of relative or absolute data paths which may contain regular expressions  (e.g. the file '*' wildcards is written as '.*').
     * @return An array of fully qualified URIs comprising the filesystem and absolute data input path.
     * @throws java.io.IOException If an I/O error occurs.
     */
    String[] globPaths(List<String> pathPatterns) throws IOException;

    /**
     * @param path A relative or absolute path.
     * @return A fully qualified URI comprising the filesystem and absolute data output path.
     */
    String getPath(String path);
}
