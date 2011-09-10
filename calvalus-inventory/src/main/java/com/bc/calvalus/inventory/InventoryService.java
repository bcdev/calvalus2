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
     * @return The array product sets, which may be empty.
     * @throws java.io.IOException If an I/O error occurs
     */
    ProductSet[] getProductSets(String filter) throws Exception;

    /**
     * Globs the given path pattern list against the inventory service's file system.
     * <p/>
     * <i>TODO: Use Unix file system wildcards instead (nf, 2011-09-09). See {@link AbstractInventoryService#getRegexpForPathGlob(String)}. </i>
     *
     * @param pathPatterns A list of relative or absolute data paths which may contain regular expressions.
     * @return An array of fully qualified URIs comprising the filesystem and absolute data input path.
     * @throws java.io.IOException If an I/O error occurs.
     */
    String[] globPaths(List<String> pathPatterns) throws IOException;

    /**
     * @param path A relative or absolute path.
     * @return A fully qualified URI comprising the filesystem and absolute data output path.
     */
    String getQualifiedPath(String path);

    OutputStream addFile(String userPath)throws IOException;

    boolean removeFile(String userPath)throws IOException;
}
