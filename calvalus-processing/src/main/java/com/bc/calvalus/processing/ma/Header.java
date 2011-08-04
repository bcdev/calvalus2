package com.bc.calvalus.processing.ma;

/**
 * A header is used to describe the records of a record source.
 *
 * @author MarcoZ
 * @author Norman
 */
public interface Header {
    /**
     * @return The array of attribute names.
     */
    String[] getAttributeNames();
}
