package com.bc.calvalus.catalogue;


import java.util.Date;

/**
 * A product set.
 *
 * @author Norman
 */
public class ProductSet {
    private final String path;
    private final Date minDate;
    private final Date maxDate;

    public ProductSet(String path, Date minDate, Date maxDate) {
        this.path = path;
        this.minDate = minDate;
        this.maxDate = maxDate;
    }

    public String getPath() {
        return path;
    }

    public Date getMinDate() {
        return minDate;
    }

    public Date getMaxDate() {
        return maxDate;
    }
}
