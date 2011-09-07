package com.bc.calvalus.inventory;


import java.util.Date;

/**
 * A product set.
 *
 * @author Norman
 */
public class ProductSet {
    private final String name;
    private final String path;
    private final Date minDate;
    private final Date maxDate;

    public ProductSet(String name, String path, Date minDate, Date maxDate) {
        this.name = name;
        this.path = path;
        this.minDate = minDate;
        this.maxDate = maxDate;
    }

    public String getName() {
        return name;
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
