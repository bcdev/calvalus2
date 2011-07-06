package com.bc.calvalus.catalogue;


import java.util.Date;

/**
 * A product set.
 *
 * @author Norman
 */
public class ProductSet {
    private String path;
    private String type;
    private String name;
    private Date minDate;
    private Date maxDate;

    public ProductSet(String path, String type, String name) {
        this.path = path;
        this.type = type;
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public Date getMinDate() {
        return minDate;
    }

    public void setMinDate(Date minDate) {
        this.minDate = minDate;
    }

    public Date getMaxDate() {
        return maxDate;
    }

    public void setMaxDate(Date maxDate) {
        this.maxDate = maxDate;
    }
}
