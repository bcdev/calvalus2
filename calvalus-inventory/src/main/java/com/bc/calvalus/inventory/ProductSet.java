package com.bc.calvalus.inventory;


import java.util.Date;

/**
 * A product set.
 *
 * @author Norman
 */
public class ProductSet {

    private final String productType;
    private final String name;
    private final String path;
    private final Date minDate;
    private final Date maxDate;
    private final String regionName;
    private final String regionWKT;

    public ProductSet(String productType, String name, String path, Date minDate, Date maxDate, String regionName, String regionWKT) {
        this.productType = productType;
        this.name = name;
        this.path = path;
        this.minDate = minDate;
        this.maxDate = maxDate;
        this.regionName = regionName;
        this.regionWKT = regionWKT;
    }

    public String getProductType() {
        return productType;
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

    public String getRegionName() {
        return regionName;
    }

    public String getRegionWKT() {
        return regionWKT;
    }
}
