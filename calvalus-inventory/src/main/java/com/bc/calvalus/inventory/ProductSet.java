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
    private final String[] bandNames;
    private final String geoInventory;

    public ProductSet(String productType, String name, String path) {
        this(productType, name, path, null, null, null, null, new String[0], null);
    }

    public ProductSet(String productType, String name, String path, Date minDate, Date maxDate, String regionName, String regionWKT, String[] bandNames) {
        this(productType, name, path, minDate, maxDate, regionName, regionWKT, bandNames, null);
    }

    public ProductSet(String productType, String name, String path, Date minDate, Date maxDate, String regionName, String regionWKT, String[] bandNames, String geoInventory) {
        if (productType == null) {
            throw new NullPointerException("productType");
        }
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (path == null) {
            throw new NullPointerException("path");
        }
        if (bandNames == null) {
            throw new NullPointerException("bandNames");
        }
        this.productType = productType;
        this.name = name;
        this.path = path;
        this.minDate = minDate;
        this.maxDate = maxDate;
        this.regionName = regionName;
        this.regionWKT = regionWKT;
        this.bandNames = bandNames;
        this.geoInventory = geoInventory;
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

    public String[] getBandNames() {
        return bandNames;
    }

    public String getGeoInventory() {
        return geoInventory;
    }
}
