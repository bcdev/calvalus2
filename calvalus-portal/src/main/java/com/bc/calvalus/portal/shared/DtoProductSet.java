package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

import java.util.Date;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.inventory.ProductSet} class.
 *
 * @author Norman
 */
public class DtoProductSet implements IsSerializable {

    private String geoInventory;
    private String productType;
    private String name;
    private String path;
    private Date minDate;
    private Date maxDate;
    private String regionName;
    private String regionWKT;
    private String[] bandNames;


    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoProductSet() {
    }

    public DtoProductSet(String productType, String name, String path, Date minDate, Date maxDate, String regionName,
                         String regionWKT, String[] bandNames, String geoInventory) {
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (path == null) {
            throw new NullPointerException("path");
        }
        if (regionName == null) {
            throw new NullPointerException("regionName");
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DtoProductSet that = (DtoProductSet) o;

        if (maxDate != null ? !maxDate.equals(that.maxDate) : that.maxDate != null) {
            return false;
        }
        if (minDate != null ? !minDate.equals(that.minDate) : that.minDate != null) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (!path.equals(that.path)) {
            return false;
        }
        if (productType != null ? !productType.equals(that.productType) : that.productType != null) {
            return false;
        }
        if (!regionName.equals(that.regionName)) {
            return false;
        }
        return regionWKT != null ? regionWKT.equals(that.regionWKT) : that.regionWKT == null;
    }

    @Override
    public int hashCode() {
        int result = (productType != null ? productType.hashCode() : 0);
        result = 31 * result + name.hashCode();
        result = 31 * result + path.hashCode();
        result = 31 * result + (minDate != null ? minDate.hashCode() : 0);
        result = 31 * result + (maxDate != null ? maxDate.hashCode() : 0);
        result = 31 * result + (regionName.hashCode());
        result = 31 * result + (regionWKT != null ? regionWKT.hashCode() : 0);
        result = 31 * result + (regionWKT != null ? regionWKT.hashCode() : 0);
        return result;
    }
}
