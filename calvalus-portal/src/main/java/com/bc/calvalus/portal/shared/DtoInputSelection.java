package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

import java.util.List;

/**
 * @author hans
 */
public class DtoInputSelection implements IsSerializable {

    private String collectionName;
    private List<String> productIdentifiers;
    private DtoDateRange dateRange;
    private String regionGeometry;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoInputSelection() {
    }

    public DtoInputSelection(String collectionName, List<String> productIdentifiers, DtoDateRange dateRange, String regionGeometry) {
        this.collectionName = collectionName;
        this.productIdentifiers = productIdentifiers;
        this.dateRange = dateRange;
        this.regionGeometry = regionGeometry;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public List<String> getProductIdentifiers() {
        return productIdentifiers;
    }

    public DtoDateRange getDateRange() {
        return dateRange;
    }

    public String getRegionGeometry() {
        return regionGeometry;
    }

    @Override
    public String toString() {
        return "DtoInputSelection{" +
               "collectionName='" + collectionName + '\'' +
               ", productIdentifiers=" + productIdentifiers +
               ", dateRange=" + dateRange +
               ", regionGeometry='" + regionGeometry + '\'' +
               '}';
    }
}