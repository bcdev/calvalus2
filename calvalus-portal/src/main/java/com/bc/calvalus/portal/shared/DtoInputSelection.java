package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

import java.util.List;

/**
 * @author hans
 */
public class DtoInputSelection implements IsSerializable {

    private String collectionName;
    private List<String> productIdentifiers;
    private DtoTimeSelection dateRange;
    private String regionGeometry;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoInputSelection() {
    }

    public DtoInputSelection(String collectionName, List<String> productIdentifiers, DtoTimeSelection timeSelection, String spatialSelection) {
        this.collectionName = collectionName;
        this.productIdentifiers = productIdentifiers;
        this.dateRange = timeSelection;
        this.regionGeometry = spatialSelection;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public List<String> getProductIdentifiers() {
        return productIdentifiers;
    }

    public DtoTimeSelection getDateRange() {
        return dateRange;
    }

    public String getRegionGeometry() {
        return regionGeometry;
    }
}