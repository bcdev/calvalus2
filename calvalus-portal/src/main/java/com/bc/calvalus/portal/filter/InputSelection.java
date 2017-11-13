package com.bc.calvalus.portal.filter;

import java.util.List;

/**
 * @author hans
 */
public class InputSelection {

    private String collectionName;
    private List<String> productIdentifiers;
    private TimeSelection dateRange;
    private String regionGeometry;

    public InputSelection(String collectionName, List<String> productIdentifiers, TimeSelection timeSelection, String spatialSelection) {
        this.collectionName = collectionName;
        this.productIdentifiers = productIdentifiers;
        this.dateRange = timeSelection;
        this.regionGeometry = spatialSelection;
    }

    String getCollectionName() {
        return collectionName;
    }

    List<String> getProductIdentifiers() {
        return productIdentifiers;
    }

    TimeSelection getDateRange() {
        return dateRange;
    }

    String getRegionGeometry() {
        return regionGeometry;
    }
}

class TimeSelection {
    private String startTime;
    private String endTime;

    public TimeSelection(String startTime, String endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    String getStartTime() {
        return startTime;
    }

    String getEndTime() {
        return endTime;
    }
}