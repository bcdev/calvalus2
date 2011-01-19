package com.bc.calvalus.binning;

import java.util.ArrayList;

class MyBinManager extends BinManagerImpl {
    ArrayList<SpatialBin> producedSpatialBins = new ArrayList<SpatialBin>();

    public MyBinManager(Aggregator... aggregators) {
        super(aggregators);
    }

    @Override
    public SpatialBin createSpatialBin(int binIndex) {
        SpatialBin spatialBin = super.createSpatialBin(binIndex);
        producedSpatialBins.add(spatialBin);
        return spatialBin;
    }

}
