package com.bc.calvalus.processing.l3.cellstream;

import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.io.LongWritable;
import ucar.nc2.NetcdfFile;

import java.io.IOException;
import java.util.Date;

/**
 * An abstract reader for reading binned data from NetCDf / HDF files..
 */
public abstract class AbstractNetcdfCellReader {

    private final NetcdfFile netcdfFile;

    public AbstractNetcdfCellReader(NetcdfFile netcdfFile) {
        this.netcdfFile = netcdfFile;
    }

    public void close() throws IOException {
        netcdfFile.close();
    }

    public abstract String[] getFeatureNames();

    public abstract int getCurrentIndex();

    public abstract int getNumBins();

    public abstract Date getStartDate();

    public abstract Date getEndDate();

    public abstract boolean readNext(LongWritable key, L3TemporalBin value) throws Exception;
}
