package com.bc.calvados.hadoop.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class N1InputSplit extends FileSplit {

    private final int headerSize;
    private final int granuleSize;

    public N1InputSplit(Path file, long start, long length, String[] hosts, int headerSize, int granuleSize) {
        super(file, start, length, hosts);
        this.headerSize = headerSize;
        this.granuleSize = granuleSize;
    }

    public int getHeaderSize() {
        return headerSize;
    }

    public int getGranuleSize() {
        return granuleSize;
    }
}
