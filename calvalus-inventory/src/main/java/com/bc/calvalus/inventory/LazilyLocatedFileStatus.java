package com.bc.calvalus.inventory;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class LazilyLocatedFileStatus extends LocatedFileStatus {
    public LazilyLocatedFileStatus(FileStatus status) {
        super(status, null);
    }
    public void locate(BlockLocation[] locations) {
        setBlockLocations(locations);
    }
}
