/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.ta;


import com.bc.calvalus.binning.TemporalBin;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@code TAPoint} represents the L3 processing output of the trend analysis. It comprises a region name,
 * start and stop date of the and final temporal bin that aggregates all bins in the given region.
 *
 * @author Norman
 */
public class TAPoint implements Writable {

    String regionName;
    String startDate;
    String stopDate;
    TemporalBin temporalBin;

    public TAPoint() {
    }

    public TAPoint(String regionName, String startDate, String stopDate, TemporalBin temporalBin) {
        this.regionName = regionName;
        this.startDate = startDate;
        this.stopDate = stopDate;
        this.temporalBin = temporalBin;
    }

    public static TAPoint read(DataInput in) throws IOException {
        TAPoint point = new TAPoint();
        point.readFields(in);
        return point;
    }

    public String getRegionName() {
        return regionName;
    }

    public String getStartDate() {
        return startDate;
    }

    public String getStopDate() {
        return stopDate;
    }

    public TemporalBin getTemporalBin() {
        return temporalBin;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        regionName = in.readUTF();
        startDate = in.readUTF();
        stopDate = in.readUTF();
        temporalBin = new TemporalBin();
        temporalBin.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(regionName);
        out.writeUTF(startDate);
        out.writeUTF(stopDate);
        temporalBin.write(out);
    }

}
