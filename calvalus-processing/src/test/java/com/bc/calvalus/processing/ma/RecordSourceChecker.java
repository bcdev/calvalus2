/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ma;

import org.esa.snap.framework.datamodel.GeoPos;
import org.junit.Ignore;

@Ignore
public class RecordSourceChecker {

    public static void main(String[] args) throws Exception {
        String url = args[0];
        System.out.println("url = " + url);

        RecordSourceSpi recordSourceSpi = RecordSourceSpi.getForUrl(url);
        RecordSource recordSource = recordSourceSpi.createRecordSource(url);
        Iterable<Record> records = recordSource.getRecords();

        int numRecords = 0;
        for (Record record : records) {
            GeoPos location = record.getLocation();
            if (location != null && location.isValid()) {
                numRecords++;
            }
        }
        System.out.format("%s. \nNumber of records with valid geo location: %d\n",
                          recordSource.getTimeAndLocationColumnDescription(),
                          numRecords);

    }
}
