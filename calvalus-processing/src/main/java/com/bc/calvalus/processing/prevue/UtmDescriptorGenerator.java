/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.prevue;

import com.bc.calvalus.processing.ma.CsvRecordSource;
import com.bc.calvalus.processing.ma.CsvRecordWriter;
import com.bc.calvalus.processing.ma.Record;
import org.esa.beam.framework.datamodel.GeoPos;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.datum.DefaultGeodeticDatum;
import org.geotools.referencing.operation.projection.TransverseMercator;
import org.opengis.geometry.DirectPosition;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.GeodeticDatum;
import org.opengis.referencing.operation.MathTransform;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DecimalFormat;

/**
 * * Generates an UTM product for every prevue site
 * with two band 'easting' and 'northing' containing the 'meters' for each point.
 */
public class UtmDescriptorGenerator {

    public static void main(String[] args) throws Exception {
        File prevueCSVFile = new File(args[0]);
        if (!prevueCSVFile.exists()) {
            throw new IllegalArgumentException("prevue-CSV file does not exist:" + prevueCSVFile.getAbsolutePath());
        }

        File outDir = new File(args[1]);
        if (outDir.exists() && !outDir.isDirectory()) {
            throw new IllegalArgumentException("'outdir' must be a directory");
        }
        outDir.mkdirs();

        FileReader reader = new FileReader(prevueCSVFile);
        try {
            CsvRecordSource recordSource = new CsvRecordSource(reader, CsvRecordWriter.DEFAULT_DATE_FORMAT);
            DecimalFormat decimalFormat = new DecimalFormat("000");

            for (Record record : recordSource.getRecords()) {
                GeoPos location = record.getLocation();
                Double id = (Double) record.getAttributeValues()[0];
                String idAsString = decimalFormat.format(id);
                System.out.println("idAsString = " + idAsString);

                GeodeticDatum datum = DefaultGeodeticDatum.WGS84;
                int zoneIndex = PrevueMapper.getZoneIndex(location.getLon());
                final boolean south = location.getLat() < 0.0;
                ParameterValueGroup tmParameters = PrevueMapper.createTransverseMercatorParameters(zoneIndex, south, datum);
                final String projName = PrevueMapper.getProjectionName(zoneIndex, south);

                CoordinateReferenceSystem crsUtmAutomatic = PrevueMapper.createCrs(projName, new TransverseMercator.Provider(), tmParameters, datum);

                DefaultGeographicCRS wgs84 = DefaultGeographicCRS.WGS84;
                MathTransform transform = CRS.findMathTransform(wgs84, crsUtmAutomatic);
                DirectPosition centerWgs84 = new DirectPosition2D(wgs84, location.getLon(), location.getLat());
                DirectPosition centerUTM = transform.transform(centerWgs84, null);

                File utmFile = new File(outDir, idAsString + "_" + "UTM" + zoneIndex + (south ? "S" : "N"));
                if (utmFile.exists()) {
                    utmFile.delete();
                }
                FileWriter fileWriter = new FileWriter(utmFile);
                try {
                    fileWriter.write(projName+ "\n\n");
                    fileWriter.write("easting\n");
                    double eastingCenter = centerUTM.getOrdinate(0);
                    for (int y = 0; y < 49; y++) {
                        StringBuilder line = new StringBuilder();
                        for (int x = 0; x < 49; x++) {
                            double easting = eastingCenter + ((x - 25) * 260);
                            line.append(easting);
                            if (x != 48) {
                                line.append(" ");
                            }
                        }
                        line.append("\n");
                        fileWriter.write(line.toString());
                    }
                    fileWriter.write("\n");

                    fileWriter.write("northing\n");
                    double northingCenter = centerUTM.getOrdinate(1);
                    for (int y = 0; y < 49; y++) {
                        StringBuilder line = new StringBuilder();
                        double northing = northingCenter + ((y - 25) * 260);
                        for (int x = 0; x < 49; x++) {
                            line.append(northing);
                            if (x != 48) {
                                line.append(" ");
                            }
                        }
                        line.append("\n");
                        fileWriter.write(line.toString());
                    }
                    fileWriter.write("\n");
                } finally {
                    fileWriter.close();
                }
            }
        } finally {
            reader.close();
        }

    }
}
