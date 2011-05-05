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

package com.bc.calvalus.processing;

import com.bc.ceres.core.Assert;
import org.apache.hadoop.util.hash.Hash;
import org.esa.beam.util.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;


public class LcRequest {
    private static final Map<String, String> GEOMETRIES = new HashMap<String, String>();
    static {
        GEOMETRIES.put("Africa", "polygon((15 17, 15 -10, 22 -10, 22 17, 15 17))");
        GEOMETRIES.put("WesternEurope", "polygon((-7 54, -7 38.5, 5.5 38.5, 5.5 54, -7 54))");
        GEOMETRIES.put("NorthAmerica", "polygon((-117 55, -117 35, -107 35, -107 55, -117 55))");
        GEOMETRIES.put("NorthwestAsia", "polygon((28.6 41.4, 28.6 39.3, 27.3 39.3, 27.3 41.4, 28.6 41.4))");
        GEOMETRIES.put("Australia", "polygon((138.3 -14.6, 138.3 -36.2, 148.2 -36.2, 148.2 -14.6, 138.3 -14.6))");
        GEOMETRIES.put("CentralAsia", "polygon((86.6 43.2, 86.6 33.3, 99.7 33.3, 99.7 43.2, 86.6 43.2))");
    }
    private static final String[] YEARS = {"2005", "2006", "2007", "2008", "2009"};
    private static final String[] MONTH = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};

    public static void main(String[] args) throws IOException {
        format("lc-l2-template.xml", "Africa", "2009");
        format("lc-l2-template.xml", "WesternEurope", "2009");
        format("lc-l2-template.xml", "NorthAmerica", "2009");
    }

    private static void format(String templateName, String region, String ... years) throws IOException {
        Assert.notNull(templateName, "templateName");
        Assert.notNull(region, "region");
        Assert.argument(GEOMETRIES.containsKey(region), "valid region");
        Assert.argument(years.length > 0, "years.length > 0");

        String geometry = GEOMETRIES.get(region);
        String template = readTemplate(templateName);
        for (String year : years) {
            String request = template.
                    replaceAll("\\$YEAR", year).
                    replaceAll("\\$REGION", region).
                    replaceAll("\\$GEOMETRY", geometry);
            String filename = templateName.replace("template", String.format("%s-%s", region, year));
            writeRequest(request, new File(filename));
        }
    }

    private static void writeRequest(String request, File file) throws IOException {
        System.out.println("writing request to: " + file.getAbsolutePath());
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(file);
            fileWriter.write(request);
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }

    private static String readTemplate(String templateName) throws IOException {
        InputStream inputStream = LcRequest.class.getResourceAsStream(templateName);
        Reader reader = new InputStreamReader(inputStream);
        return FileUtils.readText(reader);
    }
}
