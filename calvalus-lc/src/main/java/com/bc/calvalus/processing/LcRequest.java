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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.StringUtils;
import org.esa.beam.util.io.FileUtils;
import sun.swing.StringUIClientPropertyKey;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LcRequest {

    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final DateFormat DATE_FORMAT = ProductData.UTC.createDateFormat(DATE_PATTERN);
    static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;

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

    static int index = 0;

    public static void main(String[] args) throws Exception {
//        formatL2Template("Africa", "2009");
//        formatL2Template("WesternEurope", "2009");
//        formatL2Template("NorthAmerica", "2009");

        formatL3Template("Africa", "2009-01-01", "2009-12-31", 10);
        formatL3Template("WesternEurope", "2009-01-01", "2009-12-31", 10);
        formatL3Template("NorthAmerica", "2009-01-01", "2009-12-31", 10);

        formatL3Template("Africa", "2009-01-01", "2009-12-31", 15);
        formatL3Template("Africa", "2009-01-01", "2009-12-31", 30);

        formatL3Template("WesternEurope", "2009-01-01", "2009-12-31", 15);
        formatL3Template("NorthAmerica", "2009-01-01", "2009-12-31", 15);

        formatL3Template("WesternEurope", "2009-01-01", "2009-12-31", 30);
        formatL3Template("NorthAmerica", "2009-01-01", "2009-12-31", 30);

    }

    private static void formatL2Template(String region, String... years) throws IOException {
        formatL2("lc-l2-template.xml", region, years);
    }

    private static void formatL2(String templateName, String region, String... years) throws IOException {
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

    private static void formatL3Template(String region, String startDate, String endDate, int periodLength) throws Exception {
        formatL3("lc-l3-template.xml", region, startDate, endDate, periodLength);
    }

    private static void formatL3(String templateName, String region, String startDate, String endDate, int periodLength) throws Exception {
        String template = readTemplate(templateName);
        List<InputFiles> inputFilesList = getInputs(region, startDate, endDate, periodLength);
        String year = startDate.substring(0, 4);
        String geometry = GEOMETRIES.get(region);

        for (InputFiles inputfile : inputFilesList) {
            String request = template.
                    replaceAll("\\$YEAR", year).
                    replaceAll("\\$REGION", region).
                    replaceAll("\\$GEOMETRY", geometry).
                    replaceAll("\\$PERIOD", inputfile.perioID).
                    replaceAll("\\$INPUTS", formatInputFiles(inputfile.inputFiles));
            String filename = templateName.replace("template", String.format("%s-%s", region, inputfile.perioID));
            filename = String.format("%04d-%s", index++, filename);
            writeRequest(request, new File(filename));
        }
    }

    private static String formatInputFiles(String[] files) {
        StringBuilder sb = new StringBuilder();
        for (String file : files) {
            sb.append(formatInputFile(file));
        }
        return sb.toString();
    }

    private static String formatInputFile(String file) {
        StringBuilder sb = new StringBuilder();
        sb.append("<wps:Input>\n");
        sb.append("  <ows:Identifier>calvalus.input</ows:Identifier>\n");
        sb.append("  <wps:Reference xlink:href=\"");
        sb.append(file);
        sb.append("\" />\n");
        sb.append("</wps:Input>\n");
        return sb.toString();
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

    private static List<InputFiles> getInputs(String region, String startDate, String endDate, int periodLength) throws Exception {
        Date minDate = DATE_FORMAT.parse(startDate);
        Date maxDate = DATE_FORMAT.parse(endDate);

        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://cvmaster00:9000");
        FileSystem fs = FileSystem.get(configuration);

        String sdrRoot = "hdfs://cvmaster00:9000/calvalus/outputs/lc-sdr";
        String year = startDate.substring(0, 4);
        String inputDir = String.format("%s/%s/%s", sdrRoot, region, year);

        System.out.println("inputDir = " + inputDir);

        long time = minDate.getTime();
        long periodLengthMillis = periodLength * MILLIS_PER_DAY;
        List<InputFiles> result = new ArrayList<InputFiles>();
        while (true) {

            Date date1 = new Date(time);
            Date date2 = new Date(time + periodLengthMillis - 1L);

            if (date2.after(maxDate)) {
                break;
            }

            String date1Str = DATE_FORMAT.format(date1);
            String date2Str = DATE_FORMAT.format(date2);

            String periodID = String.format("%dd-%s", periodLength, date1Str);

            System.out.println("periodID = " + periodID);
            String[] inputFiles = getInputFiles(fs, inputDir, date1, date2);
            result.add(new InputFiles(periodID, inputFiles));
            time += periodLengthMillis;
        }
        return result;
    }

    private static class InputFiles {
        String perioID;
        String[] inputFiles;

        private InputFiles(String perioID, String[] inputFiles) {
            this.perioID = perioID;
            this.inputFiles = inputFiles;
        }
    }

    private static String[] getInputFiles(FileSystem fs, String inputDir, Date minDate, Date maxDate) throws Exception {
        final List<String> dayList = getDayList(minDate, maxDate);
        PathFilter pathFilter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                String pathName = path.getName();
                for (String day : dayList) {
                    String regex = "L2_of_MER_FSG_1[A-Z]{5}" + day + ".+";
                    if (pathName.matches(regex))
                        return true;
                }
                return false;
            }
        };
        try {
            return listFilePaths(fs, inputDir, pathFilter);
        } catch (IOException e) {
            throw new Exception("Failed to compute input file list.", e);
        }
    }

    public static String[] listFilePaths(FileSystem fileSystem, String dirPath, PathFilter pathfilter) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(dirPath), pathfilter);
        String[] paths = new String[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++) {
            paths[i] = fileStatuses[i].getPath().toString();
        }
        return paths;
    }


    private static List<String> getDayList(Date start, Date stop) {
        Calendar startCal = ProductData.UTC.createCalendar();
        Calendar stopCal = ProductData.UTC.createCalendar();
        startCal.setTime(start);
        stopCal.setTime(stop);
        List<String> list = new ArrayList<String>();
        do {
            list.add(String.format("%1$tY%1$tm%1$td", startCal));
            startCal.add(Calendar.DAY_OF_WEEK, 1);
        } while (!startCal.after(stopCal));

        return list;
    }

}
