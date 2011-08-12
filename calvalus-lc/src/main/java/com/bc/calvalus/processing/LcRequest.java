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
import org.esa.beam.util.io.FileUtils;

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
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


public class LcRequest {

    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final DateFormat DATE_FORMAT = ProductData.UTC.createDateFormat(DATE_PATTERN);
    static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;

    private static final Map<String, String> LC_GEOMETRIES = new HashMap<String, String>();
    private static final Map<String, String> CC_GEOMETRIES;

    static {
        LC_GEOMETRIES.put("Africa", "polygon((15 17, 15 -10, 22 -10, 22 17, 15 17))");
        LC_GEOMETRIES.put("WesternEurope", "polygon((-7 54, -7 38.5, 5.5 38.5, 5.5 54, -7 54))");
        LC_GEOMETRIES.put("NorthAmerica", "polygon((-117 55, -117 35, -107 35, -107 55, -117 55))");
        LC_GEOMETRIES.put("NorthwestAsia", "polygon((28.6 41.4, 28.6 39.3, 27.3 39.3, 27.3 41.4, 28.6 41.4))");
        LC_GEOMETRIES.put("Australia", "polygon((138.3 -14.6, 138.3 -36.2, 148.2 -36.2, 148.2 -14.6, 138.3 -14.6))");
        LC_GEOMETRIES.put("CentralAsia", "polygon((86.6 43.2, 86.6 33.3, 99.7 33.3, 99.7 43.2, 86.6 43.2))");
        CC_GEOMETRIES = createRegionMap(loadRegions("cc-regions.properties"));
    }

    private static final String[] YEARS = {"2005", "2006", "2007", "2008", "2009"};
    private static final String[] MONTH = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};

    static int index = 100;

    public static void main(String[] args) throws Exception {

//        formatLCL2Template("Africa", "2005");
//        formatLCL2Template("WesternEurope", "2005");
//        formatLCL2Template("NorthAmerica", "2005");
//        formatLCL2Template("NorthwestAsia", "2005");
//        formatLCL2Template("Australia", "2005");
//        formatLCL2Template("CentralAsia", "2005");
//
//        formatLCL2Template("Africa", "2006");
//        formatLCL2Template("WesternEurope", "2006");
//        formatLCL2Template("NorthAmerica", "2006");
//        formatLCL2Template("NorthwestAsia", "2006");
//        formatLCL2Template("Australia", "2006");
//        formatLCL2Template("CentralAsia", "2006");
//
//        formatLCL2Template("Africa", "2007");
//        formatLCL2Template("WesternEurope", "2007");
//        formatLCL2Template("NorthAmerica", "2007");
//        formatLCL2Template("NorthwestAsia", "2007");
//        formatLCL2Template("Australia", "2007");
//        formatLCL2Template("CentralAsia", "2007");
//
//        formatLCL2Template("Africa", "2008");
//        formatLCL2Template("WesternEurope", "2008");
//        formatLCL2Template("NorthAmerica", "2008");
//        formatLCL2Template("NorthwestAsia", "2008");
//        formatLCL2Template("Australia", "2008");
//        formatLCL2Template("CentralAsia", "2008");
//
//        formatLCL2Template("NorthwestAsia", "2009");
//        formatLCL2Template("Australia", "2009");
//        formatLCL2Template("CentralAsia", "2009");

        formatL3ProcessingTemplate("Africa", "2005-01-01", "2005-12-31", 10);
        formatL3ProcessingTemplate("WesternEurope", "2005-01-01", "2005-12-31", 10);
        formatL3ProcessingTemplate("NorthAmerica", "2005-01-01", "2005-12-31", 10);
//        formatL3ProcessingTemplate("NorthwestAsia", "2005-01-01", "2005-12-31", 10);
//        formatL3ProcessingTemplate("Australia", "2005-01-01", "2005-12-31", 10);
//        formatL3ProcessingTemplate("CentralAsia", "2005-01-01", "2005-12-31", 10);
//
        formatL3ProcessingTemplate("Africa", "2006-01-01", "2006-12-31", 10);
        formatL3ProcessingTemplate("WesternEurope", "2006-01-01", "2006-12-31", 10);
        formatL3ProcessingTemplate("NorthAmerica", "2006-01-01", "2006-12-31", 10);
//        formatL3ProcessingTemplate("NorthwestAsia", "2006-01-01", "2006-12-31", 10);
//        formatL3ProcessingTemplate("Australia", "2006-01-01", "2006-12-31", 10);
//        formatL3ProcessingTemplate("CentralAsia", "2006-01-01", "2006-12-31", 10);
//
        formatL3ProcessingTemplate("Africa", "2007-01-01", "2007-12-31", 10);
        formatL3ProcessingTemplate("WesternEurope", "2007-01-01", "2007-12-31", 10);
        formatL3ProcessingTemplate("NorthAmerica", "2007-01-01", "2007-12-31", 10);
//        formatL3ProcessingTemplate("NorthwestAsia", "2007-01-01", "2007-12-31", 10);
//        formatL3ProcessingTemplate("Australia", "2007-01-01", "2007-12-31", 10);
//        formatL3ProcessingTemplate("CentralAsia", "2007-01-01", "2007-12-31", 10);
//
        formatL3ProcessingTemplate("Africa", "2008-01-01", "2008-12-31", 10);
        formatL3ProcessingTemplate("WesternEurope", "2008-01-01", "2008-12-31", 10);
        formatL3ProcessingTemplate("NorthAmerica", "2008-01-01", "2008-12-31", 10);
//        formatL3ProcessingTemplate("NorthwestAsia", "2008-01-01", "2008-12-31", 10);
//        formatL3ProcessingTemplate("Australia", "2008-01-01", "2008-12-31", 10);
//        formatL3ProcessingTemplate("CentralAsia", "2008-01-01", "2008-12-31", 10);
//
        formatL3ProcessingTemplate("Africa", "2009-01-01", "2009-12-31", 10);
        formatL3ProcessingTemplate("WesternEurope", "2009-01-01", "2009-12-31", 10);
        formatL3ProcessingTemplate("NorthAmerica", "2009-01-01", "2009-12-31", 10);
//        formatL3ProcessingTemplate("NorthwestAsia", "2009-01-01", "2009-12-31", 10);
//        formatL3ProcessingTemplate("Australia", "2009-01-01", "2009-12-31", 10);
//        formatL3ProcessingTemplate("CentralAsia", "2009-01-01", "2009-12-31", 10);

//        formatL3ProcessingTemplate("Africa", "2009-01-01", "2009-12-31", 15);
//        formatL3ProcessingTemplate("Africa", "2009-01-01", "2009-12-31", 30);
//
//        formatL3ProcessingTemplate("WesternEurope", "2009-01-01", "2009-12-31", 15);
//        formatL3ProcessingTemplate("NorthAmerica", "2009-01-01", "2009-12-31", 15);
//
//        formatL3ProcessingTemplate("WesternEurope", "2009-01-01", "2009-12-31", 30);
//        formatL3ProcessingTemplate("NorthAmerica", "2009-01-01", "2009-12-31", 30);


//        formatL3FormattingTemplate("Africa", "2009-01-01", "2009-12-31", 10);
//        formatL3FormattingTemplate("WesternEurope", "2005-01-01", "2005-12-31", 10);
//        formatL3FormattingTemplate("NorthAmerica", "2009-01-01", "2009-12-31", 10);
//
//        formatL3FormattingTemplate("Africa", "2009-01-01", "2009-12-31", 15);
//        formatL3FormattingTemplate("Africa", "2009-01-01", "2009-12-31", 30);
//
//        formatL3FormattingTemplate("WesternEurope", "2009-01-01", "2009-12-31", 15);
//        formatL3FormattingTemplate("NorthAmerica", "2009-01-01", "2009-12-31", 15);
//
//        formatL3FormattingTemplate("WesternEurope", "2009-01-01", "2009-12-31", 30);
//        formatL3FormattingTemplate("NorthAmerica", "2009-01-01", "2009-12-31", 30);
//

//        formatCCProcessingTemplate("cc-l1p-template.xml", "acadia", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "amazondelta", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "antaresubatuba", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "balticsea", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "beibubay", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "benguela", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "capeverde", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "centralcalifornia", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "chesapeakebay", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "chinakoreajapan", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "greatbarrierreef", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "gulfofmexico", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "indonesianwaters", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "karasea", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "lakeseriestclair", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "lenadelta", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "mediterranean_blacksea", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "morocco", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "namibianwaters", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "northsea", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "oregon_washington", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "puertorico", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "redsea", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "riolaplata", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "southerncalifornia", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "southindia", "2008");
//        formatCCProcessingTemplate("cc-l1p-template.xml", "tasmania", "2008");

//        formatCCFormattingTemplate("l1p", "acadia", "2007");
//        formatCCFormattingTemplate("l1p", "amazondelta", "2007");
//        formatCCFormattingTemplate("l1p", "antaresubatuba", "2007");
//        formatCCFormattingTemplate("l1p", "balticsea", "2007");
//        formatCCFormattingTemplate("l1p", "beibubay", "2007");
//        formatCCFormattingTemplate("l1p", "benguela", "2007");
//        formatCCFormattingTemplate("l1p", "capeverde", "2007");
//        formatCCFormattingTemplate("l1p", "centralcalifornia", "2007");
//        formatCCFormattingTemplate("l1p", "chesapeakebay", "2007");
//        formatCCFormattingTemplate("l1p", "chinakoreajapan", "2007");
//        formatCCFormattingTemplate("l1p", "greatbarrierreef", "2007");
//        formatCCFormattingTemplate("l1p", "gulfofmexico", "2007");
//        formatCCFormattingTemplate("l1p", "indonesianwaters", "2007");
//        formatCCFormattingTemplate("l1p", "karasea", "2007");
//        formatCCFormattingTemplate("l1p", "lakeseriestclair", "2007");
//        formatCCFormattingTemplate("l1p", "lenadelta", "2007");
//        formatCCFormattingTemplate("l1p", "mediterranean_blacksea", "2007");
//        formatCCFormattingTemplate("l1p", "morocco", "2007");
//        formatCCFormattingTemplate("l1p", "namibianwaters", "2007");
//        formatCCFormattingTemplate("l1p", "northsea", "2007");
//        formatCCFormattingTemplate("l1p", "oregon_washington", "2007");
//        formatCCFormattingTemplate("l1p", "puertorico", "2007");
//        formatCCFormattingTemplate("l1p", "redsea", "2007");
//        formatCCFormattingTemplate("l1p", "riolaplata", "2007");
//        formatCCFormattingTemplate("l1p", "southerncalifornia", "2007");
//        formatCCFormattingTemplate("l1p", "southindia", "2007");
//        formatCCFormattingTemplate("l1p", "tasmania", "2007");

    }

    private static void formatLCL2Template(String region, String... years) throws IOException {
        Assert.argument(LC_GEOMETRIES.containsKey(region), "valid region");
        String geometry = LC_GEOMETRIES.get(region);
        formatL2("lc-l2-template.xml", null, region, geometry, years);
    }

    private static void formatCCProcessingTemplate(String templateName, String region, String... years) throws
                                                                                                        IOException {
        Assert.argument(CC_GEOMETRIES.containsKey(region), "valid region");
        String geometry = CC_GEOMETRIES.get(region);
        formatL2(templateName, null, region, geometry, years);
    }

    private static void formatCCFormattingTemplate(String code, String region, String... years) throws IOException {
        Assert.argument(CC_GEOMETRIES.containsKey(region), "valid region");
        String geometry = CC_GEOMETRIES.get(region);
        formatL2("cc-nc-template.xml", code, region, geometry, years);
    }

    private static void formatL2(String templateName, String code, String region, String geometry,
                                 String... years) throws IOException {
        Assert.notNull(templateName, "templateName");
        Assert.notNull(region, "region");
        Assert.argument(years.length > 0, "years.length > 0");

        String template = readTemplate(templateName);
        for (String year : years) {
            String request = template.
                    replaceAll("\\$CODE", code).
                    replaceAll("\\$YEAR", year).
                    replaceAll("\\$REGION", region).
                    replaceAll("\\$GEOMETRY", geometry);
            String filename = templateName.replace("template", String.format("%s-%s", region, year));
            filename = String.format("%04d-%s", index++, filename);
            writeRequest(request, new File(filename));
        }
    }

    private static void formatL3ProcessingTemplate(String region, String startDate, String endDate,
                                                   int periodLength) throws Exception {
        formatL3Processing("lc-l3-processing-template-v2.xml", region, startDate, endDate, periodLength);
    }

    private static void formatL3FormattingTemplate(String region, String startDate, String endDate,
                                                   int periodLength) throws Exception {
        formatL3Formatting("lc-l3-format-template.xml", region, startDate, endDate, periodLength);
    }

    private static void formatL3Processing(String templateName, String region, String startDate, String endDate,
                                           int periodLength) throws Exception {
        String template = readTemplate(templateName);
        List<InputFiles> inputFilesList = getInputs(region, startDate, endDate, periodLength);
        String year = startDate.substring(0, 4);
        String geometry = LC_GEOMETRIES.get(region);

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

    private static void formatL3Formatting(String templateName, String region, String startDate, String endDate,
                                           int periodLength) throws Exception {
        String template = readTemplate(templateName);
        List<InputFiles> inputFilesList = getInputs(region, startDate, endDate, periodLength);
        String year = startDate.substring(0, 4);

        for (InputFiles inputfile : inputFilesList) {
            String request = template.
                    replaceAll("\\$YEAR", year).
                    replaceAll("\\$REGION", region).
                    replaceAll("\\$STARTDATE", inputfile.startDate).
                    replaceAll("\\$STOPDATE", inputfile.stopDate).
                    replaceAll("\\$PERIOD", inputfile.perioID);
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

    private static List<InputFiles> getInputs(String region, String startDate, String endDate, int periodLength) throws
                                                                                                                 Exception {
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
            result.add(new InputFiles(periodID, inputFiles, date1Str, date2Str));
            time += periodLengthMillis;
        }
        return result;
    }

    private static class InputFiles {

        final String perioID;
        final String[] inputFiles;
        final String startDate;
        final String stopDate;

        private InputFiles(String perioID, String[] inputFiles, String startDate, String stopDate) {
            this.perioID = perioID;
            this.inputFiles = inputFiles;
            this.startDate = startDate;
            this.stopDate = stopDate;
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
                    if (pathName.matches(regex)) {
                        return true;
                    }
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

    public static String[] listFilePaths(FileSystem fileSystem, String dirPath, PathFilter pathfilter) throws
                                                                                                       IOException {
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

    private static Map<String, String> createRegionMap(Properties properties) {
        Set<String> regionNames = properties.stringPropertyNames();
        Map<String, String> map = new Hashtable<String, String>();
        for (String regionName : regionNames) {
            map.put(regionName, properties.getProperty(regionName));
        }
        return map;
    }

    private static Properties loadRegions(String resource) {
        Properties properties = new Properties();
        InputStream inputStream = LcRequest.class.getResourceAsStream(resource);
        if (inputStream == null) {
            throw new IllegalStateException("Resource not found: " + resource);
        }
        try {
            try {
                properties.load(inputStream);
            } finally {
                inputStream.close();
            }
            return properties;
        } catch (IOException e) {
            throw new IllegalStateException("Error reading resource: " + resource, e);
        }
    }


}
