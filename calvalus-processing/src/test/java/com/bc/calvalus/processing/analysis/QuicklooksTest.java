/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Progressable;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Ignore;
import org.junit.Test;

import javax.imageio.ImageIO;
import java.awt.image.RenderedImage;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import static org.junit.Assert.*;

/**
 * @author Marco Peters
 */
public class QuicklooksTest {

    @Test
    public void testReadSingleConfig() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("<quicklooks>\n");
        sb.append("<configs>\n");
        sb.append("<config>\n");
        sb.append("<subSamplingX>3</subSamplingX>\n");
        sb.append("<subSamplingY>5</subSamplingY>\n");
        sb.append("<RGBAExpressions>var2 + log(var3), complicated expression, 1-3 + 5,constant</RGBAExpressions>\n");
        sb.append("<RGBAMinSamples>0.45,1.34,5,77</RGBAMinSamples>\n");
        sb.append("<RGBAMaxSamples>23,5.0007,.456</RGBAMaxSamples>\n");
        sb.append("<bandName>chl_conc</bandName>\n");
        sb.append("<cpdURL>http://www.allMycpds.com/chl.cpd</cpdURL>\n");
        sb.append("<imageType>SpecialType</imageType>\n");
        sb.append("<backgroundColor>10,20,30,128</backgroundColor>\n");
        sb.append("<overlayURL>file://C:\\User\\home\\overlay.png</overlayURL>\n");
        sb.append("<maskOverlays>land,cloud,coastline</maskOverlays>\n");
        sb.append("</config>\n");
        sb.append("</configs>\n");
        sb.append("</quicklooks>\n");
        String xml = sb.toString();
        Quicklooks.QLConfig qlConfig = Quicklooks.fromXml(xml).getConfigs()[0];
        assertEquals(3, qlConfig.getSubSamplingX());
        assertEquals(5, qlConfig.getSubSamplingY());
        String[] rgbaExpressions = qlConfig.getRGBAExpressions();
        assertEquals(4, rgbaExpressions.length);
        assertEquals("var2 + log(var3)", rgbaExpressions[0]);
        assertEquals(" complicated expression", rgbaExpressions[1]);
        assertEquals(" 1-3 + 5", rgbaExpressions[2]);
        assertEquals("constant", rgbaExpressions[3]);
        double[] v1Values = qlConfig.getRGBAMinSamples();
        assertEquals(4, v1Values.length);
        assertEquals(0.45, v1Values[0], 1.0e-8);
        assertEquals(1.34, v1Values[1], 1.0e-8);
        assertEquals(5, v1Values[2], 1.0e-8);
        assertEquals(77, v1Values[3], 1.0e-8);
        double[] v2Values = qlConfig.getRGBAMaxSamples();
        assertEquals(3, v2Values.length);
        assertEquals(23, v2Values[0], 1.0e-8);
        assertEquals(5.0007, v2Values[1], 1.0e-8);
        assertEquals(0.456, v2Values[2], 1.0e-8);
        assertEquals("chl_conc", qlConfig.getBandName());
        assertEquals("http://www.allMycpds.com/chl.cpd", qlConfig.getCpdURL());
        assertEquals("SpecialType", qlConfig.getImageType());
        assertEquals(10, qlConfig.getBackgroundColor().getRed());
        assertEquals(20, qlConfig.getBackgroundColor().getGreen());
        assertEquals(30, qlConfig.getBackgroundColor().getBlue());
        assertEquals(128, qlConfig.getBackgroundColor().getAlpha());
        assertEquals("file://C:\\User\\home\\overlay.png", qlConfig.getOverlayURL());
        assertEquals(3, qlConfig.getMaskOverlays().length);
        assertEquals("land", qlConfig.getMaskOverlays()[0]);
        assertEquals("cloud", qlConfig.getMaskOverlays()[1]);
        assertEquals("coastline", qlConfig.getMaskOverlays()[2]);

    }

    @Test
    public void testReadSeveralConfigs() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("<quicklooks>\n");
        sb.append("<configs>\n");
        sb.append("<config>\n");
        sb.append("<subSamplingX>3</subSamplingX>\n");
        sb.append("<subSamplingY>5</subSamplingY>\n");
        sb.append("<bandName>chl_conc</bandName>\n");
        sb.append("<cpdURL>http://www.allMycpds.com/chl.cpd</cpdURL>\n");
        sb.append("<imageType>SpecialType</imageType>\n");
        sb.append("<overlayURL>file://C:\\User\\home\\overlay.png</overlayURL>\n");
        sb.append("</config>\n");
        sb.append("<config>\n");
        sb.append("<bandName>alpha</bandName>\n");
        sb.append("<cpdURL>http://www.allMycpds.com/alpha.cpd</cpdURL>\n");
        sb.append("<imageType>PNG</imageType>\n");
        sb.append("</config>\n");
        sb.append("<config>\n");
        sb.append("</config>\n");
        sb.append("</configs>\n");
        sb.append("</quicklooks>\n");
        String xml = sb.toString();

        Quicklooks quicklooks = Quicklooks.fromXml(xml);
        Quicklooks.QLConfig[] configs = quicklooks.getConfigs();
        assertEquals(3, configs.length);
        assertEquals("chl_conc", configs[0].getBandName());
        assertEquals("alpha", configs[1].getBandName());
        assertEquals("RGB", configs[2].getBandName());
    }

    @Ignore
    @Test
    public void testGenerateQuicklook() throws IOException {
        TaskAttemptContext context = new TaskAttemptContext() {
            @Override
            public TaskAttemptID getTaskAttemptID() {
                return null;
            }

            @Override
            public Progressable getProgressible() {
                return null;
            }

            @Override
            public JobConf getJobConf() {
                return null;
            }

            @Override
            public void setStatus(String msg) {

            }

            @Override
            public String getStatus() {
                return null;
            }

            @Override
            public float getProgress() {
                return 0;
            }

            @Override
            public Counter getCounter(Enum<?> counterName) {
                return null;
            }

            @Override
            public Counter getCounter(String groupName, String counterName) {
                return null;
            }

            @Override
            public Configuration getConfiguration() {
                return new Configuration();
            }

            @Override
            public Credentials getCredentials() {
                return null;
            }

            @Override
            public JobID getJobID() {
                return null;
            }

            @Override
            public int getNumReduceTasks() {
                return 0;
            }

            @Override
            public Path getWorkingDirectory() throws IOException {
                return null;
            }

            @Override
            public Class<?> getOutputKeyClass() {
                return null;
            }

            @Override
            public Class<?> getOutputValueClass() {
                return null;
            }

            @Override
            public Class<?> getMapOutputKeyClass() {
                return null;
            }

            @Override
            public Class<?> getMapOutputValueClass() {
                return null;
            }

            @Override
            public String getJobName() {
                return null;
            }

            @Override
            public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public RawComparator<?> getSortComparator() {
                return null;
            }

            @Override
            public String getJar() {
                return null;
            }

            @Override
            public RawComparator<?> getCombinerKeyGroupingComparator() {
                return null;
            }

            @Override
            public RawComparator<?> getGroupingComparator() {
                return null;
            }

            @Override
            public boolean getJobSetupCleanupNeeded() {
                return false;
            }

            @Override
            public boolean getTaskCleanupNeeded() {
                return false;
            }

            @Override
            public boolean getProfileEnabled() {
                return false;
            }

            @Override
            public String getProfileParams() {
                return null;
            }

            @Override
            public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
                return null;
            }

            @Override
            public String getUser() {
                return null;
            }

            @Override
            public boolean getSymlink() {
                return false;
            }

            @Override
            public Path[] getArchiveClassPaths() {
                return new Path[0];
            }

            @Override
            public URI[] getCacheArchives() throws IOException {
                return new URI[0];
            }

            @Override
            public URI[] getCacheFiles() throws IOException {
                return new URI[0];
            }

            @Override
            public Path[] getLocalCacheArchives() throws IOException {
                return new Path[0];
            }

            @Override
            public Path[] getLocalCacheFiles() throws IOException {
                return new Path[0];
            }

            @Override
            public Path[] getFileClassPaths() {
                return new Path[0];
            }

            @Override
            public String[] getArchiveTimestamps() {
                return new String[0];
            }

            @Override
            public String[] getFileTimestamps() {
                return new String[0];
            }

            @Override
            public int getMaxMapAttempts() {
                return 0;
            }

            @Override
            public int getMaxReduceAttempts() {
                return 0;
            }

            @Override
            public void progress() {

            }
        };
        Product product = ProductIO.readProduct("/windows/tmp/L3_2021-05-10_2021-05-10.nc");
        //Quicklooks.QLConfig qlConfig = Quicklooks.fromXml("<parameters> <quicklooks><config><RGBAExpressions>B4,B3,B2,</RGBAExpressions><RGBAMinSamples>0.0,0.0,0.0</RGBAMinSamples><RGBAMaxSamples>0.21,0.21,0.21</RGBAMaxSamples><shapefileURL>file:///windows/tmp/country_tiles_lines_for_ql_sub.zip</shapefileURL><imageType>png</imageType></config></quicklooks> </parameters>").getConfigs()[0];
        Quicklooks.QLConfig qlConfig = Quicklooks.fromXml("<parameters> <quicklooks><config><RGBAExpressions>B4,B3,B2,</RGBAExpressions><RGBAMinSamples>0.0,0.0,0.0</RGBAMinSamples><RGBAMaxSamples>0.21,0.21,0.21</RGBAMaxSamples><shapefileURL>file:///windows/tmp/country_tiles_orbits_lines_20190610_sub.zip</shapefileURL><imageType>png</imageType></config></quicklooks> </parameters>").getConfigs()[0];
        QuicklookGenerator quicklookGenerator = new QuicklookGenerator(context, product, qlConfig);
        RenderedImage image = quicklookGenerator.createImage();
        OutputStream outputStream = new FileOutputStream("/windows/tmp/ql." + qlConfig.getImageType());
        try {
            ImageIO.write(image, qlConfig.getImageType(), outputStream);
        } finally {
            outputStream.close();
        }
    }

    private static OutputStream createOutputStream(Mapper.Context context, String fileName) throws IOException, InterruptedException {
        Path path = new Path(FileOutputFormat.getWorkOutputPath(context), fileName);
        final FSDataOutputStream fsDataOutputStream = path.getFileSystem(context.getConfiguration()).create(path);
        return new BufferedOutputStream(fsDataOutputStream);
    }

}
