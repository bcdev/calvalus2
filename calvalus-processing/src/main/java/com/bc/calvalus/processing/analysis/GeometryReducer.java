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

package com.bc.calvalus.processing.analysis;

import com.bc.calvalus.processing.JobConfigNames;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A reducer that paints WKT geometries onto the earth.
 */
public class GeometryReducer extends Reducer<Text, Text, NullWritable, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        BufferedImage sourceWorldMap = ImageIO.read(GeometryReducer.class.getResourceAsStream("worldMap.png"));

        Color lineColor = Color.WHITE;
        Color fillColor = new Color(255, 255, 255, 150);
        WorldQuickLookGenerator worldGenerator = new WorldQuickLookGenerator(lineColor, fillColor);
        WKTReader wktReader = new WKTReader();
        for (Text wkt : values) {
            try {
                worldGenerator.addGeometry(wktReader.read(wkt.toString()));
            } catch (ParseException ignore) {
            }
        }
        Configuration jobConfig = context.getConfiguration();
        String dateStart = jobConfig.get(JobConfigNames.CALVALUS_MIN_DATE);
        String outputPrefix = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_PREFIX, "L3");
        String imageName = String.format("%s_%s_worldMap.png", outputPrefix, dateStart);

        BufferedImage worldMap = worldGenerator.createQuickLookImage(sourceWorldMap);
        Path path = new Path(FileOutputFormat.getWorkOutputPath(context), imageName);
        OutputStream outputStream = path.getFileSystem(jobConfig).create(path);

        try {
            ImageIO.write(worldMap, "png", outputStream);
        } finally {
            outputStream.close();
        }

    }
}
