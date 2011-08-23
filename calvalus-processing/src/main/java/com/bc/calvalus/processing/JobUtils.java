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


import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Diverse utilities.
 *
 * @author MarcoZ
 */
public class JobUtils {



    public static Geometry createGeometry(String geometryWkt) {
        if (geometryWkt == null || geometryWkt.isEmpty()) {
            return null;
        }
        final WKTReader wktReader = new WKTReader();
        try {
            return wktReader.read(geometryWkt);
        } catch (com.vividsolutions.jts.io.ParseException e) {
            throw new IllegalArgumentException("Illegal region geometry: " + geometryWkt, e);
        }
    }

    public static void clearAndSetOutputDir(Job job, String outputDir) throws IOException {
        final Path outputPath = clearDir(job, outputDir);
        FileOutputFormat.setOutputPath(job, outputPath);
    }

    public static Path clearDir(Job job, String dir) throws IOException {
        return clearDir(dir, job.getConfiguration());
    }

    public static Path clearDir(String dir, Configuration configuration) throws IOException {
        final Path dirPath = new Path(dir);
        final FileSystem fileSystem = dirPath.getFileSystem(configuration);
        fileSystem.delete(dirPath, true);
        return dirPath;
    }
}
