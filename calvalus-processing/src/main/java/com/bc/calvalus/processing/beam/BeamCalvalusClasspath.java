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

package com.bc.calvalus.processing.beam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

public class BeamCalvalusClasspath {

    public static void configure(String processorPackage, Configuration configuration) throws IOException {
        // put processor onto the classpath
        addPackageToClassPath(processorPackage, configuration);

        // put BEAM and BEAM 3rd party-libs onto the classpath
        final String beamPackage = "beam";
        final String beamVersion = "4.9-SNAPSHOT";
        addPackageToClassPath(beamPackage + "-" + beamVersion, configuration);
    }

    public static void addPackageToClassPath(String packageName, Configuration configuration) throws IOException {
        final FileSystem beamFileSystem = FileSystem.get(configuration);
        final Path beamPath = new Path("/calvalus/software/0.5/" + packageName);

        final FileStatus[] beamJars = beamFileSystem.listStatus(beamPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith("jar");
            }
        });
        for (FileStatus beamJar : beamJars) {
            final Path path = beamJar.getPath();
            final Path pathWithoutProtocol = new Path(path.toUri().getPath());  // for hadoops sake!
            DistributedCache.addFileToClassPath(pathWithoutProtocol, configuration);
        }
    }

}
