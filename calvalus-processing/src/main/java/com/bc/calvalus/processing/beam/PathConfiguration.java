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

package com.bc.calvalus.processing.beam;

import com.bc.ceres.core.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
* Encapsulates a Hadoop Path and a Configuration
*/
public class PathConfiguration {
    private final Path path;
    private final Configuration configuration;

    public PathConfiguration(Path path, Configuration configuration) {
        Assert.notNull(path, "path");
        Assert.notNull(configuration, "configuration");
        this.path = path;
        this.configuration = configuration;
    }

    public Path getPath() {
        return path;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
