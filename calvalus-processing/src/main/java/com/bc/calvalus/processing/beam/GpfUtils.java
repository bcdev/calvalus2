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

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.util.SystemUtils;
import org.esa.snap.runtime.Config;

import javax.media.jai.JAI;
import java.util.Map;
import java.util.logging.Logger;

/**
 * This class contains utility methods for dealing with SNAP and GPF
 */
public class GpfUtils {

    private static final long OneMiB = 1024L * 1024L;
    private static final Logger LOG = CalvalusLogger.getLogger();

    /**
     * Initializes SNAP and GPF based on the configuration.
     *
     * @param configuration The Hadoop job configuration
     */
    public static void init(Configuration configuration) {
        initGpf(configuration, configuration.getClass());
    }

    public static void initGpf(Configuration configuration, Class aClass) {
        final long maxMemory = Runtime.getRuntime().maxMemory() / OneMiB;
        LOG.info(String.format("Java Virtual Machine max memory is %d MiB", maxMemory));
        initSystemProperties(configuration);
        SystemUtils.init3rdPartyLibs(aClass);
        JAI.enableDefaultTileCache();
        final long tileCacheSize = JAI.getDefaultInstance().getTileCache().getMemoryCapacity() / OneMiB;
        LOG.info(String.format("JAI tile cache size is %d MiB", tileCacheSize));
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        String tmpDir = System.getProperties().getProperty("java.io.tmpdir");
        Config.instance().preferences().put(SystemUtils.SNAP_CACHE_DIR_PROPERTY_NAME, tmpDir);
    }

    private static void initSystemProperties(Configuration configuration) {
        for (Map.Entry<String, String> entry : configuration) {
            String key = entry.getKey();
            if (key.startsWith("calvalus.system.")) {
                String propertyName = key.substring("calvalus.system.".length());
                String propertyValue = entry.getValue();
                LOG.info(String.format("Setting system property: %s=%s", propertyName, propertyValue));
                System.setProperty(propertyName, propertyValue);
            }
        }
    }
}
