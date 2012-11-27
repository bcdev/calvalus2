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

/**
 * Names of Hadoop job configuration parameters.
 */
public interface JobConfigNames {

    String CALVALUS_CALVALUS_BUNDLE = "calvalus.calvalus.bundle";
    String CALVALUS_BEAM_BUNDLE = "calvalus.beam.bundle";

    String CALVALUS_INPUT_DIR = "calvalus.input.dir";
    String CALVALUS_INPUT_PATH_PATTERNS = "calvalus.input.pathPatterns";
    String CALVALUS_INPUT_DATE_RANGES = "calvalus.input.dateRanges";
    String CALVALUS_INPUT_REGION_NAME = "calvalus.input.regionName";
    String CALVALUS_INPUT_FORMAT = "calvalus.input.format";
    String CALVALUS_INPUT_INVENTORY = "calvalus.input.productInventory";

    String CALVALUS_OUTPUT_DIR = "calvalus.output.dir";
    String CALVALUS_OUTPUT_FORMAT = "calvalus.output.format";
    String CALVALUS_OUTPUT_COMPRESSION = "calvalus.output.compression";
    String CALVALUS_OUTPUT_CRS = "calvalus.output.crs";
    String CALVALUS_OUTPUT_BANDLIST = "calvalus.output.bandList";
    String CALVALUS_OUTPUT_REGEX = "calvalus.output.regex";
    String CALVALUS_OUTPUT_REPLACEMENT = "calvalus.output.replacement";
    String CALVALUS_OUTPUT_PREFIX = "calvalus.output.prefix";
    String CALVALUS_OUTPUT_NAMEFORMAT = "calvalus.output.nameformat";

    String CALVALUS_OUTPUT_QUICKLOOKS = "calvalus.output.quicklooks";
    String CALVALUS_QUICKLOOK_PARAMETERS = "calvalus.ql.parameters";

    String CALVALUS_L2_OPERATOR = "calvalus.l2.operator";
    String CALVALUS_L2_BUNDLE = "calvalus.l2.bundle";
    String CALVALUS_L2_PARAMETERS = "calvalus.l2.parameters";

    String CALVALUS_METADATA_TEMPLATE = "calvalus.metadata.template";

    String CALVALUS_L3_PARAMETERS = "calvalus.l3.parameters";
    String CALVALUS_MA_PARAMETERS = "calvalus.ma.parameters";
    String CALVALUS_TA_PARAMETERS = "calvalus.ta.parameters";


    String CALVALUS_MIN_DATE = "calvalus.minDate";
    String CALVALUS_MAX_DATE = "calvalus.maxDate";
    String CALVALUS_REGION_NAME = "calvalus.regionName";
    String CALVALUS_REGION_GEOMETRY = "calvalus.regionGeometry";
    String CALVALUS_USER = "calvalus.user";
    String CALVALUS_PRODUCTION_TYPE = "calvalus.productionType";
    String CALVALUS_REQUEST = "calvalus.request";
    String CALVALUS_RESUME_PROCESSING = "calvalus.resume";

    /**
     * @deprecated use following syntax to set (BEAM) system properties: calvalus.system.<system-property-name>
     */
    @Deprecated
    String CALVALUS_BEAM_TILE_CACHE_SIZE = "calvalus.beam.tileCacheSize";

}
