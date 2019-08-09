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

package com.bc.calvalus.processing;

/**
 * Names of Hadoop job configuration parameters.
 */
public interface JobConfigNames {

    String CALVALUS_CALVALUS_BUNDLE = "calvalus.calvalus.bundle";
    String CALVALUS_SNAP_BUNDLE = "calvalus.snap.bundle";

    String CALVALUS_INPUT_DIR = "calvalus.input.dir";
    String CALVALUS_INPUT_PATH_PATTERNS = "calvalus.input.pathPatterns";
    String CALVALUS_INPUT_DATE_RANGES = "calvalus.input.dateRanges";
    String CALVALUS_INPUT_REGION_NAME = "calvalus.input.regionName";
    String CALVALUS_INPUT_MIN_WIDTH = "calvalus.input.minWidth";
    String CALVALUS_INPUT_MIN_HEIGHT = "calvalus.input.minHeight";
    String CALVALUS_INPUT_FULL_SWATH = "calvalus.input.fullSwath";
    String CALVALUS_INPUT_FORMAT = "calvalus.input.format";
    String CALVALUS_INPUT_INVENTORY = "calvalus.input.productInventory";
    String CALVALUS_INPUT_TABLE = "calvalus.input.table";
    String CALVALUS_INPUT_GEO_INVENTORY = "calvalus.input.geoInventory";

    String CALVALUS_OUTPUT_DIR = "calvalus.output.dir";
    String CALVALUS_OUTPUT_FORMAT = "calvalus.output.format";
    String CALVALUS_OUTPUT_COMPRESSION = "calvalus.output.compression";
    String CALVALUS_OUTPUT_REPLACE_NAN_VALUE = "calvalus.output.replaceNanValue";
    String CALVALUS_OUTPUT_CRS = "calvalus.output.crs";
    String CALVALUS_OUTPUT_BANDLIST = "calvalus.output.bandList";
    String CALVALUS_OUTPUT_REGEX = "calvalus.output.regex";
    String CALVALUS_OUTPUT_REPLACEMENT = "calvalus.output.replacement";
    String CALVALUS_OUTPUT_PREFIX = "calvalus.output.prefix";
    String CALVALUS_OUTPUT_POSTFIX = "calvalus.output.postfix";
    String CALVALUS_OUTPUT_NAMEFORMAT = "calvalus.output.nameformat";
    String CALVALUS_OUTPUT_PRESERVE_DATE_TREE = "calvalus.output.preserveDateTree";

    String CALVALUS_OUTPUT_QUICKLOOKS = "calvalus.output.quicklooks";
    String CALVALUS_QUICKLOOK_PARAMETERS = "calvalus.ql.parameters";

    String CALVALUS_L2_OPERATOR = "calvalus.l2.operator";
    String CALVALUS_L2_PARAMETERS = "calvalus.l2.parameters";
    String CALVALUS_L2_PROCESSOR_TYPE = "calvalus.l2.processorType";

    String CALVALUS_BUNDLES = "calvalus.bundles";

    String CALVALUS_METADATA_TEMPLATE = "calvalus.metadata.template";
    String CALVALUS_LC_VERSION = "calvalus.lc.version";

    String CALVALUS_PROJECT_NAME = "calvalus.project.name";

    String CALVALUS_L3_PARAMETERS = "calvalus.l3.parameters";
    String CALVALUS_L3_START_UTC = "calvalus.l3.startUtc";
    String CALVALUS_L3_PERIOD_DURATION = "calvalus.l3.periodDuration";
    String CALVALUS_L3_FEATURE_NAMES = "calvalus.l3.featureNames";
    String CALVALUS_L3_FORMAT_PARAMETERS = "calvalus.l3.format.parameters";
    String CALVALUS_CELL_PARAMETERS = "calvalus.cell.parameters";
    String CALVALUS_L3_REDUCERS = "calvalus.l3.reducers";
    String CALVALUS_L3_COMPUTE_OUTPUTS = "calvalus.l3.computeOutputs";
    String CALVALUS_MOSAIC_PARAMETERS = "calvalus.mosaic.parameters";
    String CALVALUS_MA_PARAMETERS = "calvalus.ma.parameters";
    String CALVALUS_TA_PARAMETERS = "calvalus.ta.parameters";
    String PROCESSING_HISTORY = "processing_history";

    public static final String[] LEVEL3_METADATA_KEYS = {
            JobConfigNames.CALVALUS_MIN_DATE,
            JobConfigNames.CALVALUS_MAX_DATE,
            JobConfigNames.CALVALUS_REGION_GEOMETRY,
            JobConfigNames.CALVALUS_INPUT_REGION_NAME,
            JobConfigNames.CALVALUS_L3_PARAMETERS,
            JobConfigNames.CALVALUS_L3_FEATURE_NAMES,
            JobConfigNames.PROCESSING_HISTORY
    };

    String CALVALUS_MIN_DATE = "calvalus.minDate";
    String CALVALUS_MAX_DATE = "calvalus.maxDate";
    String CALVALUS_REGION_GEOMETRY = "calvalus.regionGeometry";
    String CALVALUS_USER = "calvalus.user";
    String CALVALUS_PRODUCTION_TYPE = "calvalus.productionType";
    String CALVALUS_PROCESS_ALL = "calvalus.processAll";

    String TA_WITH_TIMESERIES_PLOT = "withTimeseriesPlot";
    String TA_WITH_AGGREGATED_CSV = "withAggregatedCsv";
    String TA_WITH_PIXEL_CSV = "withPixelCsv";

    String TA_WITH_L3_OUTPUTS = "withL3Outputs";
    String CALVALUS_TA_SKIPL3_FLAG = "skipL3";
    String CALVALUS_TA_KEEPL3_FLAG = "keepL3";

    String CALVALUS_DEBUG_TILECACHE = "calvalus.debug.tileCache";
    String CALVALUS_DEBUG_FIRE = "calvalus.debug.fire";
}
