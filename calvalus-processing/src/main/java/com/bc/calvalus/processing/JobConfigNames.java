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

    String CALVALUS_SYSTEM_NAME = "calvalus.system.name";

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
    String CALVALUS_INPUT_PRODUCT_IDENTIFIERS = "calvalus.input.productIdentifiers";
    String CALVALUS_INPUT_COLLECTION_NAME = "calvalus.input.collectionName";
    String CALVALUS_INPUT_PRODUCT_TYPE = "calvalus.input.productType";

    String CALVALUS_OUTPUT_DIR = "calvalus.output.dir";
    String CALVALUS_OUTPUT_FORMAT = "calvalus.output.format";
    String CALVALUS_OUTPUT_PRODUCT_TYPE = "calvalus.output.productType";
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

    String CALVALUS_REQUEST_SIZE_LIMIT = "calvalus.requestSizeLimit";

    String CALVALUS_L2_OPERATOR = "calvalus.l2.operator";
    String CALVALUS_L2_PARAMETERS = "calvalus.l2.parameters";
    String CALVALUS_L2_PROCESSOR_TYPE = "calvalus.l2.processorType";
    String CALVALUS_L2_PROCESSOR_DESCRIPTION = "calvalus.l2.processorDescription";

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
    String CALVALUS_MA_USE_INPUT_PIXEL_POS = "calvalus.ma.useInputPixelPos";
    String CALVALUS_RA_PARAMETERS = "calvalus.ra.parameters";
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
    String CALVALUS_INPUT_SUBSETTING = "calvalus.input.subsetting";
    String CALVALUS_OUTPUT_SUBSETTING = "calvalus.output.subsetting";
    String CALVALUS_OUTPUT_DATE_ELEMENT = "calvalus.output.dateElement";
    String CALVALUS_OUTPUT_DATE_FORMAT = "calvalus.output.dateFormat";

    String PRODUCTION_TYPE = "productionType";
    String PRODUCTION_NAME = "productionName";
    String INPUT_INVENTORY = "inputInventory";
    String INPUT_PATH = "inputPath";
    String INPUT_TABLE = "inputTable";
    String PRODUCT_IDENTIFIERS = "productIdentifiers";
    String INPUT_FORMAT = "inputFormat";
    String DATE_RANGES = "dateRanges";
    String REGION_GEOMETRY = "regionGeometry";
    String REGION_NAME = "regionName";
    String PROCESSOR_NAME = "processorName";
    String PROCESSOR_PARAMETERS = "processorParameters";
    String AGGREGATION_PARAMETERS = "aggregationParameters";
    String QL_PARAMETERS = "qlParameters";
    String MATCHUP_PARAMETERS = "matchupParameters";
    String RA_PARAMETERS = "raParameters";
    String METADATA_TEMPLATE = "metadataTemplate";
    String OUTPUT_DIR = "outputDir";
    String OUTPUT_VERSION = "outputVersion";
    String OUTPUT_FORMAT = "outputFormat";
    String OUTPUT_COMPRESSION = "outputCompression";
    String OUTPUT_CRS = "outputCrs";
    String REPLACE_NAN_VALUE = "replaceNanValue";
    String OUTPUT_BANDS = "outputBands";
    String OUTPUT_REGEX = "outputRegex";
    String OUTPUT_REPLACEMENT = "outputReplacement";
    String OUTPUT_DATE_ELEMENT = "outputDateElement";
    String OUTPUT_DATE_FORMAT = "outputDateFormat";
    String FORCE_REPROCESS = "forceReprocess";
    String PRESERVE_DATE_TREE = "preserveDateTree";
    String INPUT_SUBSETTING = "inputSubsetting";
    String OUTPUT_SUBSETTING = "outputSubsetting";
    String QUEUE = "queue";
    String ATTEMPTS = "attempts";
    String FAILURE_PERCENT = "failurePercent";
    String MAX_REDUCERS = "maxReducers";
    String PROCESSOR_BUNDLES = "processorBundles";
    String CALVALUS = "calvalus";
    String SNAP = "snap";
    String SNAP_TILECACHE = "snap.tileCache";
}
