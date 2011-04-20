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
public interface JobConfNames {
    String CALVALUS_CALVALUS_BUNDLE = "calvalus.calvalus.bundle";
    String CALVALUS_BEAM_BUNDLE = "calvalus.beam.bundle";
    String CALVALUS_INPUT = "calvalus.input";
    String CALVALUS_OUTPUT = "calvalus.output";
    String CALVALUS_L2_OPERATOR = "calvalus.l2.operator";
    String CALVALUS_L2_BUNDLE = "calvalus.l2.bundle";
    String CALVALUS_L2_PARAMETERS = "calvalus.l2.parameters";
    String CALVALUS_L3_PARAMETERS = "calvalus.l3.parameters";
    String CALVALUS_TA_PARAMETERS = "calvalus.ta.parameters";
    String CALVALUS_FORMATTER_PARAMETERS = "calvalus.formatter.parameters";
    String CALVALUS_SYSTEM_PROPERTIES = "calvalus.system.properties";
    String CALVALUS_FAIL_FAST =  "calvalus.failFast";

    // todo - nf/** 19.04.2011: check if we can put the following into L2Config, L3Config, TAConfig
    String CALVALUS_MIN_DATE = "calvalus.minDate";
    String CALVALUS_MAX_DATE = "calvalus.maxDate";
    String CALVALUS_REGION_GEOMETRY = "calvalus.regionGeometry";
}
