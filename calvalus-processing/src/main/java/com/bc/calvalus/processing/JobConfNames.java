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
    String CALVALUS_IDENTIFIER = "calvalus.identifier";
    String CALVALUS_BUNDLE = "calvalus.bundle";
    String CALVALUS_INPUT = "calvalus.input";
    String CALVALUS_OUTPUT = "calvalus.output";
    String CALVALUS_L2_OPERATOR = "calvalus.l2.operator";
    String CALVALUS_L2_PARAMETERS = "calvalus.l2.parameters";
    String CALVALUS_L3_PARAMETERS = "calvalus.l3.parameters";
    String CALVALUS_TA_PARAMETERS = "calvalus.ta.parameters";
    String CALVALUS_FORMATTER_PARAMETERS = "calvalus.formatter.parameters";
    String CALVALUS_SYSTEM_PROPERTIES = "calvalus.system.properties";
    String CALVALUS_START_DATE = "calvalus.startDate";
    String CALVALUS_STOP_DATE = "calvalus.stopDate";
    String CALVALUS_REGION_GEOMETRY = "calvalus.regionGeometry";
    String CALVALUS_FAIL_FAST =  "calvalus.failFast";
}
